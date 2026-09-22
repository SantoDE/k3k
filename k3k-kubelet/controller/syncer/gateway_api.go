package syncer

import (
	"context"
	"reflect"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/rancher/k3k/k3k-kubelet/translate"
	"github.com/rancher/k3k/pkg/apis/k3k.io/v1beta1"
)

const (
	gatewayAPIControllerName       = "gateway_api-syncer-controller"
	gatewayAPIFinalizerName        = "gatewayapi.k3k.io/finalizer"
	gatewayAPIStatusControllerName = "gateway-api-status-syncer-controller"

	tlsRouteControllerName       = "tls-route-syncer-controller"
	tlsRouteFinalizerName        = "tlsroute.k3k.io/finalizer"
	tlsRouteStatusControllerName = "tls-route-status-syncer-controller"

	referenceGrantControllerName = "reference-grant-syncer-controller"
	referenceGrantFinalizerName  = "referencegrant.k3k.io/finalizer"

	backendTLSPolicyControllerName       = "backend-tls-policy-syncer-controller"
	backendTLSPolicyFinalizerName        = "backendtlspolicy.k3k.io/finalizer"
	backendTLSPolicyStatusControllerName = "backend-tls-policy-status-syncer-controller"
)

func newGatewayContext(clusterName, clusterNamespace string, virtMgr, hostMgr manager.Manager) *Context {
	return &Context{
		ClusterName:      clusterName,
		ClusterNamespace: clusterNamespace,
		VirtualClient:    virtMgr.GetClient(),
		HostClient:       hostMgr.GetClient(),
		HostReader:       hostMgr.GetAPIReader(),
		Translator: translate.ToHostTranslator{
			ClusterName:      clusterName,
			ClusterNamespace: clusterNamespace,
		},
	}
}

// effectiveSync returns the sync config in effect, preferring a VirtualClusterPolicy override.
func effectiveSync(cluster *v1beta1.Cluster) *v1beta1.SyncConfig {
	if cluster.Status.Policy != nil && cluster.Status.Policy.Sync != nil {
		return cluster.Status.Policy.Sync
	}
	if cluster.Spec.Sync != nil {
		return cluster.Spec.Sync
	}
	return &v1beta1.SyncConfig{}
}

// ── Generic toHost reconciler ─────────────────────────────────────────────────

// toHostReconciler handles the common create/update/delete lifecycle for syncing
// a single Gateway API type from the virtual cluster to the host cluster.
type toHostReconciler[T ctrlruntimeclient.Object] struct {
	*Context
	finalizer    string
	syncEnabled  func(*v1beta1.SyncConfig) (enabled bool, selector map[string]string)
	translateObj func(*Context, T, *v1beta1.SyncConfig) T
	newObj       func() T
}

func (r *toHostReconciler[T]) filterResources(object ctrlruntimeclient.Object) bool {
	var cluster v1beta1.Cluster
	if err := r.HostClient.Get(context.Background(), types.NamespacedName{Name: r.ClusterName, Namespace: r.ClusterNamespace}, &cluster); err != nil {
		return false
	}
	enabled, selector := r.syncEnabled(effectiveSync(&cluster))
	if !enabled {
		return object.GetDeletionTimestamp() != nil
	}
	ls := labels.SelectorFromSet(selector)
	if ls.Empty() {
		return true
	}
	return ls.Matches(labels.Set(object.GetLabels()))
}

func (r *toHostReconciler[T]) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("cluster", r.ClusterName, "clusterNamespace", r.ClusterNamespace)
	ctx = ctrl.LoggerInto(ctx, log)

	var cluster v1beta1.Cluster
	if err := r.HostClient.Get(ctx, types.NamespacedName{Name: r.ClusterName, Namespace: r.ClusterNamespace}, &cluster); err != nil {
		return reconcile.Result{}, err
	}

	virtObj := r.newObj()
	if err := r.VirtualClient.Get(ctx, req.NamespacedName, virtObj); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	syncedObj := r.translateObj(r.Context, virtObj, effectiveSync(&cluster))

	if err := controllerutil.SetOwnerReference(&cluster, syncedObj, r.HostClient.Scheme()); err != nil {
		return reconcile.Result{}, err
	}

	if !virtObj.GetDeletionTimestamp().IsZero() {
		if err := r.HostClient.Delete(ctx, syncedObj); err != nil {
			return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
		}
		if controllerutil.RemoveFinalizer(virtObj, r.finalizer) {
			return reconcile.Result{}, r.VirtualClient.Update(ctx, virtObj)
		}
		return reconcile.Result{}, nil
	}

	if controllerutil.AddFinalizer(virtObj, r.finalizer) {
		if err := r.VirtualClient.Update(ctx, virtObj); err != nil {
			return reconcile.Result{}, err
		}
	}

	hostObj := r.newObj()
	if err := r.HostReader.Get(ctx, types.NamespacedName{Name: syncedObj.GetName(), Namespace: syncedObj.GetNamespace()}, hostObj); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("creating resource on the host cluster")
			return reconcile.Result{}, r.HostClient.Create(ctx, syncedObj)
		}
		return reconcile.Result{}, err
	}

	log.Info("updating resource on the host cluster")
	syncedObj.SetResourceVersion(hostObj.GetResourceVersion())
	return reconcile.Result{}, r.HostClient.Update(ctx, syncedObj)
}

func addToHostSyncer[T ctrlruntimeclient.Object](virtMgr manager.Manager, rec *toHostReconciler[T], controllerName string) error {
	name := rec.Translator.TranslateName(rec.ClusterNamespace, controllerName)
	return ctrl.NewControllerManagedBy(virtMgr).
		Named(name).
		For(rec.newObj()).
		WithEventFilter(predicate.NewPredicateFuncs(rec.filterResources)).
		Complete(rec)
}

// ── Generic status reconciler ─────────────────────────────────────────────────

// statusReconciler mirrors the Status field of a host Gateway API object back to
// the corresponding virtual cluster object, identified via k3k annotations.
type statusReconciler[T ctrlruntimeclient.Object] struct {
	*Context
	kind   string
	newObj func() T
}

func (r *statusReconciler[T]) filterClusterObjects(obj ctrlruntimeclient.Object) bool {
	return obj.GetLabels()[translate.ClusterNameLabel] == r.ClusterName
}

func (r *statusReconciler[T]) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("cluster", r.ClusterName, "clusterNamespace", r.ClusterNamespace)
	ctx = ctrl.LoggerInto(ctx, log)

	hostObj := r.newObj()
	if err := r.HostClient.Get(ctx, req.NamespacedName, hostObj); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	annotations := hostObj.GetAnnotations()
	virtName := annotations[translate.ResourceNameAnnotation]
	virtNamespace := annotations[translate.ResourceNamespaceAnnotation]
	if virtName == "" || virtNamespace == "" {
		return reconcile.Result{}, nil
	}

	virtObj := r.newObj()
	if err := r.VirtualClient.Get(ctx, types.NamespacedName{Name: virtName, Namespace: virtNamespace}, virtObj); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	hostStatus := reflect.ValueOf(hostObj).Elem().FieldByName("Status")
	virtStatus := reflect.ValueOf(virtObj).Elem().FieldByName("Status")
	if reflect.DeepEqual(virtStatus.Interface(), hostStatus.Interface()) {
		return reconcile.Result{}, nil
	}

	log.Info("mirroring "+r.kind+" status to virtual cluster", "name", virtName, "namespace", virtNamespace)
	virtStatus.Set(hostStatus)
	return reconcile.Result{}, r.VirtualClient.Status().Update(ctx, virtObj)
}

func addStatusSyncer[T ctrlruntimeclient.Object](hostMgr manager.Manager, rec *statusReconciler[T], controllerName string) error {
	name := rec.Translator.TranslateName(rec.ClusterNamespace, controllerName)
	return ctrl.NewControllerManagedBy(hostMgr).
		Named(name).
		For(rec.newObj()).
		WithEventFilter(predicate.NewPredicateFuncs(rec.filterClusterObjects)).
		Complete(rec)
}

// ── Translation functions ─────────────────────────────────────────────────────

func translateParentRefs(ctx *Context, refs []gatewayv1.ParentReference, srcNamespace string, override *v1beta1.GatewayParentRef) []gatewayv1.ParentReference {
	if override != nil {
		ns := gatewayv1.Namespace(override.Namespace)
		return []gatewayv1.ParentReference{{
			Name:      gatewayv1.ObjectName(override.Name),
			Namespace: &ns,
		}}
	}
	out := make([]gatewayv1.ParentReference, len(refs))
	copy(out, refs)
	for i := range out {
		srcNS := srcNamespace
		if out[i].Namespace != nil {
			srcNS = string(*out[i].Namespace)
		}
		out[i].Name = gatewayv1.ObjectName(ctx.Translator.TranslateName(srcNS, string(refs[i].Name)))
		ns := gatewayv1.Namespace(ctx.ClusterNamespace)
		out[i].Namespace = &ns
	}
	return out
}

func translateHTTPRoute(ctx *Context, obj *gatewayv1.HTTPRoute, sync *v1beta1.SyncConfig) *gatewayv1.HTTPRoute {
	out := obj.DeepCopy()
	ctx.Translator.TranslateTo(out)
	out.Spec.ParentRefs = translateParentRefs(ctx, obj.Spec.ParentRefs, obj.Namespace, sync.HTTPRoutes.OverrideParentGateway)
	for i := range out.Spec.Rules {
		for j := range out.Spec.Rules[i].BackendRefs {
			out.Spec.Rules[i].BackendRefs[j].Name = gatewayv1.ObjectName(
				ctx.Translator.TranslateName(obj.Namespace, string(obj.Spec.Rules[i].BackendRefs[j].Name)),
			)
		}
	}
	return out
}

func translateTLSRoute(ctx *Context, obj *gatewayv1.TLSRoute, sync *v1beta1.SyncConfig) *gatewayv1.TLSRoute {
	out := obj.DeepCopy()
	ctx.Translator.TranslateTo(out)
	out.Spec.ParentRefs = translateParentRefs(ctx, obj.Spec.ParentRefs, obj.Namespace, sync.TLSRoutes.OverrideParentGateway)
	for i := range out.Spec.Rules {
		for j := range out.Spec.Rules[i].BackendRefs {
			out.Spec.Rules[i].BackendRefs[j].Name = gatewayv1.ObjectName(
				ctx.Translator.TranslateName(obj.Namespace, string(obj.Spec.Rules[i].BackendRefs[j].Name)),
			)
		}
	}
	return out
}

func translateReferenceGrant(ctx *Context, obj *gatewayv1.ReferenceGrant, _ *v1beta1.SyncConfig) *gatewayv1.ReferenceGrant {
	out := obj.DeepCopy()
	ctx.Translator.TranslateTo(out)
	// All virtual-cluster namespaces collapse to clusterNamespace on the host.
	for i := range out.Spec.From {
		out.Spec.From[i].Namespace = gatewayv1.Namespace(ctx.ClusterNamespace)
	}
	return out
}

func translateBackendTLSPolicy(ctx *Context, obj *gatewayv1.BackendTLSPolicy, _ *v1beta1.SyncConfig) *gatewayv1.BackendTLSPolicy {
	out := obj.DeepCopy()
	ctx.Translator.TranslateTo(out)
	for i := range out.Spec.TargetRefs {
		out.Spec.TargetRefs[i].Name = gatewayv1.ObjectName(
			ctx.Translator.TranslateName(obj.Namespace, string(obj.Spec.TargetRefs[i].Name)),
		)
	}
	for i := range out.Spec.Validation.CACertificateRefs {
		out.Spec.Validation.CACertificateRefs[i].Name = gatewayv1.ObjectName(
			ctx.Translator.TranslateName(obj.Namespace, string(obj.Spec.Validation.CACertificateRefs[i].Name)),
		)
	}
	return out
}

// ── Public Add* functions ─────────────────────────────────────────────────────

func AddGatewayAPISyncer(_ context.Context, virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string) error {
	return addToHostSyncer(virtMgr, &toHostReconciler[*gatewayv1.HTTPRoute]{
		Context:      newGatewayContext(clusterName, clusterNamespace, virtMgr, hostMgr),
		finalizer:    gatewayAPIFinalizerName,
		syncEnabled:  func(s *v1beta1.SyncConfig) (bool, map[string]string) { return s.HTTPRoutes.Enabled, s.HTTPRoutes.Selector },
		translateObj: translateHTTPRoute,
		newObj:       func() *gatewayv1.HTTPRoute { return &gatewayv1.HTTPRoute{} },
	}, gatewayAPIControllerName)
}

func AddGatewayAPIStatusSyncer(_ context.Context, virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string) error {
	return addStatusSyncer(hostMgr, &statusReconciler[*gatewayv1.HTTPRoute]{
		Context: newGatewayContext(clusterName, clusterNamespace, virtMgr, hostMgr),
		kind:    "httproute",
		newObj:  func() *gatewayv1.HTTPRoute { return &gatewayv1.HTTPRoute{} },
	}, gatewayAPIStatusControllerName)
}

func AddTLSRouteSyncer(_ context.Context, virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string) error {
	return addToHostSyncer(virtMgr, &toHostReconciler[*gatewayv1.TLSRoute]{
		Context:      newGatewayContext(clusterName, clusterNamespace, virtMgr, hostMgr),
		finalizer:    tlsRouteFinalizerName,
		syncEnabled:  func(s *v1beta1.SyncConfig) (bool, map[string]string) { return s.TLSRoutes.Enabled, s.TLSRoutes.Selector },
		translateObj: translateTLSRoute,
		newObj:       func() *gatewayv1.TLSRoute { return &gatewayv1.TLSRoute{} },
	}, tlsRouteControllerName)
}

func AddTLSRouteStatusSyncer(_ context.Context, virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string) error {
	return addStatusSyncer(hostMgr, &statusReconciler[*gatewayv1.TLSRoute]{
		Context: newGatewayContext(clusterName, clusterNamespace, virtMgr, hostMgr),
		kind:    "tlsroute",
		newObj:  func() *gatewayv1.TLSRoute { return &gatewayv1.TLSRoute{} },
	}, tlsRouteStatusControllerName)
}

func AddReferenceGrantSyncer(_ context.Context, virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string) error {
	return addToHostSyncer(virtMgr, &toHostReconciler[*gatewayv1.ReferenceGrant]{
		Context:      newGatewayContext(clusterName, clusterNamespace, virtMgr, hostMgr),
		finalizer:    referenceGrantFinalizerName,
		syncEnabled:  func(s *v1beta1.SyncConfig) (bool, map[string]string) { return s.ReferenceGrants.Enabled, s.ReferenceGrants.Selector },
		translateObj: translateReferenceGrant,
		newObj:       func() *gatewayv1.ReferenceGrant { return &gatewayv1.ReferenceGrant{} },
	}, referenceGrantControllerName)
}

func AddBackendTLSPolicySyncer(_ context.Context, virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string) error {
	return addToHostSyncer(virtMgr, &toHostReconciler[*gatewayv1.BackendTLSPolicy]{
		Context:      newGatewayContext(clusterName, clusterNamespace, virtMgr, hostMgr),
		finalizer:    backendTLSPolicyFinalizerName,
		syncEnabled:  func(s *v1beta1.SyncConfig) (bool, map[string]string) { return s.BackendTLSPolicies.Enabled, s.BackendTLSPolicies.Selector },
		translateObj: translateBackendTLSPolicy,
		newObj:       func() *gatewayv1.BackendTLSPolicy { return &gatewayv1.BackendTLSPolicy{} },
	}, backendTLSPolicyControllerName)
}

func AddBackendTLSPolicyStatusSyncer(_ context.Context, virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string) error {
	return addStatusSyncer(hostMgr, &statusReconciler[*gatewayv1.BackendTLSPolicy]{
		Context: newGatewayContext(clusterName, clusterNamespace, virtMgr, hostMgr),
		kind:    "backendtlspolicy",
		newObj:  func() *gatewayv1.BackendTLSPolicy { return &gatewayv1.BackendTLSPolicy{} },
	}, backendTLSPolicyStatusControllerName)
}
