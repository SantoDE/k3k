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

type GatewayAPIReconciler struct {
	*Context
}

func AddGatewayAPISyncer(ctx context.Context, virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string) error {
	reconciler := GatewayAPIReconciler{
		Context: &Context{
			ClusterName:      clusterName,
			ClusterNamespace: clusterNamespace,
			VirtualClient:    virtMgr.GetClient(),
			HostClient:       hostMgr.GetClient(),
			HostReader:       hostMgr.GetAPIReader(),
			Translator: translate.ToHostTranslator{
				ClusterName:      clusterName,
				ClusterNamespace: clusterNamespace,
			},
		},
	}

	name := reconciler.Translator.TranslateName(clusterNamespace, gatewayAPIControllerName)

	return ctrl.NewControllerManagedBy(virtMgr).
		Named(name).
		For(&gatewayv1.HTTPRoute{}).
		WithEventFilter(predicate.NewPredicateFuncs(reconciler.filterResources)).
		Complete(&reconciler)
}

func (r *GatewayAPIReconciler) filterResources(object ctrlruntimeclient.Object) bool {
	var cluster v1beta1.Cluster

	ctx := context.Background()

	if err := r.HostClient.Get(ctx, types.NamespacedName{Name: r.ClusterName, Namespace: r.ClusterNamespace}, &cluster); err != nil {
		return false
	}

	syncConfig := cluster.Spec.Sync.HTTPRoutes

	if !syncConfig.Enabled {
		return object.GetDeletionTimestamp() != nil
	}

	labelSelector := labels.SelectorFromSet(syncConfig.Selector)
	if labelSelector.Empty() {
		return true
	}

	return labelSelector.Matches(labels.Set(object.GetLabels()))
}

func (r *GatewayAPIReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("cluster", r.ClusterName, "clusterNamespace", r.ClusterNamespace)
	ctx = ctrl.LoggerInto(ctx, log)

	log.Info("reconciling gateway api httproute object")

	var (
		virtHTTPRoute gatewayv1.HTTPRoute
		cluster       v1beta1.Cluster
	)

	if err := r.HostClient.Get(ctx, types.NamespacedName{Name: r.ClusterName, Namespace: r.ClusterNamespace}, &cluster); err != nil {
		return reconcile.Result{}, err
	}

	appliedSync := cluster.Spec.Sync.DeepCopy()
	if cluster.Status.Policy != nil && cluster.Status.Policy.Sync != nil {
		appliedSync = cluster.Status.Policy.Sync
	}

	syncConfig := appliedSync.HTTPRoutes

	if err := r.VirtualClient.Get(ctx, req.NamespacedName, &virtHTTPRoute); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	syncedHTTPRoute := r.httproute(&virtHTTPRoute, syncConfig)

	if err := controllerutil.SetOwnerReference(&cluster, syncedHTTPRoute, r.HostClient.Scheme()); err != nil {
		return reconcile.Result{}, err
	}

	if !virtHTTPRoute.DeletionTimestamp.IsZero() {
		if err := r.HostClient.Delete(ctx, syncedHTTPRoute); err != nil {
			return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
		}

		if controllerutil.RemoveFinalizer(&virtHTTPRoute, gatewayAPIFinalizerName) {
			if err := r.VirtualClient.Update(ctx, &virtHTTPRoute); err != nil {
				return reconcile.Result{}, err
			}
		}

		return reconcile.Result{}, nil
	}

	if controllerutil.AddFinalizer(&virtHTTPRoute, gatewayAPIFinalizerName) {
		if err := r.VirtualClient.Update(ctx, &virtHTTPRoute); err != nil {
			return reconcile.Result{}, err
		}
	}

	var hostHTTPRoute gatewayv1.HTTPRoute
	if err := r.HostReader.Get(ctx, types.NamespacedName{Name: syncedHTTPRoute.Name, Namespace: syncedHTTPRoute.Namespace}, &hostHTTPRoute); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("creating httproute on the host cluster")
			return reconcile.Result{}, r.HostClient.Create(ctx, syncedHTTPRoute)
		}

		return reconcile.Result{}, err
	}

	log.Info("updating httproute on the host cluster")

	syncedHTTPRoute.ResourceVersion = hostHTTPRoute.ResourceVersion
	return reconcile.Result{}, r.HostClient.Update(ctx, syncedHTTPRoute)
}

func (r *GatewayAPIReconciler) httproute(obj *gatewayv1.HTTPRoute, syncConfig v1beta1.GatewayAPISyncConfig) *gatewayv1.HTTPRoute {
	hostHTTPRoute := obj.DeepCopy()
	r.Translator.TranslateTo(hostHTTPRoute)

	if syncConfig.OverrideParentGateway != nil {
		ns := gatewayv1.Namespace(syncConfig.OverrideParentGateway.Namespace)
		hostHTTPRoute.Spec.ParentRefs = []gatewayv1.ParentReference{{
			Name:      gatewayv1.ObjectName(syncConfig.OverrideParentGateway.Name),
			Namespace: &ns,
		}}
	} else {
		for i := range hostHTTPRoute.Spec.ParentRefs {
			ref := &hostHTTPRoute.Spec.ParentRefs[i]
			srcNS := obj.Namespace
			if ref.Namespace != nil {
				srcNS = string(*ref.Namespace)
			}
			ref.Name = gatewayv1.ObjectName(r.Translator.TranslateName(srcNS, string(ref.Name)))
			ns := gatewayv1.Namespace(r.ClusterNamespace)
			ref.Namespace = &ns
		}
	}

	for i := range hostHTTPRoute.Spec.Rules {
		for j := range hostHTTPRoute.Spec.Rules[i].BackendRefs {
			ref := &hostHTTPRoute.Spec.Rules[i].BackendRefs[j]
			ref.Name = gatewayv1.ObjectName(r.Translator.TranslateName(obj.Namespace, string(ref.Name)))
		}
	}

	return hostHTTPRoute
}

// GatewayAPIStatusReconciler watches HTTPRoute objects on the host cluster and mirrors
// their status back to the corresponding virtual cluster HTTPRoute.
type GatewayAPIStatusReconciler struct {
	*Context
}

// AddGatewayAPIStatusSyncer registers a controller on the host manager that watches host
// HTTPRoutes and propagates their status to the matching virtual HTTPRoute.
func AddGatewayAPIStatusSyncer(ctx context.Context, virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string) error {
	reconciler := GatewayAPIStatusReconciler{
		Context: &Context{
			ClusterName:      clusterName,
			ClusterNamespace: clusterNamespace,
			VirtualClient:    virtMgr.GetClient(),
			HostClient:       hostMgr.GetClient(),
			HostReader:       hostMgr.GetAPIReader(),
			Translator: translate.ToHostTranslator{
				ClusterName:      clusterName,
				ClusterNamespace: clusterNamespace,
			},
		},
	}

	name := reconciler.Translator.TranslateName(clusterNamespace, gatewayAPIStatusControllerName)

	return ctrl.NewControllerManagedBy(hostMgr).
		Named(name).
		For(&gatewayv1.HTTPRoute{}).
		WithEventFilter(predicate.NewPredicateFuncs(reconciler.filterHostRoutes)).
		Complete(&reconciler)
}

// filterHostRoutes selects only host HTTPRoutes that belong to this virtual cluster.
// The hostMgr cache is already scoped to clusterNamespace, so namespace filtering is implicit.
func (r *GatewayAPIStatusReconciler) filterHostRoutes(obj ctrlruntimeclient.Object) bool {
	return obj.GetLabels()[translate.ClusterNameLabel] == r.ClusterName
}

func (r *GatewayAPIStatusReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("cluster", r.ClusterName, "clusterNamespace", r.ClusterNamespace)
	ctx = ctrl.LoggerInto(ctx, log)

	var hostRoute gatewayv1.HTTPRoute
	if err := r.HostClient.Get(ctx, req.NamespacedName, &hostRoute); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	annotations := hostRoute.GetAnnotations()
	virtName := annotations[translate.ResourceNameAnnotation]
	virtNamespace := annotations[translate.ResourceNamespaceAnnotation]
	if virtName == "" || virtNamespace == "" {
		return reconcile.Result{}, nil
	}

	var virtRoute gatewayv1.HTTPRoute
	if err := r.VirtualClient.Get(ctx, types.NamespacedName{Name: virtName, Namespace: virtNamespace}, &virtRoute); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	if reflect.DeepEqual(virtRoute.Status, hostRoute.Status) {
		return reconcile.Result{}, nil
	}

	log.Info("mirroring httproute status to virtual cluster", "name", virtName, "namespace", virtNamespace)
	virtRoute.Status = hostRoute.Status
	return reconcile.Result{}, r.VirtualClient.Status().Update(ctx, &virtRoute)
}

// ── TLSRoute ──────────────────────────────────────────────────────────────────

type TLSRouteReconciler struct {
	*Context
}

func AddTLSRouteSyncer(ctx context.Context, virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string) error {
	reconciler := TLSRouteReconciler{
		Context: &Context{
			ClusterName:      clusterName,
			ClusterNamespace: clusterNamespace,
			VirtualClient:    virtMgr.GetClient(),
			HostClient:       hostMgr.GetClient(),
			HostReader:       hostMgr.GetAPIReader(),
			Translator: translate.ToHostTranslator{
				ClusterName:      clusterName,
				ClusterNamespace: clusterNamespace,
			},
		},
	}
	name := reconciler.Translator.TranslateName(clusterNamespace, tlsRouteControllerName)
	return ctrl.NewControllerManagedBy(virtMgr).
		Named(name).
		For(&gatewayv1.TLSRoute{}).
		WithEventFilter(predicate.NewPredicateFuncs(reconciler.filterResources)).
		Complete(&reconciler)
}

func (r *TLSRouteReconciler) filterResources(object ctrlruntimeclient.Object) bool {
	var cluster v1beta1.Cluster
	ctx := context.Background()
	if err := r.HostClient.Get(ctx, types.NamespacedName{Name: r.ClusterName, Namespace: r.ClusterNamespace}, &cluster); err != nil {
		return false
	}
	syncConfig := cluster.Spec.Sync.TLSRoutes
	if !syncConfig.Enabled {
		return object.GetDeletionTimestamp() != nil
	}
	labelSelector := labels.SelectorFromSet(syncConfig.Selector)
	if labelSelector.Empty() {
		return true
	}
	return labelSelector.Matches(labels.Set(object.GetLabels()))
}

func (r *TLSRouteReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("cluster", r.ClusterName, "clusterNamespace", r.ClusterNamespace)
	ctx = ctrl.LoggerInto(ctx, log)

	var (
		virtTLSRoute gatewayv1.TLSRoute
		cluster      v1beta1.Cluster
	)

	if err := r.HostClient.Get(ctx, types.NamespacedName{Name: r.ClusterName, Namespace: r.ClusterNamespace}, &cluster); err != nil {
		return reconcile.Result{}, err
	}

	appliedSync := cluster.Spec.Sync.DeepCopy()
	if cluster.Status.Policy != nil && cluster.Status.Policy.Sync != nil {
		appliedSync = cluster.Status.Policy.Sync
	}

	if err := r.VirtualClient.Get(ctx, req.NamespacedName, &virtTLSRoute); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	syncedTLSRoute := r.tlsroute(&virtTLSRoute, appliedSync.TLSRoutes)

	if err := controllerutil.SetOwnerReference(&cluster, syncedTLSRoute, r.HostClient.Scheme()); err != nil {
		return reconcile.Result{}, err
	}

	if !virtTLSRoute.DeletionTimestamp.IsZero() {
		if err := r.HostClient.Delete(ctx, syncedTLSRoute); err != nil {
			return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
		}
		if controllerutil.RemoveFinalizer(&virtTLSRoute, tlsRouteFinalizerName) {
			if err := r.VirtualClient.Update(ctx, &virtTLSRoute); err != nil {
				return reconcile.Result{}, err
			}
		}
		return reconcile.Result{}, nil
	}

	if controllerutil.AddFinalizer(&virtTLSRoute, tlsRouteFinalizerName) {
		if err := r.VirtualClient.Update(ctx, &virtTLSRoute); err != nil {
			return reconcile.Result{}, err
		}
	}

	var hostTLSRoute gatewayv1.TLSRoute
	if err := r.HostReader.Get(ctx, types.NamespacedName{Name: syncedTLSRoute.Name, Namespace: syncedTLSRoute.Namespace}, &hostTLSRoute); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("creating tlsroute on the host cluster")
			return reconcile.Result{}, r.HostClient.Create(ctx, syncedTLSRoute)
		}
		return reconcile.Result{}, err
	}

	log.Info("updating tlsroute on the host cluster")
	syncedTLSRoute.ResourceVersion = hostTLSRoute.ResourceVersion
	return reconcile.Result{}, r.HostClient.Update(ctx, syncedTLSRoute)
}

func (r *TLSRouteReconciler) tlsroute(obj *gatewayv1.TLSRoute, syncConfig v1beta1.GatewayAPISyncConfig) *gatewayv1.TLSRoute {
	hostTLSRoute := obj.DeepCopy()
	r.Translator.TranslateTo(hostTLSRoute)

	if syncConfig.OverrideParentGateway != nil {
		ns := gatewayv1.Namespace(syncConfig.OverrideParentGateway.Namespace)
		hostTLSRoute.Spec.ParentRefs = []gatewayv1.ParentReference{{
			Name:      gatewayv1.ObjectName(syncConfig.OverrideParentGateway.Name),
			Namespace: &ns,
		}}
	} else {
		for i := range hostTLSRoute.Spec.ParentRefs {
			ref := &hostTLSRoute.Spec.ParentRefs[i]
			srcNS := obj.Namespace
			if ref.Namespace != nil {
				srcNS = string(*ref.Namespace)
			}
			ref.Name = gatewayv1.ObjectName(r.Translator.TranslateName(srcNS, string(ref.Name)))
			ns := gatewayv1.Namespace(r.ClusterNamespace)
			ref.Namespace = &ns
		}
	}

	for i := range hostTLSRoute.Spec.Rules {
		for j := range hostTLSRoute.Spec.Rules[i].BackendRefs {
			ref := &hostTLSRoute.Spec.Rules[i].BackendRefs[j]
			ref.Name = gatewayv1.ObjectName(r.Translator.TranslateName(obj.Namespace, string(ref.Name)))
		}
	}

	return hostTLSRoute
}

type TLSRouteStatusReconciler struct {
	*Context
}

func AddTLSRouteStatusSyncer(ctx context.Context, virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string) error {
	reconciler := TLSRouteStatusReconciler{
		Context: &Context{
			ClusterName:      clusterName,
			ClusterNamespace: clusterNamespace,
			VirtualClient:    virtMgr.GetClient(),
			HostClient:       hostMgr.GetClient(),
			HostReader:       hostMgr.GetAPIReader(),
			Translator: translate.ToHostTranslator{
				ClusterName:      clusterName,
				ClusterNamespace: clusterNamespace,
			},
		},
	}
	name := reconciler.Translator.TranslateName(clusterNamespace, tlsRouteStatusControllerName)
	return ctrl.NewControllerManagedBy(hostMgr).
		Named(name).
		For(&gatewayv1.TLSRoute{}).
		WithEventFilter(predicate.NewPredicateFuncs(reconciler.filterHostRoutes)).
		Complete(&reconciler)
}

func (r *TLSRouteStatusReconciler) filterHostRoutes(obj ctrlruntimeclient.Object) bool {
	return obj.GetLabels()[translate.ClusterNameLabel] == r.ClusterName
}

func (r *TLSRouteStatusReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("cluster", r.ClusterName, "clusterNamespace", r.ClusterNamespace)
	ctx = ctrl.LoggerInto(ctx, log)

	var hostRoute gatewayv1.TLSRoute
	if err := r.HostClient.Get(ctx, req.NamespacedName, &hostRoute); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	annotations := hostRoute.GetAnnotations()
	virtName := annotations[translate.ResourceNameAnnotation]
	virtNamespace := annotations[translate.ResourceNamespaceAnnotation]
	if virtName == "" || virtNamespace == "" {
		return reconcile.Result{}, nil
	}

	var virtRoute gatewayv1.TLSRoute
	if err := r.VirtualClient.Get(ctx, types.NamespacedName{Name: virtName, Namespace: virtNamespace}, &virtRoute); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	if reflect.DeepEqual(virtRoute.Status, hostRoute.Status) {
		return reconcile.Result{}, nil
	}

	log.Info("mirroring tlsroute status to virtual cluster", "name", virtName, "namespace", virtNamespace)
	virtRoute.Status = hostRoute.Status
	return reconcile.Result{}, r.VirtualClient.Status().Update(ctx, &virtRoute)
}

// ── ReferenceGrant ────────────────────────────────────────────────────────────

type ReferenceGrantReconciler struct {
	*Context
}

func AddReferenceGrantSyncer(ctx context.Context, virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string) error {
	reconciler := ReferenceGrantReconciler{
		Context: &Context{
			ClusterName:      clusterName,
			ClusterNamespace: clusterNamespace,
			VirtualClient:    virtMgr.GetClient(),
			HostClient:       hostMgr.GetClient(),
			HostReader:       hostMgr.GetAPIReader(),
			Translator: translate.ToHostTranslator{
				ClusterName:      clusterName,
				ClusterNamespace: clusterNamespace,
			},
		},
	}
	name := reconciler.Translator.TranslateName(clusterNamespace, referenceGrantControllerName)
	return ctrl.NewControllerManagedBy(virtMgr).
		Named(name).
		For(&gatewayv1.ReferenceGrant{}).
		WithEventFilter(predicate.NewPredicateFuncs(reconciler.filterResources)).
		Complete(&reconciler)
}

func (r *ReferenceGrantReconciler) filterResources(object ctrlruntimeclient.Object) bool {
	var cluster v1beta1.Cluster
	ctx := context.Background()
	if err := r.HostClient.Get(ctx, types.NamespacedName{Name: r.ClusterName, Namespace: r.ClusterNamespace}, &cluster); err != nil {
		return false
	}
	syncConfig := cluster.Spec.Sync.ReferenceGrants
	if !syncConfig.Enabled {
		return object.GetDeletionTimestamp() != nil
	}
	labelSelector := labels.SelectorFromSet(syncConfig.Selector)
	if labelSelector.Empty() {
		return true
	}
	return labelSelector.Matches(labels.Set(object.GetLabels()))
}

func (r *ReferenceGrantReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("cluster", r.ClusterName, "clusterNamespace", r.ClusterNamespace)
	ctx = ctrl.LoggerInto(ctx, log)

	var (
		virtGrant gatewayv1.ReferenceGrant
		cluster   v1beta1.Cluster
	)

	if err := r.HostClient.Get(ctx, types.NamespacedName{Name: r.ClusterName, Namespace: r.ClusterNamespace}, &cluster); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.VirtualClient.Get(ctx, req.NamespacedName, &virtGrant); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	syncedGrant := r.referenceGrant(&virtGrant)

	if err := controllerutil.SetOwnerReference(&cluster, syncedGrant, r.HostClient.Scheme()); err != nil {
		return reconcile.Result{}, err
	}

	if !virtGrant.DeletionTimestamp.IsZero() {
		if err := r.HostClient.Delete(ctx, syncedGrant); err != nil {
			return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
		}
		if controllerutil.RemoveFinalizer(&virtGrant, referenceGrantFinalizerName) {
			if err := r.VirtualClient.Update(ctx, &virtGrant); err != nil {
				return reconcile.Result{}, err
			}
		}
		return reconcile.Result{}, nil
	}

	if controllerutil.AddFinalizer(&virtGrant, referenceGrantFinalizerName) {
		if err := r.VirtualClient.Update(ctx, &virtGrant); err != nil {
			return reconcile.Result{}, err
		}
	}

	var hostGrant gatewayv1.ReferenceGrant
	if err := r.HostReader.Get(ctx, types.NamespacedName{Name: syncedGrant.Name, Namespace: syncedGrant.Namespace}, &hostGrant); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("creating referencegrant on the host cluster")
			return reconcile.Result{}, r.HostClient.Create(ctx, syncedGrant)
		}
		return reconcile.Result{}, err
	}

	log.Info("updating referencegrant on the host cluster")
	syncedGrant.ResourceVersion = hostGrant.ResourceVersion
	return reconcile.Result{}, r.HostClient.Update(ctx, syncedGrant)
}

func (r *ReferenceGrantReconciler) referenceGrant(obj *gatewayv1.ReferenceGrant) *gatewayv1.ReferenceGrant {
	hostGrant := obj.DeepCopy()
	r.Translator.TranslateTo(hostGrant)

	// All virtual-cluster namespaces map to clusterNamespace on the host.
	for i := range hostGrant.Spec.From {
		hostGrant.Spec.From[i].Namespace = gatewayv1.Namespace(r.ClusterNamespace)
	}

	return hostGrant
}

// ── BackendTLSPolicy ──────────────────────────────────────────────────────────

type BackendTLSPolicyReconciler struct {
	*Context
}

func AddBackendTLSPolicySyncer(ctx context.Context, virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string) error {
	reconciler := BackendTLSPolicyReconciler{
		Context: &Context{
			ClusterName:      clusterName,
			ClusterNamespace: clusterNamespace,
			VirtualClient:    virtMgr.GetClient(),
			HostClient:       hostMgr.GetClient(),
			HostReader:       hostMgr.GetAPIReader(),
			Translator: translate.ToHostTranslator{
				ClusterName:      clusterName,
				ClusterNamespace: clusterNamespace,
			},
		},
	}
	name := reconciler.Translator.TranslateName(clusterNamespace, backendTLSPolicyControllerName)
	return ctrl.NewControllerManagedBy(virtMgr).
		Named(name).
		For(&gatewayv1.BackendTLSPolicy{}).
		WithEventFilter(predicate.NewPredicateFuncs(reconciler.filterResources)).
		Complete(&reconciler)
}

func (r *BackendTLSPolicyReconciler) filterResources(object ctrlruntimeclient.Object) bool {
	var cluster v1beta1.Cluster
	ctx := context.Background()
	if err := r.HostClient.Get(ctx, types.NamespacedName{Name: r.ClusterName, Namespace: r.ClusterNamespace}, &cluster); err != nil {
		return false
	}
	syncConfig := cluster.Spec.Sync.BackendTLSPolicies
	if !syncConfig.Enabled {
		return object.GetDeletionTimestamp() != nil
	}
	labelSelector := labels.SelectorFromSet(syncConfig.Selector)
	if labelSelector.Empty() {
		return true
	}
	return labelSelector.Matches(labels.Set(object.GetLabels()))
}

func (r *BackendTLSPolicyReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("cluster", r.ClusterName, "clusterNamespace", r.ClusterNamespace)
	ctx = ctrl.LoggerInto(ctx, log)

	var (
		virtPolicy gatewayv1.BackendTLSPolicy
		cluster    v1beta1.Cluster
	)

	if err := r.HostClient.Get(ctx, types.NamespacedName{Name: r.ClusterName, Namespace: r.ClusterNamespace}, &cluster); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.VirtualClient.Get(ctx, req.NamespacedName, &virtPolicy); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	syncedPolicy := r.backendTLSPolicy(&virtPolicy)

	if err := controllerutil.SetOwnerReference(&cluster, syncedPolicy, r.HostClient.Scheme()); err != nil {
		return reconcile.Result{}, err
	}

	if !virtPolicy.DeletionTimestamp.IsZero() {
		if err := r.HostClient.Delete(ctx, syncedPolicy); err != nil {
			return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
		}
		if controllerutil.RemoveFinalizer(&virtPolicy, backendTLSPolicyFinalizerName) {
			if err := r.VirtualClient.Update(ctx, &virtPolicy); err != nil {
				return reconcile.Result{}, err
			}
		}
		return reconcile.Result{}, nil
	}

	if controllerutil.AddFinalizer(&virtPolicy, backendTLSPolicyFinalizerName) {
		if err := r.VirtualClient.Update(ctx, &virtPolicy); err != nil {
			return reconcile.Result{}, err
		}
	}

	var hostPolicy gatewayv1.BackendTLSPolicy
	if err := r.HostReader.Get(ctx, types.NamespacedName{Name: syncedPolicy.Name, Namespace: syncedPolicy.Namespace}, &hostPolicy); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("creating backendtlspolicy on the host cluster")
			return reconcile.Result{}, r.HostClient.Create(ctx, syncedPolicy)
		}
		return reconcile.Result{}, err
	}

	log.Info("updating backendtlspolicy on the host cluster")
	syncedPolicy.ResourceVersion = hostPolicy.ResourceVersion
	return reconcile.Result{}, r.HostClient.Update(ctx, syncedPolicy)
}

func (r *BackendTLSPolicyReconciler) backendTLSPolicy(obj *gatewayv1.BackendTLSPolicy) *gatewayv1.BackendTLSPolicy {
	hostPolicy := obj.DeepCopy()
	r.Translator.TranslateTo(hostPolicy)

	for i := range hostPolicy.Spec.TargetRefs {
		hostPolicy.Spec.TargetRefs[i].Name = gatewayv1.ObjectName(
			r.Translator.TranslateName(obj.Namespace, string(obj.Spec.TargetRefs[i].Name)),
		)
	}

	for i := range hostPolicy.Spec.Validation.CACertificateRefs {
		hostPolicy.Spec.Validation.CACertificateRefs[i].Name = gatewayv1.ObjectName(
			r.Translator.TranslateName(obj.Namespace, string(obj.Spec.Validation.CACertificateRefs[i].Name)),
		)
	}

	return hostPolicy
}

type BackendTLSPolicyStatusReconciler struct {
	*Context
}

func AddBackendTLSPolicyStatusSyncer(ctx context.Context, virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string) error {
	reconciler := BackendTLSPolicyStatusReconciler{
		Context: &Context{
			ClusterName:      clusterName,
			ClusterNamespace: clusterNamespace,
			VirtualClient:    virtMgr.GetClient(),
			HostClient:       hostMgr.GetClient(),
			HostReader:       hostMgr.GetAPIReader(),
			Translator: translate.ToHostTranslator{
				ClusterName:      clusterName,
				ClusterNamespace: clusterNamespace,
			},
		},
	}
	name := reconciler.Translator.TranslateName(clusterNamespace, backendTLSPolicyStatusControllerName)
	return ctrl.NewControllerManagedBy(hostMgr).
		Named(name).
		For(&gatewayv1.BackendTLSPolicy{}).
		WithEventFilter(predicate.NewPredicateFuncs(reconciler.filterHostPolicies)).
		Complete(&reconciler)
}

func (r *BackendTLSPolicyStatusReconciler) filterHostPolicies(obj ctrlruntimeclient.Object) bool {
	return obj.GetLabels()[translate.ClusterNameLabel] == r.ClusterName
}

func (r *BackendTLSPolicyStatusReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("cluster", r.ClusterName, "clusterNamespace", r.ClusterNamespace)
	ctx = ctrl.LoggerInto(ctx, log)

	var hostPolicy gatewayv1.BackendTLSPolicy
	if err := r.HostClient.Get(ctx, req.NamespacedName, &hostPolicy); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	annotations := hostPolicy.GetAnnotations()
	virtName := annotations[translate.ResourceNameAnnotation]
	virtNamespace := annotations[translate.ResourceNamespaceAnnotation]
	if virtName == "" || virtNamespace == "" {
		return reconcile.Result{}, nil
	}

	var virtPolicy gatewayv1.BackendTLSPolicy
	if err := r.VirtualClient.Get(ctx, types.NamespacedName{Name: virtName, Namespace: virtNamespace}, &virtPolicy); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	if reflect.DeepEqual(virtPolicy.Status, hostPolicy.Status) {
		return reconcile.Result{}, nil
	}

	log.Info("mirroring backendtlspolicy status to virtual cluster", "name", virtName, "namespace", virtNamespace)
	virtPolicy.Status = hostPolicy.Status
	return reconcile.Result{}, r.VirtualClient.Status().Update(ctx, &virtPolicy)
}
