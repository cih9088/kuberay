package scheduler_plugins

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	scheudlingPplugin "sigs.k8s.io/scheduler-plugins/apis/scheduling"
	schedulingPluginv1alpha "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	quotav1 "k8s.io/apiserver/pkg/quota/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	schedulerinterface "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/interface"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

const (
	PodGroupName      = "podgroups.scheduling.volcano.sh"
	QueueNameLabelKey = "volcano.sh/queue-name"
)

type PluginScheduler struct {
	dynClient dynamic.Interface
	log       logr.Logger
}

type SchedulerPluginsSchedulerFactory struct{}

func getGroupVersionResource() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    scheudlingPplugin.GroupName,
		Version:  "v1alpha1",
		Resource: "podgroups",
	}
}

func GetPluginName() string {
	return "scheduler-plugins-scheduler"
}

func (v *PluginScheduler) Name() string {
	return GetPluginName()
}

func (v *PluginScheduler) DoBatchSchedulingOnSubmission(
	ctx context.Context,
	app *rayv1.RayCluster,
) error {
	var minMember int32
	var totalResource corev1.ResourceList
	if !utils.IsAutoscalingEnabled(&app.Spec) {
		minMember = utils.CalculateDesiredReplicas(ctx, app) + 1
		totalResource = utils.CalculateDesiredResources(app)
	} else {
		minMember = utils.CalculateMinReplicas(app) + 1
		totalResource = utils.CalculateMinResources(app)
	}

	return v.syncPodGroup(ctx, app, minMember, totalResource)
}

func getAppPodGroupName(app *rayv1.RayCluster) string {
	return fmt.Sprintf("ray-%s-pg", app.Name)
}

func (v *PluginScheduler) syncPodGroup(
	ctx context.Context,
	app *rayv1.RayCluster,
	size int32,
	totalResource corev1.ResourceList,
) error {
	podGroupName := getAppPodGroupName(app)
	if pgUnstructured, err := v.dynClient.Resource(getGroupVersionResource()).Namespace(app.Namespace).Get(ctx, podGroupName, metav1.GetOptions{}); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}

		pg := createPodGroup(app, podGroupName, size, totalResource)
		pgUnstructuredContent, err := runtime.DefaultUnstructuredConverter.ToUnstructured(
			pg,
		)
		if err != nil {
			return err
		}
		pgUnstructured = &unstructured.Unstructured{}
		pgUnstructured.SetUnstructuredContent(pgUnstructuredContent)

		if _, err := v.dynClient.Resource(getGroupVersionResource()).Namespace(app.Namespace).Create(
			ctx, pgUnstructured, metav1.CreateOptions{},
		); err != nil {
			if errors.IsAlreadyExists(err) {
				v.log.Info("pod group already exists, no need to create")
				return nil
			}

			v.log.Error(err, "Pod group CREATE error!", "PodGroup.Error", err)
			return err
		}
	} else {
		var pg schedulingPluginv1alpha.PodGroup
		err = runtime.DefaultUnstructuredConverter.
			FromUnstructured(pgUnstructured.UnstructuredContent(), &pg)

		if pg.Spec.MinMember != size || !quotav1.Equals(pg.Spec.MinResources, totalResource) {
			pg.Spec.MinMember = size
			pg.Spec.MinResources = totalResource
			pgUnstructuredContent, err := runtime.DefaultUnstructuredConverter.ToUnstructured(
				pg,
			)
			if err != nil {
				return err
			}
			pgUnstructured = &unstructured.Unstructured{}
			pgUnstructured.SetUnstructuredContent(pgUnstructuredContent)

			if _, err := v.dynClient.Resource(getGroupVersionResource()).Namespace(app.Namespace).Update(
				ctx, pgUnstructured, metav1.UpdateOptions{},
			); err != nil {
				v.log.Error(err, "Pod group UPDATE error!", "podGroup", podGroupName)
				return err
			}
		}
	}
	return nil
}

func createPodGroup(
	app *rayv1.RayCluster,
	podGroupName string,
	size int32,
	totalResource corev1.ResourceList,
) *schedulingPluginv1alpha.PodGroup {
	return &schedulingPluginv1alpha.PodGroup{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodGroup",
			APIVersion: "scheduling.x-k8s.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: app.Namespace,
			Name:      podGroupName,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(app, rayv1.SchemeGroupVersion.WithKind("RayCluster")),
			},
		},
		Spec: schedulingPluginv1alpha.PodGroupSpec{
			MinMember:    size,
			MinResources: totalResource,
		},
	}
}

func (v *PluginScheduler) AddMetadataToPod(
	_ context.Context,
	app *rayv1.RayCluster,
	groupName string,
	pod *corev1.Pod,
) {
	pod.Annotations[schedulingPluginv1alpha.PodGroupLabel] = getAppPodGroupName(app)
	pod.Spec.SchedulerName = v.Name()
}

func (pf *SchedulerPluginsSchedulerFactory) New(
	ctx context.Context,
	config *rest.Config,
) (schedulerinterface.BatchScheduler, error) {
	dynClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize client with error %w", err)
	}

	return &PluginScheduler{
		dynClient: dynClient,
		log:       logf.Log.WithName("scheduler_plugins"),
	}, nil
}

func (pf *SchedulerPluginsSchedulerFactory) AddToScheme(scheme *runtime.Scheme) {
	utilruntime.Must(schedulingPluginv1alpha.AddToScheme(scheme))
}

func (pf *SchedulerPluginsSchedulerFactory) ConfigureReconciler(
	b *builder.Builder,
) *builder.Builder {
	return b.Owns(&schedulingPluginv1alpha.PodGroup{})
}
