package tools

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	v1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	fakepipelineclientset "github.com/tektoncd/pipeline/pkg/client/clientset/versioned/fake"
	informers "github.com/tektoncd/pipeline/pkg/client/informers/externalversions"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

func TestListStepActions(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		lselector string
		prefix    string
		input     []*v1beta1.StepAction
		output    []*v1beta1.StepAction
	}{
		{
			name:      "No namespace, no filters",
			namespace: "",
			lselector: "",
			prefix:    "",
			input: []*v1beta1.StepAction{
				{ObjectMeta: metav1.ObjectMeta{Name: "stepaction1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "stepaction2"}},
			},
			output: []*v1beta1.StepAction{
				{ObjectMeta: metav1.ObjectMeta{Name: "stepaction1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "stepaction2"}},
			},
		},
		{
			name:      "With namespace",
			namespace: "test-namespace1",
			lselector: "",
			prefix:    "",
			input: []*v1beta1.StepAction{
				{ObjectMeta: metav1.ObjectMeta{Name: "stepaction1", Namespace: "test-namespace1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "stepaction2", Namespace: "test-namespace2"}},
			},
			output: []*v1beta1.StepAction{
				{ObjectMeta: metav1.ObjectMeta{Name: "stepaction1", Namespace: "test-namespace1"}},
			},
		},
		{
			name:      "With label selector",
			namespace: "",
			lselector: "key=value",
			prefix:    "",
			input: []*v1beta1.StepAction{
				{ObjectMeta: metav1.ObjectMeta{Name: "stepaction1", Labels: map[string]string{"key": "value"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "stepaction2"}},
			},
			output: []*v1beta1.StepAction{
				{ObjectMeta: metav1.ObjectMeta{Name: "stepaction1", Labels: map[string]string{"key": "value"}}},
			},
		},
		{
			name:      "With prefix filter",
			namespace: "",
			lselector: "",
			prefix:    "step",
			input: []*v1beta1.StepAction{
				{ObjectMeta: metav1.ObjectMeta{Name: "stepaction1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "otheraction"}},
			},
			output: []*v1beta1.StepAction{
				{ObjectMeta: metav1.ObjectMeta{Name: "stepaction1"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fakepipelineclientset.NewSimpleClientset()
			informer := informers.NewSharedInformerFactory(fakeClient, 0)
			stepactionsInformer := informer.Tekton().V1beta1().StepActions()

			for _, item := range tt.input {
				_, _ = fakeClient.TektonV1beta1().StepActions(item.Namespace).Create(t.Context(), item, metav1.CreateOptions{})
				_ = stepactionsInformer.Informer().GetIndexer().Add(item)
			}

			stopCh := make(chan struct{})
			defer close(stopCh)
			informer.Start(stopCh)
			cache.WaitForCacheSync(stopCh, stepactionsInformer.Informer().HasSynced)

			actual, err := listStepActionsHelper(stepactionsInformer, tt.namespace, tt.lselector, tt.prefix)
			require.NoError(t, err)

			var actualObjs []*v1beta1.StepAction
			_ = json.Unmarshal([]byte(actual), &actualObjs)
			require.ElementsMatch(t, tt.output, actualObjs)
		})
	}
}

func TestListTaskruns(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		lselector string
		prefix    string
		input     []*v1.TaskRun
		output    []*v1.TaskRun
	}{
		{
			name:      "No namespace, no filters",
			namespace: "",
			lselector: "",
			prefix:    "",
			input: []*v1.TaskRun{
				{ObjectMeta: metav1.ObjectMeta{Name: "taskrun1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "taskrun2"}},
			},
			output: []*v1.TaskRun{
				{ObjectMeta: metav1.ObjectMeta{Name: "taskrun1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "taskrun2"}},
			},
		},
		{
			name:      "With namespace",
			namespace: "test-namespace1",
			lselector: "",
			prefix:    "",
			input: []*v1.TaskRun{
				{ObjectMeta: metav1.ObjectMeta{Name: "taskrun1", Namespace: "test-namespace1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "taskrun2", Namespace: "test-namespace2"}},
			},
			output: []*v1.TaskRun{
				{ObjectMeta: metav1.ObjectMeta{Name: "taskrun1", Namespace: "test-namespace1"}},
			},
		},
		{
			name:      "With label selector",
			namespace: "",
			lselector: "key=value",
			prefix:    "",
			input: []*v1.TaskRun{
				{ObjectMeta: metav1.ObjectMeta{Name: "taskrun1", Labels: map[string]string{"key": "value"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "taskrun"}},
			},
			output: []*v1.TaskRun{
				{ObjectMeta: metav1.ObjectMeta{Name: "taskrun1", Labels: map[string]string{"key": "value"}}},
			},
		},
		{
			name:      "With prefix filter",
			namespace: "",
			lselector: "",
			prefix:    "task",
			input: []*v1.TaskRun{
				{ObjectMeta: metav1.ObjectMeta{Name: "taskrun1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "otherrun"}},
			},
			output: []*v1.TaskRun{
				{ObjectMeta: metav1.ObjectMeta{Name: "taskrun1"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fakepipelineclientset.NewSimpleClientset()
			informer := informers.NewSharedInformerFactory(fakeClient, 0)
			taskrunInformer := informer.Tekton().V1().TaskRuns()

			for _, item := range tt.input {
				_, _ = fakeClient.TektonV1().TaskRuns(item.Namespace).Create(t.Context(), item, metav1.CreateOptions{})
				_ = taskrunInformer.Informer().GetIndexer().Add(item)
			}

			stopCh := make(chan struct{})
			defer close(stopCh)
			informer.Start(stopCh)
			cache.WaitForCacheSync(stopCh, taskrunInformer.Informer().HasSynced)

			actual, err := listTaskRunsHelper(taskrunInformer, tt.namespace, tt.lselector, tt.prefix)
			require.NoError(t, err)

			var actualObjs []*v1.TaskRun
			_ = json.Unmarshal([]byte(actual), &actualObjs)
			require.ElementsMatch(t, tt.output, actualObjs)
		})
	}
}

func TestListPipelineruns(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		lselector string
		prefix    string
		input     []*v1.PipelineRun
		output    []*v1.PipelineRun
	}{
		{
			name:      "No namespace, no filters",
			namespace: "",
			lselector: "",
			prefix:    "",
			input: []*v1.PipelineRun{
				{ObjectMeta: metav1.ObjectMeta{Name: "pipelinerun1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pipelinerun2"}},
			},
			output: []*v1.PipelineRun{
				{ObjectMeta: metav1.ObjectMeta{Name: "pipelinerun1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pipelinerun2"}},
			},
		},
		{
			name:      "With namespace",
			namespace: "test-namespace1",
			lselector: "",
			prefix:    "",
			input: []*v1.PipelineRun{
				{ObjectMeta: metav1.ObjectMeta{Name: "pipelinerun1", Namespace: "test-namespace1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pipelinerun2", Namespace: "test-namespace2"}},
			},
			output: []*v1.PipelineRun{
				{ObjectMeta: metav1.ObjectMeta{Name: "pipelinerun1", Namespace: "test-namespace1"}},
			},
		},
		{
			name:      "With label selector",
			namespace: "",
			lselector: "key=value",
			prefix:    "",
			input: []*v1.PipelineRun{
				{ObjectMeta: metav1.ObjectMeta{Name: "pipelinerun1", Labels: map[string]string{"key": "value"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pipelinerun2"}},
			},
			output: []*v1.PipelineRun{
				{ObjectMeta: metav1.ObjectMeta{Name: "pipelinerun1", Labels: map[string]string{"key": "value"}}},
			},
		},
		{
			name:      "With prefix filter",
			namespace: "",
			lselector: "",
			prefix:    "pipeline",
			input: []*v1.PipelineRun{
				{ObjectMeta: metav1.ObjectMeta{Name: "pipelinerun1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "otherrun"}},
			},
			output: []*v1.PipelineRun{
				{ObjectMeta: metav1.ObjectMeta{Name: "pipelinerun1"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fakepipelineclientset.NewSimpleClientset()
			informer := informers.NewSharedInformerFactory(fakeClient, 0)
			pipelinerunInformer := informer.Tekton().V1().PipelineRuns()

			for _, item := range tt.input {
				_, _ = fakeClient.TektonV1().PipelineRuns(item.Namespace).Create(t.Context(), item, metav1.CreateOptions{})
				_ = pipelinerunInformer.Informer().GetIndexer().Add(item)
			}

			stopCh := make(chan struct{})
			defer close(stopCh)
			informer.Start(stopCh)
			cache.WaitForCacheSync(stopCh, pipelinerunInformer.Informer().HasSynced)

			actual, err := listPipelineruns(pipelinerunInformer, tt.namespace, tt.lselector, tt.prefix)
			require.NoError(t, err)

			var actualObjs []*v1.PipelineRun
			_ = json.Unmarshal([]byte(actual), &actualObjs)
			require.ElementsMatch(t, tt.output, actualObjs)
		})
	}
}
