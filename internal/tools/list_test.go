package tools

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
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
