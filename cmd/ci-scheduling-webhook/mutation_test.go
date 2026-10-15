package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)

// patchOp represents a single JSON Patch (RFC 6902) operation returned
// in the webhook response.
type patchOp struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value"`
}

func TestMain(m *testing.M) {
	// Register admission and core types so the webhook codecs can
	// deserialise AdmissionReview and Pod objects in test requests.
	scheme := runtime.NewScheme()
	_ = admissionv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	codecs = serializer.NewCodecFactory(scheme)

	// Create arch-specific RuntimeClass objects for s390x and ppc64le
	// so the webhook discovers them via the informer cache.
	var runtimeClassObjects []runtime.Object
	for _, arch := range []string{"s390x", "ppc64le"} {
		for _, class := range []PodClass{PodClassBuilds, PodClassTests, PodClassLongTests, PodClassProwJobs} {
			runtimeClassObjects = append(runtimeClassObjects, &nodev1.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("ci-scheduler-runtime-%s-%s", class, arch),
				},
				Handler: "crun",
			})
		}
	}

	fakeClient := fake.NewSimpleClientset(runtimeClassObjects...)
	factory := informers.NewSharedInformerFactory(fakeClient, 0)

	// Initialise nodesInformer so findHostnamesToPreclude does not panic.
	nodesInformer = factory.Core().V1().Nodes().Informer()
	_ = nodesInformer.AddIndexers(cache.Indexers{
		IndexNodesByCiWorkload: func(obj interface{}) ([]string, error) {
			node := obj.(*corev1.Node)
			workloads := []string{""}
			if workload, ok := node.Labels[CiWorkloadLabelName]; ok {
				workloads = []string{workload}
			}
			return workloads, nil
		},
	})

	// Initialise runtimeClassInformer so archSpecificRuntimeClassExists
	// can look up the arch-specific RuntimeClasses created above.
	runtimeClassInformer = factory.Node().V1().RuntimeClasses().Informer()

	stopCh := make(chan struct{})
	factory.Start(stopCh)
	factory.WaitForCacheSync(stopCh)

	os.Exit(m.Run())
}

// callMutatePod builds an AdmissionReview wrapping the given pod,
// POSTs it to the mutatePod HTTP handler via httptest, and returns
// the parsed JSON Patch operations from the webhook response.
func callMutatePod(t *testing.T, pod corev1.Pod, namespace, podName string) []patchOp {
	t.Helper()

	// Set TypeMeta so the codec recognises the object GVK.
	pod.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}

	rawPod, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("marshal pod: %v", err)
	}

	review := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "admission.k8s.io/v1",
			Kind:       "AdmissionReview",
		},
		Request: &admissionv1.AdmissionRequest{
			UID: "test-uid",
			Resource: metav1.GroupVersionResource{
				Group: "", Version: "v1", Resource: "pods",
			},
			Namespace: namespace,
			Name:      podName,
			Object:    runtime.RawExtension{Raw: rawPod},
		},
	}

	body, err := json.Marshal(review)
	if err != nil {
		t.Fatalf("marshal admission review: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/mutate", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mutatePod(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp admissionv1.AdmissionReview
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Response == nil {
		t.Fatal("response is nil")
	}
	if !resp.Response.Allowed {
		t.Fatal("expected Allowed=true")
	}

	var patches []patchOp
	if len(resp.Response.Patch) > 0 {
		if err := json.Unmarshal(resp.Response.Patch, &patches); err != nil {
			t.Fatalf("unmarshal patches: %v", err)
		}
	}
	return patches
}

// --------------- assertion helpers ---------------

func findPatches(patches []patchOp, path string) []patchOp {
	var out []patchOp
	for _, p := range patches {
		if p.Path == path {
			out = append(out, p)
		}
	}
	return out
}

func assertNoPatch(t *testing.T, patches []patchOp, path string) {
	t.Helper()
	if ps := findPatches(patches, path); len(ps) > 0 {
		t.Errorf("unexpected patch at %q: %+v", path, ps)
	}
}

func assertPatchExists(t *testing.T, patches []patchOp, path string) patchOp {
	t.Helper()
	ps := findPatches(patches, path)
	if len(ps) == 0 {
		t.Fatalf("expected patch at %q, found none; all patches: %+v", path, patches)
	}
	return ps[len(ps)-1]
}

func assertPatchStringValue(t *testing.T, patches []patchOp, path, want string) { //nolint:unparam // path is a general-purpose parameter
	t.Helper()
	p := assertPatchExists(t, patches, path)
	got, ok := p.Value.(string)
	if !ok {
		t.Fatalf("patch %q value is %T, want string", path, p.Value)
	}
	if got != want {
		t.Errorf("patch %q = %q, want %q", path, got, want)
	}
}

func nodeSelectorMap(t *testing.T, patches []patchOp) map[string]interface{} {
	t.Helper()
	p := assertPatchExists(t, patches, "/spec/nodeSelector")
	m, ok := p.Value.(map[string]interface{})
	if !ok {
		t.Fatalf("nodeSelector value is %T, want map[string]interface{}", p.Value)
	}
	return m
}

func labelsMap(t *testing.T, patches []patchOp) map[string]interface{} {
	t.Helper()
	p := assertPatchExists(t, patches, "/metadata/labels")
	m, ok := p.Value.(map[string]interface{})
	if !ok {
		t.Fatalf("labels value is %T, want map[string]interface{}", p.Value)
	}
	return m
}

func assertMapHas(t *testing.T, m map[string]interface{}, key, value, ctx string) {
	t.Helper()
	got, ok := m[key]
	if !ok {
		t.Errorf("%s: key %q absent from %v", ctx, key, m)
		return
	}
	if got != value {
		t.Errorf("%s: key %q = %v, want %q", ctx, key, got, value)
	}
}

func assertMapMissing(t *testing.T, m map[string]interface{}, key, ctx string) {
	t.Helper()
	if v, ok := m[key]; ok {
		t.Errorf("%s: key %q should be absent, got %v", ctx, key, v)
	}
}

// archNodeAffinity builds a required nodeAffinity that pins a pod to the given
// architecture(s) via kubernetes.io/arch In [...], mirroring how CI test pods
// commonly convey their target architecture (rather than via nodeSelector).
func archNodeAffinity(archs ...string) *corev1.Affinity {
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      KubernetesArchLabelName,
								Operator: corev1.NodeSelectorOpIn,
								Values:   archs,
							},
						},
					},
				},
			},
		},
	}
}

// --------------- mutation tests ---------------

func TestMutatePod(t *testing.T) {

	t.Run("s390x build pod gets arch-specific RuntimeClass", func(t *testing.T) {
		pod := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{CiBuildNameLabelName: "test-build"},
			},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{KubernetesArchLabelName: "s390x"},
				Containers:   []corev1.Container{{Name: "build"}},
			},
		}

		patches := callMutatePod(t, pod, "ci-op-xxxx", "test-pod")

		// Arch-specific RuntimeClass assigned (not the standard workload-segregated one).
		assertPatchStringValue(t, patches, "/spec/runtimeClassName", "ci-scheduler-runtime-builds-s390x")

		// kubernetes.io/arch preserved, ci-workload NOT in nodeSelector.
		ns := nodeSelectorMap(t, patches)
		assertMapHas(t, ns, KubernetesArchLabelName, "s390x", "nodeSelector")
		assertMapMissing(t, ns, CiWorkloadLabelName, "nodeSelector")

		// ci-workload label IS set for observability.
		labels := labelsMap(t, patches)
		assertMapHas(t, labels, CiWorkloadLabelName, string(PodClassBuilds), "labels")
	})

	t.Run("s390x test pod gets arch-specific RuntimeClass", func(t *testing.T) {
		pod := corev1.Pod{
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{KubernetesArchLabelName: "s390x"},
				Containers:   []corev1.Container{{Name: "test"}},
			},
		}

		patches := callMutatePod(t, pod, "ci-op-xxxx", "test-pod")

		assertPatchStringValue(t, patches, "/spec/runtimeClassName", "ci-scheduler-runtime-tests-s390x")

		ns := nodeSelectorMap(t, patches)
		assertMapHas(t, ns, KubernetesArchLabelName, "s390x", "nodeSelector")
		assertMapMissing(t, ns, CiWorkloadLabelName, "nodeSelector")

		labels := labelsMap(t, patches)
		assertMapHas(t, labels, CiWorkloadLabelName, string(PodClassTests), "labels")
	})

	t.Run("ppc64le build pod gets arch-specific RuntimeClass", func(t *testing.T) {
		pod := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{CiBuildNameLabelName: "test-build"},
			},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{KubernetesArchLabelName: "ppc64le"},
				Containers:   []corev1.Container{{Name: "build"}},
			},
		}

		patches := callMutatePod(t, pod, "ci-op-xxxx", "test-pod")

		assertPatchStringValue(t, patches, "/spec/runtimeClassName", "ci-scheduler-runtime-builds-ppc64le")

		ns := nodeSelectorMap(t, patches)
		assertMapHas(t, ns, KubernetesArchLabelName, "ppc64le", "nodeSelector")
		assertMapMissing(t, ns, CiWorkloadLabelName, "nodeSelector")

		labels := labelsMap(t, patches)
		assertMapHas(t, labels, CiWorkloadLabelName, string(PodClassBuilds), "labels")
	})

	t.Run("amd64 build pod gets standard RuntimeClass", func(t *testing.T) {
		pod := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{CiBuildNameLabelName: "test-build"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "build"}},
			},
		}

		patches := callMutatePod(t, pod, "ci-op-xxxx", "test-pod")

		// Standard RuntimeClass assigned.
		assertPatchStringValue(t, patches, "/spec/runtimeClassName", "ci-scheduler-runtime-builds")

		// ci-workload IS in nodeSelector.
		ns := nodeSelectorMap(t, patches)
		assertMapHas(t, ns, CiWorkloadLabelName, string(PodClassBuilds), "nodeSelector")
	})

	t.Run("arm64 test pod gets standard RuntimeClass", func(t *testing.T) {
		pod := corev1.Pod{
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{KubernetesArchLabelName: "arm64"},
				Containers:   []corev1.Container{{Name: "test"}},
			},
		}

		patches := callMutatePod(t, pod, "ci-op-xxxx", "test-pod")

		assertPatchStringValue(t, patches, "/spec/runtimeClassName", "ci-scheduler-runtime-tests")

		ns := nodeSelectorMap(t, patches)
		assertMapHas(t, ns, CiWorkloadLabelName, string(PodClassTests), "nodeSelector")
		assertMapHas(t, ns, KubernetesArchLabelName, "arm64", "nodeSelector")
	})

	t.Run("s390x pod still gets DNS init container", func(t *testing.T) {
		pod := corev1.Pod{
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{KubernetesArchLabelName: "s390x"},
				Containers:   []corev1.Container{{Name: "test"}},
			},
		}

		patches := callMutatePod(t, pod, "ci-op-xxxx", "test-pod")

		p := assertPatchExists(t, patches, "/spec/initContainers")
		containers, ok := p.Value.([]interface{})
		if !ok {
			t.Fatalf("initContainers value is %T, want []interface{}", p.Value)
		}
		found := false
		for _, c := range containers {
			cm, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			if cm["name"] == "ci-scheduling-dns-wait" {
				found = true
				break
			}
		}
		if !found {
			t.Error("ci-scheduling-dns-wait init container not found in patches")
		}
	})

	t.Run("s390x pod still gets labels", func(t *testing.T) {
		pod := corev1.Pod{
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{KubernetesArchLabelName: "s390x"},
				Containers:   []corev1.Container{{Name: "test"}},
			},
		}

		namespace := "ci-op-yyyy"
		patches := callMutatePod(t, pod, namespace, "test-pod")

		labels := labelsMap(t, patches)
		assertMapHas(t, labels, CiWorkloadLabelName, string(PodClassTests), "labels")
		assertMapHas(t, labels, CiWorkloadNamespaceLabelName, namespace, "labels")
	})

	t.Run("s390x build pod skips high-perf", func(t *testing.T) {
		pod := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{CiBuildNameLabelName: "test-build"},
			},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{KubernetesArchLabelName: "s390x"},
				Containers: []corev1.Container{
					{
						Name: "build",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("64Gi"),
							},
						},
					},
				},
			},
		}

		patches := callMutatePod(t, pod, "ci-op-xxxx", "test-pod")

		// No high-perf toleration patch.
		assertNoPatch(t, patches, "/spec/tolerations")

		// nodeSelector must not contain ci-instance-type.
		ns := nodeSelectorMap(t, patches)
		assertMapMissing(t, ns, "ci-instance-type", "nodeSelector")
	})

	t.Run("s390x pod skips node preclude affinity", func(t *testing.T) {
		pod := corev1.Pod{
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{KubernetesArchLabelName: "s390x"},
				Containers:   []corev1.Container{{Name: "test"}},
			},
		}

		patches := callMutatePod(t, pod, "ci-op-xxxx", "test-pod")

		// With an arch-specific RuntimeClass, affinityChanged stays false,
		// so no affinity patch (and therefore no hostname NotIn preclusion).
		assertNoPatch(t, patches, "/spec/affinity")
	})

	t.Run("prowjob pod in ci namespace unaffected", func(t *testing.T) {
		pod := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{CiCreatedByProwLabelName: "true"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "test"}},
			},
		}

		patches := callMutatePod(t, pod, CiNamepsace, "test-pod")

		// Prowjob gets standard RuntimeClass regardless of arch.
		assertPatchStringValue(t, patches, "/spec/runtimeClassName", "ci-scheduler-runtime-prowjobs")

		ns := nodeSelectorMap(t, patches)
		assertMapHas(t, ns, CiWorkloadLabelName, string(PodClassProwJobs), "nodeSelector")

		labels := labelsMap(t, patches)
		assertMapHas(t, labels, CiWorkloadLabelName, string(PodClassProwJobs), "labels")
	})

	// --- Scenario A: unknown arch falls back to generic RuntimeClass ---
	t.Run("unknown arch falls back to generic RuntimeClass", func(t *testing.T) {
		pod := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{CiBuildNameLabelName: "riscv-build"},
			},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{KubernetesArchLabelName: "riscv64"},
				Containers:   []corev1.Container{{Name: "build"}},
			},
		}

		patches := callMutatePod(t, pod, "ci-op-riscv", "riscv-pod")

		// No ci-scheduler-runtime-builds-riscv64 exists, so the webhook
		// must fall back to the standard workload-segregated RuntimeClass.
		assertPatchStringValue(t, patches, "/spec/runtimeClassName", "ci-scheduler-runtime-builds")

		// ci-workload IS in nodeSelector (generic path) and arch is preserved.
		ns := nodeSelectorMap(t, patches)
		assertMapHas(t, ns, CiWorkloadLabelName, string(PodClassBuilds), "nodeSelector")
		assertMapHas(t, ns, KubernetesArchLabelName, "riscv64", "nodeSelector")
	})

	// --- Scenario B1: amd64 build pod gets affinity with spot preference ---
	t.Run("amd64 build pod gets affinity with spot preference", func(t *testing.T) {
		pod := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{CiBuildNameLabelName: "amd64-build"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "build"}},
			},
		}

		patches := callMutatePod(t, pod, "ci-op-amd64", "amd64-build-pod")

		assertPatchStringValue(t, patches, "/spec/runtimeClassName", "ci-scheduler-runtime-builds")

		// Affinity patch must exist with spotinst.io preference.
		affinityPatch := assertPatchExists(t, patches, "/spec/affinity")
		affinityMap, ok := affinityPatch.Value.(map[string]interface{})
		if !ok {
			t.Fatalf("affinity value is %T, want map[string]interface{}", affinityPatch.Value)
		}
		nodeAffinity, ok := affinityMap["nodeAffinity"].(map[string]interface{})
		if !ok {
			t.Fatal("nodeAffinity not found in affinity patch")
		}
		preferred, ok := nodeAffinity["preferredDuringSchedulingIgnoredDuringExecution"].([]interface{})
		if !ok || len(preferred) == 0 {
			t.Fatal("preferredDuringSchedulingIgnoredDuringExecution not found or empty")
		}
		term, ok := preferred[0].(map[string]interface{})
		if !ok {
			t.Fatal("first preferred term is not a map")
		}
		pref, ok := term["preference"].(map[string]interface{})
		if !ok {
			t.Fatal("preference not found in preferred term")
		}
		matchExprs, ok := pref["matchExpressions"].([]interface{})
		if !ok || len(matchExprs) == 0 {
			t.Fatal("matchExpressions not found or empty in preference")
		}
		expr, ok := matchExprs[0].(map[string]interface{})
		if !ok {
			t.Fatal("first matchExpression is not a map")
		}
		if got := expr["key"]; got != "spotinst.io/node-lifecycle" {
			t.Errorf("spot preference key = %v, want %q", got, "spotinst.io/node-lifecycle")
		}
	})

	// --- Scenario B2: amd64 high-perf build pod gets ci-instance-type ---
	t.Run("amd64 high-perf build pod gets ci-instance-type", func(t *testing.T) {
		pod := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{CiBuildNameLabelName: "hp-build"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "build",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("64Gi"),
							},
						},
					},
				},
			},
		}

		patches := callMutatePod(t, pod, "ci-op-hp", "hp-build-pod")

		assertPatchStringValue(t, patches, "/spec/runtimeClassName", "ci-scheduler-runtime-builds")

		// patchHighPerfPod uses "replace" on /spec/nodeSelector, so the
		// last nodeSelector patch contains ci-instance-type but not
		// ci-workload (the earlier "add" patch is overwritten).
		ns := nodeSelectorMap(t, patches)
		assertMapHas(t, ns, "ci-instance-type", "high-perf", "nodeSelector")

		// Toleration patch must exist with ci-instance-type.
		tolPatch := assertPatchExists(t, patches, "/spec/tolerations")
		tolSlice, ok := tolPatch.Value.([]interface{})
		if !ok {
			t.Fatalf("tolerations value is %T, want []interface{}", tolPatch.Value)
		}
		foundTol := false
		for _, item := range tolSlice {
			tol, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			if tol["key"] == "ci-instance-type" && tol["value"] == "high-perf" {
				foundTol = true
				break
			}
		}
		if !foundTol {
			t.Error("expected toleration with key=ci-instance-type value=high-perf not found")
		}
	})

	// --- Scenario D: s390x build and test pods coexist with different RuntimeClasses ---
	t.Run("s390x build and test pods can coexist", func(t *testing.T) {
		buildPod := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{CiBuildNameLabelName: "s390x-build"},
			},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{KubernetesArchLabelName: "s390x"},
				Containers:   []corev1.Container{{Name: "build"}},
			},
		}

		testPod := corev1.Pod{
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{KubernetesArchLabelName: "s390x"},
				Containers:   []corev1.Container{{Name: "test"}},
			},
		}

		buildPatches := callMutatePod(t, buildPod, "ci-op-coexist", "s390x-build-pod")
		testPatches := callMutatePod(t, testPod, "ci-op-coexist", "s390x-test-pod")

		// Build pod gets builds-s390x, test pod gets tests-s390x.
		assertPatchStringValue(t, buildPatches, "/spec/runtimeClassName", "ci-scheduler-runtime-builds-s390x")
		assertPatchStringValue(t, testPatches, "/spec/runtimeClassName", "ci-scheduler-runtime-tests-s390x")

		// Neither gets ci-workload in nodeSelector (arch-specific RC handles scheduling).
		buildNS := nodeSelectorMap(t, buildPatches)
		assertMapMissing(t, buildNS, CiWorkloadLabelName, "build nodeSelector")

		testNS := nodeSelectorMap(t, testPatches)
		assertMapMissing(t, testNS, CiWorkloadLabelName, "test nodeSelector")
	})

	// --- Scenario E: s390x long test pod gets arch-specific RuntimeClass ---
	t.Run("s390x long test pod gets arch-specific RuntimeClass", func(t *testing.T) {
		pod := corev1.Pod{
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{KubernetesArchLabelName: "s390x"},
				Containers:   []corev1.Container{{Name: "test"}},
			},
		}

		patches := callMutatePod(t, pod, "ci-op-longtest", "release-analysis-aggregator-xxx")

		// Pod name triggers LongTests reclassification, and longtests-s390x RC exists.
		assertPatchStringValue(t, patches, "/spec/runtimeClassName", "ci-scheduler-runtime-longtests-s390x")

		// ci-workload label reflects longtests classification.
		labels := labelsMap(t, patches)
		assertMapHas(t, labels, CiWorkloadLabelName, string(PodClassLongTests), "labels")

		// No ci-workload in nodeSelector (arch-specific path).
		ns := nodeSelectorMap(t, patches)
		assertMapMissing(t, ns, CiWorkloadLabelName, "nodeSelector")
		assertMapHas(t, ns, KubernetesArchLabelName, "s390x", "nodeSelector")
	})

	// --- Scenario F: pod outside CI namespaces is not mutated for scheduling ---
	t.Run("non-CI pod with s390x arch is not mutated for scheduling", func(t *testing.T) {
		pod := corev1.Pod{
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{KubernetesArchLabelName: "s390x"},
				Containers:   []corev1.Container{{Name: "worker"}},
			},
		}

		patches := callMutatePod(t, pod, "default", "random-pod")

		// Pod in non-CI namespace gets PodClassNone => no RuntimeClass, no nodeSelector, no labels.
		assertNoPatch(t, patches, "/spec/runtimeClassName")
		assertNoPatch(t, patches, "/spec/nodeSelector")
		assertNoPatch(t, patches, "/metadata/labels")
		assertNoPatch(t, patches, "/spec/affinity")
		assertNoPatch(t, patches, "/spec/initContainers")
	})

	// --- arch conveyed via nodeAffinity (the shape real CI test pods use) ---
	t.Run("s390x test pod with arch in nodeAffinity gets arch-specific RuntimeClass", func(t *testing.T) {
		pod := corev1.Pod{
			Spec: corev1.PodSpec{
				// No kubernetes.io/arch in nodeSelector — arch is expressed
				// only via a required nodeAffinity, as real test pods do.
				Affinity:   archNodeAffinity("s390x"),
				Containers: []corev1.Container{{Name: "test"}},
			},
		}

		patches := callMutatePod(t, pod, "ci-op-xxxx", "test-pod")

		// The webhook must still route to the arch-specific RuntimeClass.
		assertPatchStringValue(t, patches, "/spec/runtimeClassName", "ci-scheduler-runtime-tests-s390x")

		// Arch-specific path: ci-workload must NOT be added to nodeSelector.
		ns := nodeSelectorMap(t, patches)
		assertMapMissing(t, ns, CiWorkloadLabelName, "nodeSelector")

		// No hostname-preclusion affinity patch on the arch-specific path.
		assertNoPatch(t, patches, "/spec/affinity")
	})

	t.Run("multi-arch nodeAffinity falls back to generic RuntimeClass", func(t *testing.T) {
		// A job that can run on more than one architecture must NOT be pinned
		// to an arch-specific RuntimeClass, even when arch RCs exist for those
		// architectures.
		pod := corev1.Pod{
			Spec: corev1.PodSpec{
				Affinity:   archNodeAffinity("s390x", "ppc64le"),
				Containers: []corev1.Container{{Name: "test"}},
			},
		}

		patches := callMutatePod(t, pod, "ci-op-xxxx", "test-pod")

		// Ambiguous arch => generic path, with ci-workload nodeSelector.
		assertPatchStringValue(t, patches, "/spec/runtimeClassName", "ci-scheduler-runtime-tests")
		ns := nodeSelectorMap(t, patches)
		assertMapHas(t, ns, CiWorkloadLabelName, string(PodClassTests), "nodeSelector")
	})

	// --- regression lock: build pod with arch in nodeSelector + no arch RC ---
	t.Run("amd64 build pod with arch in nodeSelector keeps generic path", func(t *testing.T) {
		// Real build pods carry kubernetes.io/arch in nodeSelector. With no
		// amd64 arch-specific RuntimeClass, such a pod must stay on the generic
		// path: standard RuntimeClass, ci-workload nodeSelector preserved, arch
		// preserved, and the builds-only spot-instance preference intact.
		pod := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{CiBuildNameLabelName: "amd64-build"},
			},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{KubernetesArchLabelName: "amd64"},
				Containers:   []corev1.Container{{Name: "build"}},
			},
		}

		patches := callMutatePod(t, pod, "ci-op-amd64", "amd64-build-pod")

		assertPatchStringValue(t, patches, "/spec/runtimeClassName", "ci-scheduler-runtime-builds")

		ns := nodeSelectorMap(t, patches)
		assertMapHas(t, ns, CiWorkloadLabelName, string(PodClassBuilds), "nodeSelector")
		assertMapHas(t, ns, KubernetesArchLabelName, "amd64", "nodeSelector")

		// The builds-only generic branch (spot-instance preference) must run.
		affinityPatch := assertPatchExists(t, patches, "/spec/affinity")
		affinityMap, ok := affinityPatch.Value.(map[string]interface{})
		if !ok {
			t.Fatalf("affinity value is %T, want map[string]interface{}", affinityPatch.Value)
		}
		nodeAffinity, ok := affinityMap["nodeAffinity"].(map[string]interface{})
		if !ok {
			t.Fatal("nodeAffinity not found in affinity patch")
		}
		if _, ok := nodeAffinity["preferredDuringSchedulingIgnoredDuringExecution"].([]interface{}); !ok {
			t.Error("expected spot-instance preference on the generic amd64 build path")
		}
	})
}

// --------------- node mutation tests ---------------

// callMutateNode builds an AdmissionReview wrapping the given node,
// POSTs it to the mutatePod HTTP handler (which dispatches to mutateNode
// for node resources), and returns the parsed JSON Patch operations.
func callMutateNode(t *testing.T, node corev1.Node) []patchOp {
	t.Helper()

	node.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Node"}

	rawNode, err := json.Marshal(node)
	if err != nil {
		t.Fatalf("marshal node: %v", err)
	}

	review := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "admission.k8s.io/v1",
			Kind:       "AdmissionReview",
		},
		Request: &admissionv1.AdmissionRequest{
			UID: "test-uid-node",
			Resource: metav1.GroupVersionResource{
				Group: "", Version: "v1", Resource: "nodes",
			},
			Name:   node.Name,
			Object: runtime.RawExtension{Raw: rawNode},
		},
	}

	body, err := json.Marshal(review)
	if err != nil {
		t.Fatalf("marshal admission review: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/mutate", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mutatePod(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp admissionv1.AdmissionReview
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Response == nil {
		t.Fatal("response is nil")
	}
	if !resp.Response.Allowed {
		t.Fatal("expected Allowed=true")
	}

	var patches []patchOp
	if len(resp.Response.Patch) > 0 {
		if err := json.Unmarshal(resp.Response.Patch, &patches); err != nil {
			t.Fatalf("unmarshal patches: %v", err)
		}
	}
	return patches
}

func TestMutateNode(t *testing.T) {

	t.Run("s390x CI node gets auto-tainted", func(t *testing.T) {
		node := corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "b10-s390x-test",
				Labels: map[string]string{
					CiWorkloadLabelName:     string(PodClassTests),
					KubernetesArchLabelName: "s390x",
				},
			},
		}

		patches := callMutateNode(t, node)

		// Must have scale-down-disabled annotation.
		assertPatchExists(t, patches, "/metadata/annotations/cluster-autoscaler.kubernetes.io~1scale-down-disabled")

		// Must have the ci-worker taint.
		taintPatch := assertPatchExists(t, patches, "/spec/taints")
		taintSlice, ok := taintPatch.Value.([]interface{})
		if !ok {
			t.Fatalf("taints value is %T, want []interface{}", taintPatch.Value)
		}
		foundTaint := false
		for _, item := range taintSlice {
			taint, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			if taint["key"] == CIWorkerTaintKey &&
				taint["value"] == CIWorkerTaintValue &&
				taint["effect"] == "NoSchedule" {
				foundTaint = true
				break
			}
		}
		if !foundTaint {
			t.Errorf("expected taint %s not found in patches: %+v", CIWorkerTaintKey, patches)
		}
	})

	t.Run("s390x node already tainted is not double-tainted", func(t *testing.T) {
		node := corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "b10-s390x-test",
				Labels: map[string]string{
					CiWorkloadLabelName:     string(PodClassTests),
					KubernetesArchLabelName: "s390x",
				},
				Annotations: map[string]string{
					NodeDisableScaleDownAnnotationKey: "true",
				},
			},
			Spec: corev1.NodeSpec{
				Taints: []corev1.Taint{
					{
						Key:    CIWorkerTaintKey,
						Value:  CIWorkerTaintValue,
						Effect: corev1.TaintEffectNoSchedule,
					},
				},
			},
		}

		patches := callMutateNode(t, node)

		// Both annotation and taint already present — no patches expected.
		assertNoPatch(t, patches, "/spec/taints")
		assertNoPatch(t, patches, "/spec/taints/-")
	})

	t.Run("amd64 CI node is not auto-tainted", func(t *testing.T) {
		node := corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "ip-10-28-64-10",
				Labels: map[string]string{
					CiWorkloadLabelName:     string(PodClassTests),
					KubernetesArchLabelName: "amd64",
				},
			},
		}

		patches := callMutateNode(t, node)

		// scale-down-disabled annotation is expected.
		assertPatchExists(t, patches, "/metadata/annotations/cluster-autoscaler.kubernetes.io~1scale-down-disabled")

		// No taint patch — amd64 nodes use workload-segregated taints from MachineSets.
		assertNoPatch(t, patches, "/spec/taints")
		assertNoPatch(t, patches, "/spec/taints/-")
	})

	t.Run("ppc64le CI node gets same ci-worker taint", func(t *testing.T) {
		node := corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "ppc64le-worker-01",
				Labels: map[string]string{
					CiWorkloadLabelName:     string(PodClassBuilds),
					KubernetesArchLabelName: "ppc64le",
				},
			},
		}

		patches := callMutateNode(t, node)

		taintPatch := assertPatchExists(t, patches, "/spec/taints")
		taintSlice, ok := taintPatch.Value.([]interface{})
		if !ok {
			t.Fatalf("taints value is %T, want []interface{}", taintPatch.Value)
		}
		foundTaint := false
		for _, item := range taintSlice {
			taint, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			if taint["key"] == CIWorkerTaintKey &&
				taint["value"] == CIWorkerTaintValue &&
				taint["effect"] == "NoSchedule" {
				foundTaint = true
				break
			}
		}
		if !foundTaint {
			t.Errorf("expected taint %s not found in patches: %+v", CIWorkerTaintKey, patches)
		}
	})

	t.Run("non-CI node is not tainted", func(t *testing.T) {
		node := corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "plain-worker",
				Labels: map[string]string{
					KubernetesArchLabelName: "s390x",
				},
			},
		}

		patches := callMutateNode(t, node)

		// No ci-workload label means PodClassNone — no patches at all.
		assertNoPatch(t, patches, "/spec/taints")
		assertNoPatch(t, patches, "/spec/taints/-")
		assertNoPatch(t, patches, "/metadata/annotations/cluster-autoscaler.kubernetes.io~1scale-down-disabled")
	})

	t.Run("s390x node with existing taints appends", func(t *testing.T) {
		node := corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "b10-s390x-test",
				Labels: map[string]string{
					CiWorkloadLabelName:     string(PodClassTests),
					KubernetesArchLabelName: "s390x",
				},
				Annotations: map[string]string{
					NodeDisableScaleDownAnnotationKey: "true",
				},
			},
			Spec: corev1.NodeSpec{
				Taints: []corev1.Taint{
					{
						Key:    "some-other-taint",
						Value:  "yes",
						Effect: corev1.TaintEffectNoSchedule,
					},
				},
			},
		}

		patches := callMutateNode(t, node)

		// With existing taints, should use "/spec/taints/-" (append).
		taintPatch := assertPatchExists(t, patches, "/spec/taints/-")
		taint, ok := taintPatch.Value.(map[string]interface{})
		if !ok {
			t.Fatalf("taint value is %T, want map[string]interface{}", taintPatch.Value)
		}
		if taint["key"] != CIWorkerTaintKey {
			t.Errorf("taint key = %v, want %s", taint["key"], CIWorkerTaintKey)
		}
	})

	t.Run("stale ci-worker taint removed when no arch RuntimeClass", func(t *testing.T) {
		// arm64 has no arch-specific RuntimeClass, so a node that still carries
		// the ci-worker taint (e.g. an arch RC was created then deleted) must
		// have it removed — otherwise generic-path pods, which do not tolerate
		// ci-worker, would be stranded off the node.
		node := corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "arm64-ci-node",
				Labels: map[string]string{
					CiWorkloadLabelName:     string(PodClassTests),
					KubernetesArchLabelName: "arm64",
				},
				Annotations: map[string]string{
					NodeDisableScaleDownAnnotationKey: "true",
				},
			},
			Spec: corev1.NodeSpec{
				Taints: []corev1.Taint{
					{
						Key:    CIWorkerTaintKey,
						Value:  CIWorkerTaintValue,
						Effect: corev1.TaintEffectNoSchedule,
					},
				},
			},
		}

		patches := callMutateNode(t, node)

		p := assertPatchExists(t, patches, "/spec/taints/0")
		if p.Op != "remove" {
			t.Errorf("expected remove op at /spec/taints/0, got %q", p.Op)
		}
		if p.Value != nil {
			t.Errorf("remove patch should carry no value, got %v", p.Value)
		}
	})
}

// --- Scenario C: archSpecificRuntimeClassExists unit tests ---

func TestArchSpecificRuntimeClassExists(t *testing.T) {
	tests := []struct {
		name     string
		podClass PodClass
		arch     string
		want     bool
	}{
		{
			name:     "builds-s390x exists",
			podClass: PodClassBuilds,
			arch:     "s390x",
			want:     true,
		},
		{
			name:     "tests-s390x exists",
			podClass: PodClassTests,
			arch:     "s390x",
			want:     true,
		},
		{
			name:     "builds-arm64 does not exist",
			podClass: PodClassBuilds,
			arch:     "arm64",
			want:     false,
		},
		{
			name:     "builds-riscv64 does not exist",
			podClass: PodClassBuilds,
			arch:     "riscv64",
			want:     false,
		},
		{
			name:     "builds with empty arch returns false",
			podClass: PodClassBuilds,
			arch:     "",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := archSpecificRuntimeClassExists(tt.podClass, tt.arch)
			if got != tt.want {
				t.Errorf("archSpecificRuntimeClassExists(%q, %q) = %v, want %v",
					tt.podClass, tt.arch, got, tt.want)
			}
		})
	}
}
