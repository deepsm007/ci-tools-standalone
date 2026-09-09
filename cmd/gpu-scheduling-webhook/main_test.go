package main

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMutatePod(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		pod     *corev1.Pod
		wantPod corev1.Pod
	}{
		{
			name: "Request a GPU therefore add toleration",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "c1",
							Command: []string{"cmd"},
							Image:   "img",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									nvidiaGPUResource: resource.MustParse("1"),
								},
								Limits: corev1.ResourceList{
									nvidiaGPUResource: resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			wantPod: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "c1",
							Command: []string{"cmd"},
							Image:   "img",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									nvidiaGPUResource: resource.MustParse("1"),
								},
								Limits: corev1.ResourceList{
									nvidiaGPUResource: resource.MustParse("1"),
								},
							},
						},
					},
					Tolerations: []corev1.Toleration{nvidiaGPUToleration},
				},
			},
		},
		{
			name: "No GPU request a GPU, leave pod untouched",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:    "c1",
					Command: []string{"cmd"},
					Image:   "img",
				}}},
			},
			wantPod: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:    "c1",
					Command: []string{"cmd"},
					Image:   "img",
				}}},
			},
		},
		{
			name: "Do not add the same toleration again",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "c1",
							Command: []string{"cmd"},
							Image:   "img",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									nvidiaGPUResource: resource.MustParse("1"),
								},
								Limits: corev1.ResourceList{
									nvidiaGPUResource: resource.MustParse("1"),
								},
							},
						},
					},
					Tolerations: []corev1.Toleration{nvidiaGPUToleration},
				},
			},
			wantPod: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "c1",
							Command: []string{"cmd"},
							Image:   "img",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									nvidiaGPUResource: resource.MustParse("1"),
								},
								Limits: corev1.ResourceList{
									nvidiaGPUResource: resource.MustParse("1"),
								},
							},
						},
					},
					Tolerations: []corev1.Toleration{nvidiaGPUToleration},
				},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			pod := testCase.pod.DeepCopy()
			pgs := gpuTolerator{}
			if err := pgs.Default(context.TODO(), pod); err != nil {
				t.Fatalf("Default() error = %v", err)
			}
			if diff := cmp.Diff(testCase.wantPod, *pod); diff != "" {
				t.Error(diff)
			}
		})
	}
}
