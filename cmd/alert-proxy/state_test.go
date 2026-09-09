package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestConfigMapStateRejectsNewerAndMalformedSchemas(t *testing.T) {
	ctx := context.Background()
	for name, raw := range map[string]string{"newer": `{"schemaVersion":999,"mode":"active"}`, "missing": `{"mode":"active"}`, "malformed": `{`} {
		t.Run(name, func(t *testing.T) {
			client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "state", Namespace: "ci"}, Data: map[string]string{stateDataKey: raw}})
			store := NewConfigMapStateStore(client, "ci", "state", nil)
			if _, err := store.Read(ctx); err == nil {
				t.Fatal("invalid state was accepted")
			}
			before, _ := client.CoreV1().ConfigMaps("ci").Get(ctx, "state", metav1.GetOptions{})
			if before.Data[stateDataKey] != raw {
				t.Fatal("invalid state was overwritten")
			}
		})
	}
}

func TestConfigMapStateUpdatesOnePrecreatedVersionedDocument(t *testing.T) {
	ctx := context.Background()
	initial, err := json.Marshal(newState())
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "state", Namespace: "ci"}, Data: map[string]string{stateDataKey: string(initial)}})
	store := NewConfigMapStateStore(client, "ci", "state", nil)
	if err := store.Update(ctx, func(s *State) error { s.Mode = "draining"; return nil }); err != nil {
		t.Fatal(err)
	}
	cm, _ := client.CoreV1().ConfigMaps("ci").Get(ctx, "state", metav1.GetOptions{})
	if len(cm.Data) != 1 {
		t.Fatalf("data=%v", cm.Data)
	}
	var state State
	if err := json.Unmarshal([]byte(cm.Data[stateDataKey]), &state); err != nil {
		t.Fatal(err)
	}
	if state.SchemaVersion != currentSchemaVersion || state.Mode != "draining" {
		t.Fatalf("state=%#v", state)
	}
}

func TestConfigMapStateNotFoundNeverCreates(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	store := NewConfigMapStateStore(client, "ci", "state", nil)
	if err := store.Update(ctx, func(*State) error { return nil }); err == nil {
		t.Fatal("missing precreated state ConfigMap was silently created")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" {
			t.Fatalf("state store attempted create despite get/update-only RBAC: %#v", action)
		}
	}
}

func TestStateSizeLimitDoesNotPartiallyCommit(t *testing.T) {
	store := NewMemoryStateStore()
	err := store.Update(context.Background(), func(s *State) error {
		s.Groups["large"] = &GroupState{GroupKey: "large", ExternalURL: strings.Repeat("x", maxStateBytes), RecentDeliveries: map[string]DeliveryRecord{}}
		return nil
	})
	if err == nil {
		t.Fatal("oversized state committed")
	}
	state, _ := store.Read(context.Background())
	if state.Groups["large"] != nil {
		t.Fatal("oversized mutation partially committed")
	}
}

func TestConfigMapStateRetriesConflictFromFreshResourceVersion(t *testing.T) {
	ctx := context.Background()
	initial, _ := json.Marshal(newState())
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "state", Namespace: "ci"}, Data: map[string]string{stateDataKey: string(initial)}})
	updates := 0
	client.PrependReactor("update", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		updates++
		if updates == 1 {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, "state", errors.New("conflict"))
		}
		return false, nil, nil
	})
	store := NewConfigMapStateStore(client, "ci", "state", nil)
	if err := store.Update(ctx, func(s *State) error { s.Mode = "draining"; return nil }); err != nil {
		t.Fatal(err)
	}
	if updates != 2 {
		t.Fatalf("conflict was not retried exactly once: updates=%d", updates)
	}
	state, err := store.Read(ctx)
	if err != nil || state.Mode != "draining" {
		t.Fatalf("conflict retry lost mutation: state=%#v err=%v", state, err)
	}
}
