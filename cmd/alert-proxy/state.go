package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

const (
	currentSchemaVersion = 1
	stateDataKey         = "state.json"
	maxStateBytes        = 900 * 1024
)

type State struct {
	SchemaVersion      int                            `json:"schemaVersion"`
	Mode               string                         `json:"mode"`
	Groups             map[string]*GroupState         `json:"groups"`
	SilenceRefs        map[string]*SilenceRef         `json:"silenceRefs"`
	SilenceOperations  map[string]*SilenceOperation   `json:"silenceOperations"`
	Outbox             map[string]*OutboxWork         `json:"outbox"`
	Requests           map[string]*RequestReservation `json:"requests"`
	NextOutboxSequence int64                          `json:"nextOutboxSequence"`
}

type AlertSnapshot struct {
	Fingerprint  string            `json:"fingerprint"`
	Status       string            `json:"status"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt,omitempty"`
	GeneratorURL string            `json:"generatorURL,omitempty"`
}

type KnownMember struct {
	LastExplicitStatus string    `json:"lastExplicitStatus"`
	LastExplicitAt     time.Time `json:"lastExplicitAt"`
}

type AckState struct {
	Actor string    `json:"actor"`
	At    time.Time `json:"at"`
}

type EpisodeSummary struct {
	EpisodeID         string    `json:"episodeID"`
	StartedAt         time.Time `json:"startedAt"`
	ClosedAt          time.Time `json:"closedAt"`
	ClosedReason      string    `json:"closedReason"`
	NotificationCount int       `json:"notificationCount"`
	Ack               *AckState `json:"ack,omitempty"`
}

type SlackObject struct {
	PostID          string    `json:"postID"`
	MessageTS       string    `json:"messageTS,omitempty"`
	DesiredRevision int64     `json:"desiredRevision"`
	AppliedRevision int64     `json:"appliedRevision"`
	Phase           string    `json:"phase"`
	LastAttemptAt   time.Time `json:"lastAttemptAt,omitempty"`
	LastError       string    `json:"lastError,omitempty"`
}

type DeliveryRecord struct {
	ProcessedAt time.Time `json:"processedAt"`
}

type GroupState struct {
	Source                        string                    `json:"source"`
	Receiver                      string                    `json:"receiver"`
	GroupKey                      string                    `json:"groupKey"`
	GroupLabels                   map[string]string         `json:"groupLabels"`
	ExternalURL                   string                    `json:"externalURL,omitempty"`
	Channel                       string                    `json:"channel"`
	ShortID                       string                    `json:"shortID"`
	AlertShortIDs                 map[string]string         `json:"alertShortIDs,omitempty"`
	LatestBatch                   []AlertSnapshot           `json:"latestBatch"`
	KnownMembers                  map[string]KnownMember    `json:"knownMembers"`
	PreviousDeliveredFingerprints []string                  `json:"previousDeliveredFingerprints"`
	EpisodeID                     string                    `json:"episodeID"`
	EpisodeStartedAt              time.Time                 `json:"episodeStartedAt"`
	FirstSeen                     time.Time                 `json:"firstSeen"`
	LastSeen                      time.Time                 `json:"lastSeen"`
	NotificationCount             int                       `json:"notificationCount"`
	Status                        string                    `json:"status"`
	ClosedAt                      time.Time                 `json:"closedAt,omitempty"`
	ClosedReason                  string                    `json:"closedReason,omitempty"`
	EscalatedAt                   time.Time                 `json:"escalatedAt,omitempty"`
	Ack                           *AckState                 `json:"ack,omitempty"`
	ScheduledSilence              *SilencePresentation      `json:"scheduledSilence,omitempty"`
	PreviousEpisode               *EpisodeSummary           `json:"previousEpisode,omitempty"`
	Parent                        *SlackObject              `json:"parent,omitempty"`
	MemberList                    *SlackObject              `json:"memberList,omitempty"`
	RecentDeliveries              map[string]DeliveryRecord `json:"recentDeliveries"`
}

type SilencePresentation struct {
	Actor     string    `json:"actor"`
	Matchers  []Matcher `json:"matchers"`
	StartsAt  time.Time `json:"startsAt"`
	EndsAt    time.Time `json:"endsAt"`
	Operation string    `json:"operation"`
}

type SilenceOrigin struct {
	GroupKey  string `json:"groupKey,omitempty"`
	EpisodeID string `json:"episodeID,omitempty"`
}

type SilenceRef struct {
	SilenceID         string         `json:"silenceID,omitempty"`
	ShortID           string         `json:"shortID"`
	Channel           string         `json:"channel,omitempty"`
	ThreadTS          string         `json:"threadTS,omitempty"`
	ParentPostID      string         `json:"parentPostID,omitempty"`
	CanonicalMatchers []Matcher      `json:"canonicalMatchers"`
	Source            string         `json:"source"`
	Generation        int64          `json:"generation"`
	Origin            *SilenceOrigin `json:"origin,omitempty"`
	Actor             string         `json:"actor"`
	Reason            string         `json:"reason"`
	CreatedAt         time.Time      `json:"createdAt"`
	CanonicalStartsAt time.Time      `json:"canonicalStartsAt,omitempty"`
	CanonicalEndsAt   time.Time      `json:"canonicalEndsAt,omitempty"`
	LastObservedState string         `json:"lastObservedState,omitempty"`
	LastObservedAt    time.Time      `json:"lastObservedAt,omitempty"`
	LastOperationID   string         `json:"lastOperationID"`
	AuditPostID       string         `json:"auditPostID,omitempty"`
	AuditMessageTS    string         `json:"auditMessageTS,omitempty"`
}

type SilenceOperation struct {
	Action              string         `json:"action"`
	OwnershipID         string         `json:"ownershipID"`
	OwnershipGeneration int64          `json:"ownershipGeneration"`
	ReplacesOwnershipID string         `json:"replacesOwnershipID,omitempty"`
	Actor               string         `json:"actor"`
	Reason              string         `json:"reason"`
	RequestedAt         time.Time      `json:"requestedAt"`
	DeadlineAt          time.Time      `json:"deadlineAt"`
	RequestedMatchers   []Matcher      `json:"requestedMatchers,omitempty"`
	RequestedEndsAt     time.Time      `json:"requestedEndsAt,omitempty"`
	Source              string         `json:"source"`
	Origin              *SilenceOrigin `json:"origin,omitempty"`
	ReplyTarget         *OutboxTarget  `json:"replyTarget,omitempty"`
	Preset              string         `json:"preset,omitempty"`
	Phase               string         `json:"phase"`
	ObservedSilenceIDs  []string       `json:"observedSilenceIDs,omitempty"`
	Attempt             int            `json:"attempt,omitempty"`
	LastAttemptAt       time.Time      `json:"lastAttemptAt,omitempty"`
	CompletedAt         time.Time      `json:"completedAt,omitempty"`
	LastError           string         `json:"lastError,omitempty"`
}

type SlackPayload struct {
	Text   string          `json:"text"`
	Blocks json.RawMessage `json:"blocks,omitempty"`
}

type OutboxTarget struct {
	Channel   string `json:"channel"`
	MessageTS string `json:"messageTS,omitempty"`
	ThreadTS  string `json:"threadTS,omitempty"`
}

type OutboxWork struct {
	Sequence         int64         `json:"sequence"`
	GroupKey         string        `json:"groupKey,omitempty"`
	Object           string        `json:"object"`
	Kind             string        `json:"kind"`
	DesiredRevision  int64         `json:"desiredRevision"`
	Target           OutboxTarget  `json:"target"`
	DependsOnPostID  string        `json:"dependsOnPostID,omitempty"`
	ImmutablePayload *SlackPayload `json:"immutablePayload,omitempty"`
	ProbeDelivery    bool          `json:"probeDelivery,omitempty"`
	AuditFallback    *OutboxTarget `json:"auditFallback,omitempty"`
	AuditDependsOn   string        `json:"auditDependsOn,omitempty"`
	Phase            string        `json:"phase"`
	Attempt          int           `json:"attempt"`
	LastAttemptAt    time.Time     `json:"lastAttemptAt,omitempty"`
	LastError        string        `json:"lastError,omitempty"`
}

type RequestReservation struct {
	Kind        string    `json:"kind"`
	OperationID string    `json:"operationID,omitempty"`
	Phase       string    `json:"phase"`
	ReservedAt  time.Time `json:"reservedAt"`
	CompletedAt time.Time `json:"completedAt,omitempty"`
	ExpiresAt   time.Time `json:"expiresAt"`
}

func newState() *State {
	return &State{SchemaVersion: currentSchemaVersion, Mode: "active", Groups: map[string]*GroupState{}, SilenceRefs: map[string]*SilenceRef{}, SilenceOperations: map[string]*SilenceOperation{}, Outbox: map[string]*OutboxWork{}, Requests: map[string]*RequestReservation{}}
}

func normalizeState(s *State) error {
	if s.SchemaVersion == 0 {
		return errors.New("state schemaVersion is required")
	}
	if s.SchemaVersion > currentSchemaVersion {
		return fmt.Errorf("state schema version %d is newer than supported version %d", s.SchemaVersion, currentSchemaVersion)
	}
	if s.SchemaVersion < currentSchemaVersion {
		return fmt.Errorf("state schema version %d has no migration to version %d", s.SchemaVersion, currentSchemaVersion)
	}
	if s.Mode != "active" && s.Mode != "draining" {
		return fmt.Errorf("invalid state mode %q", s.Mode)
	}
	if s.Groups == nil {
		s.Groups = map[string]*GroupState{}
	}
	if s.SilenceRefs == nil {
		s.SilenceRefs = map[string]*SilenceRef{}
	}
	if s.SilenceOperations == nil {
		s.SilenceOperations = map[string]*SilenceOperation{}
	}
	if s.Outbox == nil {
		s.Outbox = map[string]*OutboxWork{}
	}
	if s.Requests == nil {
		s.Requests = map[string]*RequestReservation{}
	}
	for key, g := range s.Groups {
		if g == nil || g.GroupKey == "" || g.GroupKey != key || g.EpisodeID == "" || g.ShortID == "" {
			return fmt.Errorf("group %q is malformed", key)
		}
		if g.ClosedAt.IsZero() && g.Parent == nil {
			return fmt.Errorf("open group %q has no parent Slack object", key)
		}
		if g.KnownMembers == nil {
			g.KnownMembers = map[string]KnownMember{}
		}
		if g.AlertShortIDs == nil {
			g.AlertShortIDs = map[string]string{}
		}
		if g.RecentDeliveries == nil {
			g.RecentDeliveries = map[string]DeliveryRecord{}
		}
	}
	for id, work := range s.Outbox {
		if work == nil || (work.Kind != "post" && work.Kind != "update" && work.Kind != "reaction") || (work.Phase != "pending" && work.Phase != "inFlight" && work.Phase != "failed") {
			return fmt.Errorf("outbox work %q is malformed", id)
		}
	}
	for id, operation := range s.SilenceOperations {
		if operation == nil || operation.OwnershipID == "" || (operation.Phase != "prepared" && operation.Phase != "submitted" && operation.Phase != "ambiguous" && operation.Phase != "completed" && operation.Phase != "failed") {
			return fmt.Errorf("silence operation %q is malformed", id)
		}
	}
	return nil
}

func cloneState(s *State) (*State, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	var out State
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return &out, normalizeState(&out)
}

type StateStore interface {
	Read(context.Context) (*State, error)
	Update(context.Context, func(*State) error) error
}

type ConfigMapStateStore struct {
	client          kubernetes.Interface
	namespace, name string
	metrics         *Metrics
}

func NewConfigMapStateStore(client kubernetes.Interface, namespace, name string, metrics *Metrics) *ConfigMapStateStore {
	return &ConfigMapStateStore{client: client, namespace: namespace, name: name, metrics: metrics}
}

func decodeConfigMap(cm *corev1.ConfigMap) (*State, error) {
	raw := cm.Data[stateDataKey]
	if raw == "" {
		return nil, errors.New("state ConfigMap has no state.json document")
	}
	var s State
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return nil, fmt.Errorf("decode state: %w", err)
	}
	if err := normalizeState(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

func (s *ConfigMapStateStore) Read(ctx context.Context) (*State, error) {
	cm, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return decodeConfigMap(cm)
}

func (s *ConfigMapStateStore) Update(ctx context.Context, mutate func(*State) error) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cms := s.client.CoreV1().ConfigMaps(s.namespace)
		cm, err := cms.Get(ctx, s.name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		state, err := decodeConfigMap(cm)
		if err != nil {
			return err
		}
		if err := mutate(state); err != nil {
			return err
		}
		if err := normalizeState(state); err != nil {
			return err
		}
		raw, err := json.Marshal(state)
		if err != nil {
			return fmt.Errorf("encode state: %w", err)
		}
		if len(raw) > maxStateBytes {
			return fmt.Errorf("state is %d bytes, exceeds %d-byte safety limit", len(raw), maxStateBytes)
		}
		cm.Data = map[string]string{stateDataKey: string(raw)}
		_, err = cms.Update(ctx, cm, metav1.UpdateOptions{})
		if err == nil && s.metrics != nil {
			s.metrics.stateBytes.Set(float64(len(raw)))
		}
		return err
	})
	if err != nil && s.metrics != nil {
		s.metrics.statePersistErrors.Inc()
	}
	return err
}

type MemoryStateStore struct {
	mu    sync.Mutex
	state *State
	fail  error
}

func NewMemoryStateStore() *MemoryStateStore { return &MemoryStateStore{state: newState()} }
func (s *MemoryStateStore) Read(_ context.Context) (*State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return nil, s.fail
	}
	return cloneState(s.state)
}
func (s *MemoryStateStore) Update(_ context.Context, mutate func(*State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return s.fail
	}
	c, err := cloneState(s.state)
	if err != nil {
		return err
	}
	if err := mutate(c); err != nil {
		return err
	}
	if err := normalizeState(c); err != nil {
		return err
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if len(raw) > maxStateBytes {
		return fmt.Errorf("state exceeds size limit")
	}
	s.state = c
	return nil
}

func pruneState(s *State, now time.Time, deliveryTTL, requestTTL, auditTTL time.Duration) {
	for _, g := range s.Groups {
		for id, d := range g.RecentDeliveries {
			if now.Sub(d.ProcessedAt) > deliveryTTL {
				delete(g.RecentDeliveries, id)
			}
		}
	}
	for id, r := range s.Requests {
		if now.After(r.ExpiresAt) || now.Sub(r.ReservedAt) > requestTTL {
			delete(s.Requests, id)
		}
	}
	for id, op := range s.SilenceOperations {
		if (op.Phase == "completed" || op.Phase == "failed") && !op.CompletedAt.IsZero() && now.Sub(op.CompletedAt) > auditTTL {
			delete(s.SilenceOperations, id)
		}
	}
	for id, ref := range s.SilenceRefs {
		if (ref.LastObservedState == "expired" || ref.LastObservedState == "deleted") && now.Sub(ref.LastObservedAt) > auditTTL {
			delete(s.SilenceRefs, id)
		}
	}
}

func hasLiveSilenceWork(s *State) bool {
	for _, op := range s.SilenceOperations {
		if op.Phase != "completed" && op.Phase != "failed" {
			return true
		}
	}
	for _, ref := range s.SilenceRefs {
		if ref.LastObservedState == "active" || ref.LastObservedState == "pending" {
			return true
		}
	}
	return false
}

func hasNonTerminalSilenceOperation(s *State) bool {
	for _, op := range s.SilenceOperations {
		if op.Phase != "completed" && op.Phase != "failed" {
			return true
		}
	}
	return false
}

func nonTerminalOperationFor(s *State, ownershipIDs ...string) bool {
	wanted := map[string]bool{}
	for _, id := range ownershipIDs {
		if id != "" {
			wanted[id] = true
		}
	}
	for _, op := range s.SilenceOperations {
		if op.Phase == "completed" || op.Phase == "failed" {
			continue
		}
		if wanted[op.OwnershipID] || wanted[op.ReplacesOwnershipID] {
			return true
		}
	}
	return false
}

func putOutbox(s *State, id string, work *OutboxWork) {
	if existing := s.Outbox[id]; existing != nil {
		work.Sequence = existing.Sequence
	} else {
		s.NextOutboxSequence++
		work.Sequence = s.NextOutboxSequence
	}
	s.Outbox[id] = work
}

func sortedOutboxIDs(s *State) []string {
	ids := make([]string, 0, len(s.Outbox))
	for id := range s.Outbox {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		left, right := s.Outbox[ids[i]], s.Outbox[ids[j]]
		if left.Sequence == right.Sequence {
			return ids[i] < ids[j]
		}
		return left.Sequence < right.Sequence
	})
	return ids
}
