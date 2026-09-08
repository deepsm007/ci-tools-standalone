package main

import (
	"context"
	"errors"
	"regexp"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func testPolicy() *SilencePolicy {
	return &SilencePolicy{Config: SilenceConfig{
		Default: 24 * time.Hour, MinDuration: 2 * time.Hour, MaxDuration: 7 * 24 * time.Hour,
		Presets:             []Preset{{Name: "2-hours", Label: "2 hours", Duration: 2 * time.Hour}, {Name: "1-day", Label: "1 day", Duration: 24 * time.Hour, Default: true}},
		NeverSilenceable:    []*regexp.Regexp{regexp.MustCompile(`.*-Down$`)},
		RequiredExactLabels: []string{"alertname"}, RequireOneExactLabelFrom: []string{"job_name", "namespace"}, MinCurrentlyMatchedAlerts: 1, MaxCurrentlyMatchedAlerts: 5,
	}}
}

func testRenderer() *Renderer {
	return &Renderer{SilencesEnabled: true, Policy: testPolicy(), Now: func() time.Time { return time.Unix(1000, 0).UTC() }}
}

type fakeSlack struct {
	mu                              sync.Mutex
	posts, updates, reactions       int
	postErr, updateErr, reactionErr error
	postTS                          string
	emptyPostTS                     bool
	onPost                          func()
	users                           map[string]string
}

func (f *fakeSlack) PostMessage(context.Context, OutboxTarget, SlackPayload) (string, error) {
	f.mu.Lock()
	f.posts++
	fn := f.onPost
	err := f.postErr
	ts := f.postTS
	f.mu.Unlock()
	if fn != nil {
		fn()
	}
	if ts == "" && !f.emptyPostTS {
		ts = "123.456"
	}
	return ts, err
}
func (f *fakeSlack) UpdateMessage(context.Context, OutboxTarget, SlackPayload) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates++
	return f.updateErr
}
func (f *fakeSlack) PostEphemeral(context.Context, string, string, string) error { return nil }
func (f *fakeSlack) OpenView(context.Context, string, any) error                 { return nil }
func (f *fakeSlack) AddReaction(context.Context, string, string, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reactions++
	return f.reactionErr
}
func (f *fakeSlack) LookupUserByEmail(_ context.Context, email string) (string, error) {
	if id := f.users[email]; id != "" {
		return id, nil
	}
	return "", errors.New("not found")
}

type fakeAM struct {
	mu                       sync.Mutex
	silences                 map[string]AMSilence
	alerts                   []AMAlert
	next                     int
	upserts, expires         int
	upsertState              string
	lastUpsert               AMSilence
	upsertErrAfterSideEffect error
	expireErrAfterSideEffect error
	listErr                  error
	onListAlerts             func()
	onListSilences           func()
}

func newFakeAM() *fakeAM { return &fakeAM{silences: map[string]AMSilence{}} }
func (f *fakeAM) ListSilences(context.Context) ([]AMSilence, error) {
	f.mu.Lock()
	if f.listErr != nil {
		f.mu.Unlock()
		return nil, f.listErr
	}
	out := make([]AMSilence, 0, len(f.silences))
	for _, s := range f.silences {
		out = append(out, s)
	}
	fn := f.onListSilences
	f.mu.Unlock()
	if fn != nil {
		fn()
	}
	return out, nil
}
func (f *fakeAM) GetSilence(_ context.Context, id string) (AMSilence, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.silences[id]
	if !ok {
		return AMSilence{}, errors.New("not found")
	}
	return s, nil
}
func (f *fakeAM) UpsertSilence(_ context.Context, s AMSilence) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.upserts++
	f.lastUpsert = s
	id := s.ID
	if id == "" {
		f.next++
		id = "s-" + time.Unix(int64(f.next), 0).Format("150405")
	}
	s.ID = id
	state := f.upsertState
	if state == "" {
		state = "active"
	}
	s.Status.State = state
	f.silences[id] = s
	if f.upsertErrAfterSideEffect != nil {
		err := f.upsertErrAfterSideEffect
		f.upsertErrAfterSideEffect = nil
		return "", err
	}
	return id, nil
}
func (f *fakeAM) ExpireSilence(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.silences[id]
	if !ok {
		return errors.New("not found")
	}
	f.expires++
	s.Status.State = "expired"
	f.silences[id] = s
	if f.expireErrAfterSideEffect != nil {
		err := f.expireErrAfterSideEffect
		f.expireErrAfterSideEffect = nil
		return err
	}
	return nil
}
func (f *fakeAM) ListAlerts(context.Context) ([]AMAlert, error) {
	f.mu.Lock()
	out := append([]AMAlert(nil), f.alerts...)
	fn := f.onListAlerts
	f.mu.Unlock()
	if fn != nil {
		fn()
	}
	return out, nil
}

func activeAlert(labels map[string]string) AMAlert {
	a := AMAlert{Labels: labels, StartsAt: time.Unix(1, 0), EndsAt: time.Now().Add(time.Hour)}
	a.Status.State = "suppressed"
	return a
}
func testAuthorizer(actor string) *Authorizer {
	return &Authorizer{groupName: "test-platform-ci-admins", now: time.Now, users: map[string]bool{actor: true}, refreshedAt: time.Now()}
}

func metricCounterValue(counter prometheus.Counter) float64 {
	metric := &dto.Metric{}
	_ = counter.Write(metric)
	return metric.GetCounter().GetValue()
}
