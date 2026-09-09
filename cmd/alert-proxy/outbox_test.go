package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type failNthUpdateStore struct {
	StateStore
	failAt int
	calls  int
}

func (s *failNthUpdateStore) Update(ctx context.Context, mutate func(*State) error) error {
	s.calls++
	if s.calls == s.failAt {
		return errors.New("injected state commit failure")
	}
	return s.StateStore.Update(ctx, mutate)
}

func openTestGroup() *GroupState {
	return &GroupState{GroupKey: "g", Channel: "C", ShortID: "short", EpisodeID: "e", Status: "firing", EpisodeStartedAt: time.Now().Add(-time.Hour), LastSeen: time.Now(), LatestBatch: []AlertSnapshot{{Fingerprint: "f", Status: "firing", Labels: map[string]string{"alertname": "Broken", "namespace": "ci"}}}, KnownMembers: map[string]KnownMember{"f": {LastExplicitStatus: "firing"}}, AlertShortIDs: map[string]string{"f": "af"}, Parent: &SlackObject{PostID: "post", DesiredRevision: 1, Phase: "pending"}, RecentDeliveries: map[string]DeliveryRecord{}}
}

func TestOutboxRetainsNewerRevisionAcrossFirstPost(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	g := openTestGroup()
	_ = store.Update(ctx, func(s *State) error {
		s.Groups[g.GroupKey] = g
		s.Outbox["parent:post"] = &OutboxWork{GroupKey: "g", Object: "parent", Kind: "post", DesiredRevision: 1, Target: OutboxTarget{Channel: "C"}, Phase: "pending"}
		return nil
	})
	slack := &fakeSlack{postTS: "100.1"}
	slack.onPost = func() {
		_ = store.Update(ctx, func(s *State) error {
			s.Groups["g"].NotificationCount++
			s.Groups["g"].Parent.DesiredRevision = 2
			s.Outbox["parent:post"].DesiredRevision = 2
			return nil
		})
	}
	worker := NewOutboxWorker(store, slack, testRenderer(), nil)
	did, err := worker.ProcessNext(ctx)
	if err != nil || !did {
		t.Fatalf("did=%v err=%v", did, err)
	}
	state, _ := store.Read(ctx)
	work := state.Outbox["parent:post"]
	if work == nil || work.Kind != "update" || work.Target.MessageTS != "100.1" || state.Groups["g"].Parent.AppliedRevision != 1 {
		t.Fatalf("newer revision lost: %#v group=%#v", work, state.Groups["g"].Parent)
	}
}

func TestAmbiguousPostIsRetriedAfterRecovery(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	g := openTestGroup()
	_ = store.Update(ctx, func(s *State) error {
		s.Groups["g"] = g
		s.Outbox["parent:post"] = &OutboxWork{GroupKey: "g", Object: "parent", Kind: "post", DesiredRevision: 1, Target: OutboxTarget{Channel: "C"}, Phase: "pending"}
		return nil
	})
	slack := &fakeSlack{postErr: errors.New("connection lost after request")}
	worker := NewOutboxWorker(store, slack, testRenderer(), nil)
	worker.now = func() time.Time { return time.Unix(100, 0) }
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	if state.Outbox["parent:post"].Phase != "pending" {
		t.Fatal("ambiguous post was not retained")
	}
	_ = store.Update(ctx, func(s *State) error { s.Outbox["parent:post"].Phase = "inFlight"; return nil })
	if err := worker.RecoverInFlight(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ = store.Read(ctx)
	if state.Outbox["parent:post"].Phase != "pending" {
		t.Fatal("stale in-flight post was not recovered")
	}
}

func TestDeletedTrackedParentGetsReplacementPost(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	g := openTestGroup()
	g.Parent.MessageTS = "1.2"
	_ = store.Update(ctx, func(s *State) error {
		s.Groups["g"] = g
		s.Outbox["parent:post"] = &OutboxWork{GroupKey: "g", Object: "parent", Kind: "update", DesiredRevision: 1, Target: OutboxTarget{Channel: "C", MessageTS: "1.2"}, Phase: "pending"}
		return nil
	})
	worker := NewOutboxWorker(store, &fakeSlack{updateErr: &SlackError{Code: "message_not_found"}}, testRenderer(), nil)
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	if state.Groups["g"].Parent.PostID == "post" {
		t.Fatal("deleted parent retained old post identity")
	}
	replacement := state.Outbox["parent:"+state.Groups["g"].Parent.PostID]
	if replacement == nil || replacement.Kind != "post" {
		t.Fatalf("replacement missing: %#v", state.Outbox)
	}
}

func TestDeletedMemberListGetsThreadReplacementWithoutReplacingParent(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	g := openTestGroup()
	g.Parent.MessageTS = "1.2"
	g.MemberList = &SlackObject{PostID: "members", MessageTS: "1.3", DesiredRevision: 2, Phase: "posted"}
	_ = store.Update(ctx, func(s *State) error {
		s.Groups["g"] = g
		s.Outbox["member-list:members"] = &OutboxWork{GroupKey: "g", Object: "memberList", Kind: "update", DesiredRevision: 2, Target: OutboxTarget{Channel: "C", MessageTS: "1.3"}, Phase: "pending"}
		return nil
	})
	worker := NewOutboxWorker(store, &fakeSlack{updateErr: &SlackError{Code: "message_not_found"}}, testRenderer(), nil)
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	if state.Groups["g"].Parent.PostID != "post" || state.Groups["g"].Parent.MessageTS != "1.2" {
		t.Fatalf("member-list deletion replaced parent: %#v", state.Groups["g"].Parent)
	}
	replacement := state.Outbox["member-list:"+state.Groups["g"].MemberList.PostID]
	if replacement == nil || replacement.Kind != "post" || replacement.Target.ThreadTS != "1.2" || replacement.DependsOnPostID != "" {
		t.Fatalf("member-list replacement missing or incorrectly targeted: %#v", replacement)
	}
}

func TestMissingReactionScopeDoesNotFailEscalation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	payload := SlackPayload{Text: "warning"}
	_ = store.Update(ctx, func(s *State) error {
		s.Outbox["reaction"] = &OutboxWork{Object: "reaction", Kind: "reaction", Target: OutboxTarget{Channel: "C", MessageTS: "1"}, ImmutablePayload: &payload, Phase: "pending"}
		return nil
	})
	worker := NewOutboxWorker(store, &fakeSlack{reactionErr: &SlackError{Code: "missing_scope"}}, testRenderer(), nil)
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	if state.Outbox["reaction"] != nil {
		t.Fatal("optional reaction remained failed in outbox")
	}
}

func TestMissingReactionMessageIsDroppedWithoutRetry(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	payload := SlackPayload{Text: "warning"}
	_ = store.Update(ctx, func(s *State) error {
		s.Outbox["reaction"] = &OutboxWork{GroupKey: "g", Object: "reaction", Kind: "reaction", Target: OutboxTarget{Channel: "C", MessageTS: "deleted"}, ImmutablePayload: &payload, Phase: "pending"}
		return nil
	})
	worker := NewOutboxWorker(store, &fakeSlack{reactionErr: &SlackError{Code: "message_not_found"}}, testRenderer(), nil)
	if did, err := worker.ProcessNext(ctx); err != nil || !did {
		t.Fatalf("did=%v err=%v", did, err)
	}
	state, _ := store.Read(ctx)
	if state.Outbox["reaction"] != nil {
		t.Fatalf("optional reaction remained in retry loop: %#v", state.Outbox["reaction"])
	}
}

func TestSuccessfulReactionUsesOneShotCompletion(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	payload := SlackPayload{Text: "warning"}
	if err := store.Update(ctx, func(s *State) error {
		s.Outbox["reaction"] = &OutboxWork{Object: "reaction", Kind: "reaction", Target: OutboxTarget{Channel: "C", MessageTS: "1"}, ImmutablePayload: &payload, Phase: "pending"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	slack := &fakeSlack{}
	worker := NewOutboxWorker(store, slack, testRenderer(), metrics)
	if did, err := worker.ProcessNext(ctx); err != nil || !did {
		t.Fatalf("did=%v err=%v", did, err)
	}
	state, err := store.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.Outbox["reaction"] != nil || slack.reactions != 1 {
		t.Fatalf("reaction was not completed once: work=%#v calls=%d", state.Outbox["reaction"], slack.reactions)
	}
	if got := metricCounterValue(metrics.slackMessages.WithLabelValues("reaction")); got != 1 {
		t.Fatalf("reaction success metric=%v", got)
	}
	if got := metricCounterValue(metrics.slackMessages.WithLabelValues("update")); got != 0 {
		t.Fatalf("reaction was misreported as update: %v", got)
	}
}

func TestPostSuccessStateCommitFailureIsMeasuredAmbiguous(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStateStore()
	g := openTestGroup()
	_ = base.Update(ctx, func(s *State) error {
		s.Groups[g.GroupKey] = g
		putOutbox(s, "parent:"+g.Parent.PostID, &OutboxWork{GroupKey: g.GroupKey, Object: "parent", Kind: "post", DesiredRevision: 1, Target: OutboxTarget{Channel: g.Channel}, Phase: "pending"})
		return nil
	})
	store := &failNthUpdateStore{StateStore: base, failAt: 2}
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	slack := &fakeSlack{}
	worker := NewOutboxWorker(store, slack, testRenderer(), metrics)
	did, err := worker.ProcessNext(ctx)
	if err == nil || !did {
		t.Fatalf("did=%v err=%v", did, err)
	}
	state, _ := base.Read(ctx)
	if slack.posts != 1 || state.Outbox["parent:"+g.Parent.PostID].Phase != "inFlight" {
		t.Fatalf("failpoint did not cover post/commit window: posts=%d work=%#v", slack.posts, state.Outbox["parent:"+g.Parent.PostID])
	}
	if got := metricCounterValue(metrics.ambiguousPosts.WithLabelValues("parent")); got != 1 {
		t.Fatalf("ambiguous post metric=%v", got)
	}
}

func TestBackoffPreservesPerGroupOrderWithoutBlockingOtherGroups(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	payload := SlackPayload{Text: "reply"}
	now := time.Unix(100, 0)
	_ = store.Update(ctx, func(s *State) error {
		putOutbox(s, "g1-parent", &OutboxWork{GroupKey: "g1", Object: "eventReply", Kind: "post", Target: OutboxTarget{Channel: "C", ThreadTS: "1"}, ImmutablePayload: &payload, Phase: "pending", Attempt: 1, LastAttemptAt: now})
		putOutbox(s, "g1-reply", &OutboxWork{GroupKey: "g1", Object: "eventReply", Kind: "post", Target: OutboxTarget{Channel: "C", ThreadTS: "1"}, ImmutablePayload: &payload, Phase: "pending"})
		putOutbox(s, "g2-reply", &OutboxWork{GroupKey: "g2", Object: "eventReply", Kind: "post", Target: OutboxTarget{Channel: "C", ThreadTS: "2"}, ImmutablePayload: &payload, Phase: "pending"})
		return nil
	})
	slack := &fakeSlack{}
	worker := NewOutboxWorker(store, slack, testRenderer(), nil)
	worker.now = func() time.Time { return now }
	if did, err := worker.ProcessNext(ctx); err != nil || !did {
		t.Fatalf("did=%v err=%v", did, err)
	}
	state, _ := store.Read(ctx)
	if state.Outbox["g2-reply"] != nil || state.Outbox["g1-reply"] == nil {
		t.Fatalf("later same-group work bypassed backoff: %#v", state.Outbox)
	}
}

func TestEmptySlackPostTimestampRemainsAmbiguous(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	g := openTestGroup()
	_ = store.Update(ctx, func(s *State) error {
		s.Groups[g.GroupKey] = g
		putOutbox(s, "parent:"+g.Parent.PostID, &OutboxWork{GroupKey: g.GroupKey, Object: "parent", Kind: "post", DesiredRevision: 1, Target: OutboxTarget{Channel: g.Channel}, Phase: "pending"})
		return nil
	})
	worker := NewOutboxWorker(store, &fakeSlack{emptyPostTS: true}, testRenderer(), nil)
	worker.now = func() time.Time { return time.Unix(100, 0) }
	if did, err := worker.ProcessNext(ctx); err != nil || !did {
		t.Fatalf("did=%v err=%v", did, err)
	}
	state, _ := store.Read(ctx)
	if state.Outbox["parent:"+g.Parent.PostID] == nil || state.Outbox["parent:"+g.Parent.PostID].Phase != "pending" || state.Groups[g.GroupKey].Parent.MessageTS != "" {
		t.Fatalf("empty Slack timestamp was committed as success: %#v group=%#v", state.Outbox, state.Groups[g.GroupKey])
	}
}

func TestDeletedParentReplacementKeepsPrerequisiteSequence(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	g := openTestGroup()
	g.Parent.MessageTS = "1.2"
	payload := SlackPayload{Text: "reply"}
	_ = store.Update(ctx, func(s *State) error {
		s.Groups[g.GroupKey] = g
		s.SilenceRefs["owner"] = &SilenceRef{ThreadTS: "1.2"}
		putOutbox(s, "parent:"+g.Parent.PostID, &OutboxWork{GroupKey: g.GroupKey, Object: "parent", Kind: "update", DesiredRevision: 1, Target: OutboxTarget{Channel: g.Channel, MessageTS: "1.2"}, Phase: "pending"})
		putOutbox(s, "dependent", &OutboxWork{GroupKey: g.GroupKey, Object: "eventReply", Kind: "post", Target: OutboxTarget{Channel: g.Channel, ThreadTS: "1.2"}, ImmutablePayload: &payload, Phase: "pending"})
		return nil
	})
	slack := &fakeSlack{updateErr: &SlackError{Code: "message_not_found"}}
	worker := NewOutboxWorker(store, slack, testRenderer(), nil)
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	replacementID := "parent:" + state.Groups[g.GroupKey].Parent.PostID
	if state.Outbox[replacementID] == nil || state.Outbox[replacementID].Sequence >= state.Outbox["dependent"].Sequence {
		t.Fatalf("replacement moved behind dependent: %#v", state.Outbox)
	}
	if state.SilenceRefs["owner"].ThreadTS != "" || state.SilenceRefs["owner"].ParentPostID != state.Groups[g.GroupKey].Parent.PostID {
		t.Fatalf("silence audit correlation was not rewired: %#v", state.SilenceRefs["owner"])
	}
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ = store.Read(ctx)
	if state.Outbox[replacementID] != nil || state.Outbox["dependent"].DependsOnPostID != "" || state.Outbox["dependent"].Target.ThreadTS == "" {
		t.Fatalf("replacement prerequisite was not delivered first: %#v", state.Outbox)
	}
	if state.SilenceRefs["owner"].ThreadTS == "" || state.SilenceRefs["owner"].ParentPostID != "" {
		t.Fatalf("silence audit correlation did not adopt replacement timestamp: %#v", state.SilenceRefs["owner"])
	}
}

func TestDeletedMemberReplacementKeepsPrerequisiteSequence(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	g := openTestGroup()
	g.Parent.MessageTS = "1.2"
	g.MemberList = &SlackObject{PostID: "members", MessageTS: "1.3", DesiredRevision: 2, Phase: "posted"}
	payload := SlackPayload{Text: "reply"}
	_ = store.Update(ctx, func(s *State) error {
		s.Groups[g.GroupKey] = g
		putOutbox(s, "member-list:members", &OutboxWork{GroupKey: g.GroupKey, Object: "memberList", Kind: "update", DesiredRevision: 2, Target: OutboxTarget{Channel: g.Channel, MessageTS: "1.3"}, Phase: "pending"})
		putOutbox(s, "dependent", &OutboxWork{GroupKey: g.GroupKey, Object: "eventReply", Kind: "post", Target: OutboxTarget{Channel: g.Channel}, DependsOnPostID: "members", ImmutablePayload: &payload, Phase: "pending"})
		return nil
	})
	worker := NewOutboxWorker(store, &fakeSlack{updateErr: &SlackError{Code: "message_not_found"}}, testRenderer(), nil)
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	replacementID := "member-list:" + state.Groups[g.GroupKey].MemberList.PostID
	if state.Outbox[replacementID] == nil || state.Outbox[replacementID].Sequence >= state.Outbox["dependent"].Sequence {
		t.Fatalf("member replacement moved behind dependent: %#v", state.Outbox)
	}
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ = store.Read(ctx)
	if state.Outbox[replacementID] != nil || state.Outbox["dependent"].DependsOnPostID != "" || state.Outbox["dependent"].Target.ThreadTS == "" {
		t.Fatalf("member prerequisite was not delivered first: %#v", state.Outbox)
	}
}

func TestDeletedSilenceAuditIsRepostedWithoutReplacingParent(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	g := openTestGroup()
	g.Parent.MessageTS = "parent-ts"
	owner := ownershipID("app-ci-uwm", validMatchers())
	payload := testRenderer().RenderEventReply("expired; controls retired")
	_ = store.Update(ctx, func(s *State) error {
		s.Groups[g.GroupKey] = g
		s.SilenceRefs[owner] = &SilenceRef{SilenceID: "s", Channel: "C", ThreadTS: "parent-ts", AuditMessageTS: "audit-ts"}
		putOutbox(s, "audit-update", &OutboxWork{GroupKey: g.GroupKey, Object: "silenceAudit", Kind: "update", DesiredRevision: 1, Target: OutboxTarget{Channel: "C", MessageTS: "audit-ts"}, ImmutablePayload: &payload, Phase: "pending"})
		return nil
	})
	worker := NewOutboxWorker(store, &fakeSlack{updateErr: &SlackError{Code: "message_not_found"}}, testRenderer(), nil)
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	ref := state.SilenceRefs[owner]
	if state.Groups[g.GroupKey].Parent.PostID != "post" || ref.AuditMessageTS != "" || ref.AuditPostID == "" {
		t.Fatalf("audit deletion corrupted parent/correlation: parent=%#v ref=%#v", state.Groups[g.GroupKey].Parent, ref)
	}
	replacement := state.Outbox[ref.AuditPostID]
	if replacement == nil || replacement.Object != "silenceAudit" || replacement.Kind != "post" || replacement.Target.ThreadTS != "parent-ts" || replacement.ImmutablePayload == nil || strings.Contains(string(replacement.ImmutablePayload.Blocks), "alert-proxy:unsilence") {
		t.Fatalf("audit replacement was not a control-free thread post: %#v", replacement)
	}
}

func TestProbeDeliveryMetricOnlyCountsAcceptedParent(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	store := NewMemoryStateStore()
	g := openTestGroup()
	g.GroupLabels = map[string]string{"alert_proxy_probe": "true"}
	_ = store.Update(context.Background(), func(s *State) error {
		s.Groups[g.GroupKey] = g
		putOutbox(s, "parent:"+g.Parent.PostID, &OutboxWork{GroupKey: g.GroupKey, Object: "parent", Kind: "post", DesiredRevision: 1, Target: OutboxTarget{Channel: g.Channel}, ProbeDelivery: true, Phase: "pending"})
		return nil
	})
	worker := NewOutboxWorker(store, &fakeSlack{}, testRenderer(), metrics)
	if _, err := worker.ProcessNext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := metricCounterValue(metrics.endToEndDeliveries); got != 1 {
		t.Fatalf("probe delivery count=%v", got)
	}
}

func TestPermanentFailureDoesNotStarveLaterSameGroupCloseout(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	payload := SlackPayload{Text: "final"}
	_ = store.Update(ctx, func(s *State) error {
		putOutbox(s, "failed", &OutboxWork{GroupKey: "g", Object: "eventReply", Kind: "post", Target: OutboxTarget{Channel: "C"}, ImmutablePayload: &payload, Phase: "failed"})
		putOutbox(s, "closeout", &OutboxWork{GroupKey: "g", Object: "eventReply", Kind: "post", Target: OutboxTarget{Channel: "C"}, ImmutablePayload: &payload, Phase: "pending"})
		return nil
	})
	worker := NewOutboxWorker(store, &fakeSlack{}, testRenderer(), nil)
	if did, err := worker.ProcessNext(ctx); err != nil || !did {
		t.Fatalf("did=%v err=%v", did, err)
	}
	state, _ := store.Read(ctx)
	if state.Outbox["closeout"] != nil || state.Outbox["failed"] == nil {
		t.Fatalf("later closeout was starved or audit failure lost: %#v", state.Outbox)
	}
}

func TestOverflowRetirementSurvivesInFlightFirstPost(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	g := openTestGroup()
	g.Parent.MessageTS = "parent-ts"
	g.LatestBatch = g.LatestBatch[:1]
	g.MemberList = &SlackObject{PostID: "members", DesiredRevision: 1, Phase: "pending"}
	attempt := &OutboxWork{GroupKey: g.GroupKey, Object: "memberList", Kind: "post", DesiredRevision: 1, Target: OutboxTarget{Channel: g.Channel, ThreadTS: "parent-ts"}, Phase: "inFlight"}
	_ = store.Update(ctx, func(s *State) error {
		s.Groups[g.GroupKey] = g
		putOutbox(s, "member-list:members", attempt)
		s.Outbox["member-list:members"].Phase = "inFlight"
		return nil
	})
	if err := store.Update(ctx, func(s *State) error {
		retireMemberList(s, s.Groups[g.GroupKey], testRenderer())
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	worker := NewOutboxWorker(store, &fakeSlack{}, testRenderer(), nil)
	if err := worker.completePost(ctx, "member-list:members", attempt, "overflow-ts"); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	work := state.Outbox["member-list:members"]
	if work == nil || work.Object != "memberListRetire" || work.Kind != "update" || work.Target.MessageTS != "overflow-ts" || state.Groups[g.GroupKey].MemberList.MessageTS != "overflow-ts" {
		t.Fatalf("in-flight post lost retirement update: work=%#v member=%#v", work, state.Groups[g.GroupKey].MemberList)
	}
	if did, err := worker.ProcessNext(ctx); err != nil || !did {
		t.Fatalf("did=%v err=%v", did, err)
	}
	state, _ = store.Read(ctx)
	if state.Groups[g.GroupKey].MemberList != nil || state.Outbox["member-list:members"] != nil {
		t.Fatalf("retirement update did not finish: group=%#v outbox=%#v", state.Groups[g.GroupKey], state.Outbox)
	}
}
