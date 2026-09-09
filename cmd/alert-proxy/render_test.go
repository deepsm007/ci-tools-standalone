package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRendererCapsMembersAndMaintainsOverflowThread(t *testing.T) {
	g := openTestGroup()
	g.LatestBatch = nil
	g.AlertShortIDs = map[string]string{}
	for i := 0; i < 12; i++ {
		fp := string(rune('a' + i))
		g.LatestBatch = append(g.LatestBatch, AlertSnapshot{Fingerprint: fp, Status: "firing", Labels: map[string]string{"alertname": "Broken", "namespace": "ci"}})
		g.AlertShortIDs[fp] = "id" + fp
	}
	p := testRenderer().RenderParent(g, true)
	var blocks []map[string]any
	if err := json.Unmarshal(p.Blocks, &blocks); err != nil {
		t.Fatal(err)
	}
	if len(blocks) > 50 || !strings.Contains(string(p.Blocks), "and 2 more") || !strings.Contains(string(p.Blocks), "alert-proxy:ack:") {
		t.Fatalf("unexpected blocks: %s", p.Blocks)
	}
	thread := testRenderer().RenderMemberList(g)
	if !strings.Contains(thread.Text, "All 12 alerts") {
		t.Fatalf("overflow thread=%s", thread.Text)
	}
}

func TestRendererStripsControlsOnClose(t *testing.T) {
	g := openTestGroup()
	g.ClosedAt = time.Now()
	g.ClosedReason = "resolved"
	p := testRenderer().RenderParent(g, false)
	if strings.Contains(string(p.Blocks), "alert-proxy:ack") || strings.Contains(string(p.Blocks), "static_select") {
		t.Fatalf("closed message retained controls: %s", p.Blocks)
	}
}

func TestProbeRenderingIsCompactAndHasNoControls(t *testing.T) {
	g := openTestGroup()
	g.GroupLabels = map[string]string{"alert_proxy_probe": "true"}
	p := testRenderer().RenderParent(g, true)
	var blocks []map[string]any
	if err := json.Unmarshal(p.Blocks, &blocks); err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 || strings.Contains(string(p.Blocks), "alert-proxy:ack") || strings.Contains(string(p.Blocks), "alert-proxy:silence") || strings.Contains(string(p.Blocks), "escalat") {
		t.Fatalf("probe rendering was not compact/control-free: %s", p.Blocks)
	}
}

func TestMemberListNeverExceedsSlackBlockLimit(t *testing.T) {
	g := openTestGroup()
	g.LatestBatch = nil
	for i := 0; i < 70; i++ {
		g.LatestBatch = append(g.LatestBatch, AlertSnapshot{Fingerprint: fmt.Sprintf("fp-%d", i), Status: "firing", Labels: map[string]string{"alertname": fmt.Sprintf("Alert-%d", i)}, Annotations: map[string]string{"message": strings.Repeat("detail", 600)}})
	}
	p := testRenderer().RenderMemberList(g)
	var blocks []map[string]any
	if err := json.Unmarshal(p.Blocks, &blocks); err != nil {
		t.Fatal(err)
	}
	if len(blocks) > maxMemberListBlocks || !strings.Contains(string(p.Blocks), "Additional alert details were omitted") {
		t.Fatalf("member list blocks=%d payload=%s", len(blocks), p.Blocks)
	}
}
