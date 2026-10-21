package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
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

func TestResolvedGroupDoesNotClaimTheProblemIsFixed(t *testing.T) {
	g := openTestGroup()
	g.Status = "resolved"
	g.LatestBatch = []AlertSnapshot{{Fingerprint: "a", Status: "resolved", Labels: map[string]string{"alertname": "plank-job-with-infra-internal-role-failures"}}}
	rendered := string(testRenderer().RenderParent(g, false).Blocks)
	if strings.Contains(rendered, "RESOLVED") || strings.Contains(rendered, "white_check_mark") {
		t.Fatalf("rendered group claims resolution: %s", rendered)
	}
	if !strings.Contains(rendered, noLongerFiringStatus) || !strings.Contains(rendered, "no longer firing") {
		t.Fatalf("rendered group omits the no-longer-firing wording: %s", rendered)
	}

	if !strings.Contains(rendered, noLongerFiringCaveat) {
		t.Fatalf("rendered group omits the no-longer-firing caveat: %s", rendered)
	}

	// The outbox can re-render a group after closeEpisode has already detached it,
	// so the ClosedReason path has to reach the same wording as the live one —
	// caveat included, because that is all a settled episode ever says.
	closed := openTestGroup()
	closed.Status = "firing"
	closed.ClosedReason = "resolved"
	closedRender := string(testRenderer().RenderParent(closed, false).Blocks)
	if !strings.Contains(closedRender, noLongerFiringStatus) || !strings.Contains(closedRender, noLongerFiringCaveat) {
		t.Fatalf("closed-as-resolved group omits the no-longer-firing wording: %s", closedRender)
	}

	// Silences and staleness have their own explanation; the settlement caveat
	// would be a claim about Alertmanager that those closes never made.
	for _, reason := range []string{"silenced", "stale"} {
		other := openTestGroup()
		other.Status = "firing"
		other.ClosedReason = reason
		if rendered := string(testRenderer().RenderParent(other, false).Blocks); strings.Contains(rendered, noLongerFiringCaveat) {
			t.Fatalf("%s close carried the settlement caveat: %s", reason, rendered)
		}
	}
}

func TestRenderedAlertKeepsAnnotationLinks(t *testing.T) {
	got := renderAlert(AlertSnapshot{
		Fingerprint:  "a",
		Status:       "firing",
		Labels:       map[string]string{"alertname": "plank-job-with-infra-internal-role-failures"},
		Annotations:  map[string]string{"message": "Check on <https://deck-internal-ci.apps.ci/?job=x|deck-internal>. <!channel>"},
		GeneratorURL: "https://prometheus.example/graph?g0.expr=up<!channel",
	})
	want := ":red_circle: *plank-job-with-infra-internal-role-failures* — `firing`\n" +
		"Check on <https://deck-internal-ci.apps.ci/?job=x|deck-internal>. &lt;!channel&gt;" +
		" · <https://prometheus.example/graph?g0.expr=up%3C!channel|source>"
	if got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
}

func TestRenderedAlertSourceLinks(t *testing.T) {
	detail := "See <https://grafana.example/d/dashboard?orgId=1&viewPanel=4|Grafana Dashboard> | " +
		"<https://prow.ci.openshift.org/?job=periodic-prow-auto-config-brancher|Deck> | " +
		"<https://github.com/openshift/release/blob/main/docs/dptp-triage-sop/infrastructure-jobs.md#periodic-prow-auto-config-brancher|SOP>."
	for _, tc := range []struct {
		name         string
		generatorURL string
		sourceLink   string
	}{
		{name: "HTTPS", generatorURL: "https://query.example/graph?g0.expr=up&g0.tab=1", sourceLink: " · <https://query.example/graph?g0.expr=up&g0.tab=1|source>"},
		{name: "HTTP", generatorURL: "http://query.example/graph?g0.expr=up", sourceLink: " · <http://query.example/graph?g0.expr=up|source>"},
		{name: "link delimiters", generatorURL: "https://query.example/graph?g0.expr=up<!channel>|other", sourceLink: " · <https://query.example/graph?g0.expr=up%3C!channel%3E%7Cother|source>"},
		{name: "empty"},
		{name: "relative graph", generatorURL: "/graph?g0.expr=up&g0.tab=1"},
		{name: "relative path", generatorURL: "graph?g0.expr=up"},
		{name: "scheme relative", generatorURL: "//query.example/graph?g0.expr=up"},
		{name: "missing host", generatorURL: "https:///graph?g0.expr=up"},
		{name: "malformed escape", generatorURL: "https://query.example/%zz"},
		{name: "non HTTP scheme", generatorURL: "javascript:alert(1)"},
		{name: "URL credentials", generatorURL: "https://user:password@query.example/graph"},
		{name: "space", generatorURL: "https://query.example/graph?g0.expr=up == 0"},
		{name: "control character", generatorURL: "https://query.example/graph\n<!channel>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alert := AlertSnapshot{
				Fingerprint:  "a",
				Status:       "firing",
				Labels:       map[string]string{"alertname": "infrastructure-job-failures"},
				Annotations:  map[string]string{"message": detail},
				GeneratorURL: tc.generatorURL,
			}
			want := ":red_circle: *infrastructure-job-failures* — `firing`\n" + detail + tc.sourceLink
			if diff := cmp.Diff(want, renderAlert(alert)); diff != "" {
				t.Errorf("unexpected rendered alert (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAnnotationLinksSurviveButBroadcastsDoNot(t *testing.T) {
	for _, tc := range []struct {
		name, in, want string
	}{
		{
			name: "labelled link is passed through verbatim",
			in:   "Check on <https://deck-internal-ci.apps.ci/?job=x|deck-internal>.",
			want: "Check on <https://deck-internal-ci.apps.ci/?job=x|deck-internal>.",
		},
		{
			name: "bare link is passed through verbatim",
			in:   "See <https://example.com/a?b=1&c=2>",
			want: "See <https://example.com/a?b=1&c=2>",
		},
		{
			name: "channel broadcast is escaped",
			in:   "<!channel> everyone look",
			want: "&lt;!channel&gt; everyone look",
		},
		{
			name: "user mention is escaped",
			in:   "ping <@U12345>",
			want: "ping &lt;@U12345&gt;",
		},
		{
			name: "text around a link is still escaped",
			in:   "a < b & <https://example.com|x> c > d",
			want: "a &lt; b &amp; <https://example.com|x> c &gt; d",
		},
		{
			// The label pattern rejects angle brackets, so nothing matches and the
			// whole span is escaped rather than passed through as a link.
			name: "a broadcast cannot hide in a link label",
			in:   "<https://example.com|<!channel>>",
			want: "&lt;https://example.com|&lt;!channel&gt;&gt;",
		},
		{
			name: "a broadcast cannot hide in a link URL",
			in:   "<https://<!channel>|z>",
			want: "&lt;https://&lt;!channel&gt;|z&gt;",
		},
		{
			name: "a broadcast abutting a link is escaped",
			in:   "<https://example.com|x><!channel>",
			want: "<https://example.com|x>&lt;!channel&gt;",
		},
		{
			name: "a broadcast prefixing a link is escaped",
			in:   "<<!channel>https://example.com>",
			want: "&lt;&lt;!channel&gt;https://example.com&gt;",
		},
		{
			name: "subteam broadcast is escaped",
			in:   "<!subteam^S0|@team>",
			want: "&lt;!subteam^S0|@team&gt;",
		},
		{
			name: "channel reference is escaped",
			in:   "<#C1|general>",
			want: "&lt;#C1|general&gt;",
		},
		{
			name: "a non-http scheme is escaped",
			in:   "<javascript:alert(1)|x>",
			want: "&lt;javascript:alert(1)|x&gt;",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := slackAnnotationText(tc.in); got != tc.want {
				t.Fatalf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
}
