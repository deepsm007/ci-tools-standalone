# Alert proxy

Alert proxy groups Alertmanager webhook notifications into Slack cards and
maintains their delivery and silence state.

## Alert links

Alert annotations may contain Slack links such as `<https://example.com|SOP>`.
The renderer preserves HTTP(S) annotation links and escapes broadcasts and
mentions in the remaining annotation text.

The optional `source` link is shown only when `generatorURL` is an absolute
HTTP(S) URL with a hostname, no embedded credentials, and no whitespace.
Relative URLs such as `/graph?...` are omitted because Slack has no query-server
hostname against which to resolve them. Existing dashboard, Deck, and SOP links
in the annotation remain available.

For Thanos Ruler, configure `--alert.query-url` with a verified externally
accessible Thanos Query URL to produce usable source links. A Grafana or
Alertmanager URL does not serve the same `/graph` endpoint.
