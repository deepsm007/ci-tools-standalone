'use strict';

const $ = (id) => document.getElementById(id);
const resources = Object.fromEntries(['state', 'jobs', 'overrides', 'health', 'diagnostics'].map((key) => [key, { data: null, error: '', fetchedAt: null, loading: false, sequence: 0, controller: null }]));
const initialURL = new URL(location.href);
let view = ['overview', 'clusters', 'jobs', 'overrides', 'diagnostics'].includes(initialURL.searchParams.get('view')) ? initialURL.searchParams.get('view') : 'overview';
let filters = { name: initialURL.searchParams.get('name') || '', group: initialURL.searchParams.get('group') || '', cluster: initialURL.searchParams.get('cluster') || '' };
let pages = [''];
let pageIndex = 0;
let pageGeneration = null;
let config = null;
let configError = '';
let refreshing = false;
let refreshSequence = 0;
let paginationNotice = '';
let submittedFilters = null;
let lookupValidation = '';
let refreshTimer;
let returnFocus;
let returnOverrideID = null;
let returnDiagnosticID = null;
let diagnosticsWindow = initialURL.searchParams.get('window') === 'available' ? 'available' : '30m';
let showGeneratedFallbacks = false;
const diagnosticLimits = { scheduler: 10, dispatcher: 10 };
const diagnosticFilters = { search: '', category: '' };
let renderedDiagnosticsData = null;
let renderedDiagnosticsError = '';
let renderedDiagnosticsLoading = false;
let renderedOverrideData = null;
let renderedOverrideError = '';
const overrideFilters = { cluster: '', kind: '', status: '', scope: '', expiry: '', lifecycle: '' };

function node(tag, className, ...children) {
  const element = document.createElement(tag);
  if (className) element.className = className;
  for (const child of children.flat()) {
    if (child !== null && child !== undefined) element.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return element;
}
function button(label, action, className = 'link-button', value) {
  const element = node('button', className, label);
  element.type = 'button';
  element.dataset.action = action;
  if (value !== undefined) element.dataset.value = String(value);
  return element;
}
function badge(label, color = '') { return node('span', `badge ${color}`, label); }
function formatNumber(value) { return typeof value === 'number' && Number.isFinite(value) ? new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format(value) : '—'; }
function formatLogBytes(value) {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) return 'Not recorded';
  if (value < 1024) return `${formatNumber(value)} B`;
  if (value < 1048576) return `${formatNumber(value / 1024)} KiB`;
  return `${formatNumber(value / 1048576)} MiB`;
}
function validDate(value) { const date = value ? new Date(value) : null; return date && Number.isFinite(date.getTime()) && date.getUTCFullYear() > 1 ? date : null; }
function time(value) {
  const date = validDate(value);
  if (!date) return node('span', 'muted', 'Not recorded');
  const element = node('time', '', new Intl.DateTimeFormat(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' }).format(date));
  element.dateTime = date.toISOString(); element.title = date.toISOString();
  return element;
}
function shortDigest(value) { return value ? `${value.slice(0, 12)}${value.length > 12 ? '…' : ''}` : 'Not recorded'; }
function field(label, value, mono = false) { return node('div', '', node('div', 'field-label', label), node('div', `field-value${mono ? ' mono' : ''}`, value)); }
function panel(title, description, ...children) {
  return node('section', 'panel', node('div', 'panel-heading', node('div', '', node('h2', '', title), description ? node('p', '', description) : null)), children);
}
function stat(label, value, detail, word = false) { return node('div', 'stat', node('div', 'stat-label', label), node('div', `stat-value${word ? ' word' : ''}`, value), node('div', 'stat-detail', detail)); }
function empty(title, description) { return node('div', 'empty-state', node('h2', '', title), node('p', '', description)); }
function createTable(headers) {
  const table = node('table');
  table.append(node('thead', '', node('tr', '', headers.map((header) => node('th', '', header)))));
  const body = node('tbody'); table.append(body);
  return { wrapper: node('div', 'table-scroll', table), body };
}
function cell(value, className = '') { return node('td', className, value); }
function sourceBadge(source) {
  const labels = { baseline: 'Baseline', override: 'Override', 'runtime-override': 'Override', 'expired-override-fallback': 'Expired · fallback' };
  return badge(labels[source] || source || 'Unknown', source === 'expired-override-fallback' ? 'amber' : source?.includes('override') ? 'blue' : '');
}
function capabilities(values) { return node('div', 'capabilities', (values || []).length ? values.map((value) => node('span', 'capability', value)) : node('span', 'muted', 'None specified')); }
function stateBadge(state) { return badge(state || 'Not recorded', { Active: 'green', Pending: 'amber', Failed: 'red', Rejected: 'red', Revoked: '', Expired: '' }[state] || ''); }

async function requestJSON(path, signal) {
  const response = await fetch(path, { signal, cache: 'no-store', credentials: 'same-origin', headers: { Accept: 'application/json' } });
  if (!response.ok) {
    const error = new Error(`Request failed (${response.status})`);
    error.status = response.status;
    if (response.status === 401 || response.status === 403) error.message = `Observation access denied (${response.status}). Check the server's upstream read-token configuration.`;
    if (response.status === 503) error.message = 'Observation is unavailable (503). The upstream policy or this observation endpoint may not be ready.';
    if (path.startsWith('/api/v1/diagnostics')) error.message = `Diagnostics request failed (${response.status}). Check server diagnostics configuration and retry.`;
    throw error;
  }
  return response.json();
}
async function loadConfig() {
  try {
    config = await requestJSON('/api/v1/config'); configError = '';
    $('demo-badge').hidden = config.demo !== true;
    $('refresh-interval').textContent = `${refreshSeconds()}s`;
    scheduleRefresh();
  } catch (error) { configError = 'Dashboard configuration is unavailable. Demo mode cannot be determined.'; }
}
function jobsPath() {
  const query = new URLSearchParams({ limit: '25' });
  for (const [key, value] of Object.entries(submittedFilters || {})) if (value) query.set(key, value);
  if (pages[pageIndex]) query.set('after', pages[pageIndex]);
  if (pageGeneration !== null) query.set('generation', String(pageGeneration));
  return `/api/v1/dispatcher/jobs?${query}`;
}
async function loadResource(key, retryConflict = true, explicitCollection = false) {
  if (key === 'jobs' && !submittedFilters || key === 'diagnostics' && (!explicitCollection || view !== 'diagnostics')) return;
  const resource = resources[key];
  resource.controller?.abort();
  const controller = new AbortController(); resource.controller = controller;
  const sequence = ++resource.sequence; resource.loading = true; render(key === 'jobs');
  try {
    const path = key === 'jobs' ? jobsPath() : key === 'diagnostics' ? `/api/v1/diagnostics?window=${diagnosticsWindow}` : `/api/v1/dispatcher/${key}`;
    const data = await requestJSON(path, controller.signal);
    if (sequence !== resource.sequence) return;
    resource.data = data; resource.error = ''; resource.fetchedAt = new Date();
    if (key === 'jobs') pageGeneration = data.policy?.generation ?? null;
  } catch (error) {
    if (sequence !== resource.sequence || error.name === 'AbortError') return;
    if (key === 'jobs' && error.status === 409 && retryConflict) {
      resetPagination(); clearJobResults(); paginationNotice = 'The serving policy changed. Pagination restarted at the first page so these results use one generation.';
      await loadResource('jobs', false); return;
    }
    resource.error = error.message || 'The observation request failed.';
  } finally {
    if (sequence === resource.sequence) { resource.loading = false; render(key === 'jobs'); }
  }
}
async function refresh() {
  if (view === 'diagnostics') { if (!config) await loadConfig(); render(); return; }
  if (refreshing) return;
  const sequence = ++refreshSequence;
  refreshing = true; $('refresh').disabled = true;
  if (!config) await loadConfig();
  if (sequence !== refreshSequence) return;
  const keys = ['state', 'overrides', 'health'];
  await Promise.all(keys.map((key) => loadResource(key)));
  if (sequence !== refreshSequence) return;
  refreshing = false; $('refresh').disabled = false; render();
}
function refreshSeconds() {
  return Number.isFinite(config?.refreshIntervalSeconds) && config.refreshIntervalSeconds >= 1 ? config.refreshIntervalSeconds : 15;
}
function cancelActiveRefresh() {
  refreshSequence++; refreshing = false; $('refresh').disabled = false;
  for (const key of ['state', 'overrides', 'health', 'diagnostics']) {
    resources[key].controller?.abort(); resources[key].sequence++; resources[key].loading = false;
  }
}
function refreshForViewChange(previousView) {
  scheduleRefresh();
  if (previousView === 'diagnostics' || view === 'diagnostics') { cancelActiveRefresh(); clearDiagnosticResults(); if (view !== 'diagnostics') refresh(); else render(); }
}
function scheduleRefresh() {
  $('refresh-interval').textContent = `${refreshSeconds()}s`;
  clearInterval(refreshTimer);
  if (view === 'diagnostics') return;
  if ($('auto-refresh').checked) refreshTimer = setInterval(() => { if (!document.hidden) refresh(); }, refreshSeconds() * 1000);
}
function clearJobResults() {
  const resource = resources.jobs;
  resource.controller?.abort(); resource.sequence++;
  resource.data = null; resource.error = ''; resource.fetchedAt = null; resource.loading = false;
}
function resetPagination() { pages = ['']; pageIndex = 0; pageGeneration = null; }
function syncURL() {
  const url = new URL(location.href); url.search = ''; url.searchParams.set('view', view);
  if (view === 'jobs') for (const [key, value] of Object.entries(filters)) if (value) url.searchParams.set(key, value);
  if (view === 'diagnostics') url.searchParams.set('window', diagnosticsWindow);
  history.replaceState(null, '', url);
}
function navigate(nextView, cluster) {
  const previousView = view; view = nextView;
  if (cluster !== undefined) filters = { name: '', group: '', cluster };
  submittedFilters = null; lookupValidation = ''; paginationNotice = ''; resetPagination(); clearJobResults();
  $('content').replaceChildren(); syncURL(); render(); refreshForViewChange(previousView);
}
function render(updateJobResults = false) {
  const titles = { overview: ['Overview', 'The policy being served and where jobs are placed.'], clusters: ['Cluster policy', 'Baseline inventory and published temporary rules, observed from matching policies.'], jobs: ['Job lookup', 'Look up effective placement by job name or exact group.'], overrides: ['Overrides', 'Compare desired policy changes with what is actually being served.'], diagnostics: ['Diagnostics', 'Recent scheduler communication and dispatcher runtime observations.'] };
  $('page-title').textContent = titles[view][0]; $('breadcrumb-view').textContent = titles[view][0]; $('page-description').textContent = titles[view][1];
  document.title = `${titles[view][0]} · Dispatcher`;
  $('auto-refresh').closest('label').hidden = view === 'diagnostics';
  $('refresh').querySelector('span').textContent = view === 'diagnostics' ? 'Collect logs' : 'Refresh';
  if (view === 'diagnostics') $('refresh').disabled = resources.diagnostics.loading || !validDiagnosticWindow(diagnosticsWindow);
  $('source-kind').textContent = view === 'diagnostics' ? 'Diagnostic log sources' : 'Dispatcher observation API';
  $('source-note').textContent = view === 'diagnostics' ? 'Manual collection of bounded log evidence; source and history limits are reported per panel.' : 'Placement estimates, not physical cluster utilization.';
  document.querySelectorAll('[data-view]').forEach((link) => { if (link.dataset.view === view) link.setAttribute('aria-current', 'page'); else link.removeAttribute('aria-current'); });
  renderFreshness(); renderNotices();
  if (view === 'diagnostics' && $('diagnostics-controls')) { updateDiagnosticsView(); return; }
  // Aggregate refreshes must preserve draft fields, focus, and caret.
  if (view === 'jobs' && $('job-lookup-form')) {
    if (updateJobResults) updateJobsResults();
    updateLookupForm(); return;
  }
  if (view === 'overrides' && $('override-filter-form')) { updateOverrideView(); return; }
  const content = $('content');
  const focused = content.contains(document.activeElement) ? document.activeElement : null;
  const focusedOverrideID = focused?.dataset.overrideId;
  const focusedAction = focused?.dataset.action;
  const focusedValue = focused?.dataset.value;
  content.replaceChildren(view === 'overview' ? renderOverview() : view === 'clusters' ? renderClusters() : view === 'jobs' ? renderJobs() : view === 'diagnostics' ? renderDiagnostics() : renderOverrides());
  content.setAttribute('aria-busy', String(view === 'diagnostics' ? resources.diagnostics.loading : view === 'jobs' ? resources.jobs.loading : resources.state.loading));
  if (focusedOverrideID) findOverrideOpener(focusedOverrideID)?.focus();
  else if (focusedAction) [...content.querySelectorAll('[data-action]')].find((opener) => opener.dataset.action === focusedAction && opener.dataset.value === focusedValue)?.focus();
}
function renderFreshness() {
  const resource = resources[view === 'jobs' ? 'jobs' : view === 'overrides' ? 'overrides' : 'state'];
  const freshness = $('freshness');
  if (view === 'diagnostics') {
    const diagnostics = resources.diagnostics;
    if (!diagnostics.data) freshness.replaceChildren(diagnostics.loading ? 'Collecting logs…' : diagnostics.error ? 'Collection failed. Select Collect logs to retry.' : 'Manual collection only. No logs requested yet.');
    else freshness.replaceChildren(diagnostics.error ? 'Previous collection · Server observed ' : 'Server observed ', time(diagnostics.data.observedAt), ' · Received ', time(diagnostics.fetchedAt), diagnostics.loading ? ' · Collecting…' : ' · Results may come from a short server cache; collect again to request updates.');
    return;
  }
  if (view === 'jobs') {
    if (!submittedFilters) freshness.replaceChildren('No lookup submitted. Results are request-time observations.');
    else if (resource.loading) freshness.replaceChildren('Looking up matching jobs…');
    else if (resource.error) freshness.replaceChildren('Lookup failed. Submit Search to retry.');
    else freshness.replaceChildren('Results observed ', time(resource.data?.observedAt), ' · Requested ', time(resource.fetchedAt), ' · Submit Search to refresh results.');
    return;
  }
  const hasError = Boolean(resource.error || resources.state.error);
  const dot = node('span', `status-dot ${hasError ? 'warning' : !resource.data ? 'offline' : ''}`);
  if (!resource.fetchedAt) { freshness.replaceChildren(dot, resource.error ? 'Observation unavailable · no successful refresh yet' : 'Connecting to the dispatcher…'); return; }
  freshness.replaceChildren(dot, hasError ? 'Stale data · ' : 'Observed ', hasError ? time(resource.data?.observedAt) : time(resource.data?.observedAt), node('span', 'muted', ' · Successful refresh '), time(resource.fetchedAt));
  if (document.hidden && $('auto-refresh').checked) freshness.append(' · Auto-refresh paused while hidden');
}
function renderNotices() {
  const notices = [];
  if (view === 'diagnostics') {
    const resource = resources.diagnostics;
    if (configError && !resource.data) notices.push(node('div', 'notice', configError));
    if (resource.error) notices.push(node('div', 'notice', `${resource.error}${resource.data ? ' Showing the last successful diagnostics; results may be stale.' : ''}`));
    $('notices').replaceChildren(...notices); return;
  }
  if (configError) notices.push(node('div', 'notice', configError));
  for (const key of ['state', ...(view === 'jobs' ? ['jobs'] : ['overrides', 'clusters'].includes(view) ? ['overrides'] : ['health'])]) {
    const resource = resources[key];
    if (resource.error) notices.push(node('div', 'notice', `${key === 'state' ? 'Policy' : key[0].toUpperCase() + key.slice(1)}: ${resource.error}${resource.data ? ' Showing the last successful observation; values may be stale.' : key === 'jobs' ? ' Submit Search to retry.' : ' Use Refresh to retry.'}`));
  }
  if (resources.state.data && !resources.state.data.ready) notices.push(node('div', 'notice', 'The serving policy is not ready. No placement or policy generation is available yet.'));
  if (view === 'jobs' && paginationNotice) notices.push(node('div', 'notice info', paginationNotice));
  $('notices').replaceChildren(...notices);
}
function renderOverview() {
  const data = resources.state.data;
  if (!data) return empty(resources.state.error ? 'Dispatcher observation unavailable' : 'Loading dispatcher observations', resources.state.error ? 'Check the connection and upstream configuration, then refresh.' : 'Fetching the current serving policy.');
  if (!data.ready) return empty('Waiting for a serving policy', 'The dispatcher has not published its first policy. Observations will appear after a successful reconcile.');
  const fragment = document.createDocumentFragment();
  const clusters = data.clusters || [];
  const available = clusters.filter((cluster) => !cluster.blocked && cluster.inventoryKnown).length;
  fragment.append(node('div', 'stats', stat('Serving policy', 'Ready', `Generation ${data.policy?.generation ?? '—'}`, true), stat('Configured jobs', formatNumber(data.jobs), 'Effective placement in this observation'), stat('Estimated demand', formatNumber(data.demand), 'Estimated scheduling demand'), stat('Clusters', formatNumber(clusters.length), `${available} inventory-known and not blocked`)));
  const policy = panel('Serving policy', 'An immutable snapshot used for placement decisions.');
  policy.querySelector('.panel-heading').append(button('View policy details ↗', 'policy-details'));
  policy.append(node('div', 'policy-summary', field('GENERATION', data.policy?.generation ?? '—', true), field('INPUT DIGEST', shortDigest(data.policy?.digest), true), field('POLICY CHANGED', time(data.policy?.generatedAt)), field('PUBLISHED OVERRIDES', formatNumber(data.policy?.overrideIDs?.length || 0))));
  fragment.append(policy);
  const clusterPanel = panel('Cluster placement', `${clusters.length} clusters · effective decisions at observation time`);
  clusterPanel.querySelector('.panel-heading').append(badge('ESTIMATED DEMAND', 'teal'));
  const table = createTable(['CLUSTER', 'CAPABILITIES', 'SCHEDULING WEIGHT', 'JOBS', 'NORMALIZED DEMAND', 'INVENTORY STATUS']);
  const normalizedDemand = (cluster) => cluster.inventoryKnown && cluster.configuredCapacity > 0 && Number.isFinite(cluster.demand) ? cluster.demand / cluster.configuredCapacity : null;
  const maxDemand = Math.max(1, ...clusters.map((cluster) => normalizedDemand(cluster) ?? 0));
  clusters.forEach((cluster) => {
    const ratio = normalizedDemand(cluster);
    let demandCell = node('span', 'muted', '—');
    demandCell.title = 'Normalized demand is unavailable without known inventory and a positive scheduling weight.';
    if (ratio !== null) {
      const label = new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(ratio);
      const progress = node('progress', 'demand-progress'); progress.max = maxDemand; progress.value = ratio;
      progress.setAttribute('aria-label', `${cluster.name}: ${label} estimated demand per scheduling weight`);
      demandCell = node('div', 'demand-cell', node('div', 'demand-number', label), progress);
      demandCell.title = `Estimated demand ${formatNumber(cluster.demand)} / scheduling weight ${formatNumber(cluster.configuredCapacity)}. Not physical utilization.`;
    }
    table.body.append(node('tr', '', cell(node('div', '', button(cluster.name, 'cluster-jobs', 'link-button cluster-name', cluster.name), node('div', 'subtext', cluster.provider || (cluster.inventoryKnown ? 'Provider not specified' : 'Inventory unknown')))), cell(capabilities(cluster.capabilities)), cell(cluster.inventoryKnown ? formatNumber(cluster.configuredCapacity) : 'Unknown', 'numeric'), cell(formatNumber(cluster.jobs), 'numeric'), cell(demandCell), cell(cluster.blocked ? badge('Blocked', 'amber') : cluster.inventoryKnown ? badge('Not blocked', 'green') : badge('Unknown inventory', 'amber'))));
  });
  if (!clusters.length) { const row = node('tr'); const td = cell('No clusters in the serving snapshot.', 'empty-cell'); td.colSpan = 6; row.append(td); table.body.append(row); }
  clusterPanel.append(table.wrapper, node('div', 'table-note', 'Normalized demand = estimated demand / scheduling weight. Bars compare this ratio across clusters, not physical utilization. A dash means normalization is unavailable.'));
  fragment.append(clusterPanel, renderHealth()); return fragment;
}
function renderHealth() {
  const resource = resources.health; const data = resource.data;
  const section = panel('Reconcile health', 'Supporting metrics, independent of policy publication time.');
  section.querySelector('.panel-heading').append(badge(resource.error ? 'Unavailable' : data?.metricsAvailable ? 'Metrics available' : 'Metrics unavailable', resource.error || !data?.metricsAvailable ? '' : 'green'));
  const value = (key, timestamp = false) => data?.metricsAvailable ? timestamp ? time(data[key]) : data[key] === null || data[key] === undefined ? node('span', 'muted', 'Not recorded') : formatNumber(data[key]) : node('span', 'muted', 'Unavailable');
  section.append(node('div', 'health-grid', field('LAST SUCCESSFUL RECONCILE', value('lastSuccessfulReconcileAt', true)), field('LAST BASELINE UPDATE', value('lastBaselineUpdateAt', true)), field('RECONCILE ERRORS', value('reconcileErrors'))));
  section.append(node('p', 'health-note', data?.metricsAvailable ? 'A policy change timestamp is not a heartbeat. Reconcile metrics are shown only when reported by the upstream.' : 'Metrics are not reported by this deployment. Policy, jobs, and override observations remain available.'));
  return section;
}
function filterField(label, input) { const wrapper = node('div', 'filter'); const labelElement = node('label', '', label); labelElement.htmlFor = input.id; wrapper.append(labelElement, input); return wrapper; }
function renderJobs() {
  const section = panel('Job lookup', 'Enter a job name or exact group, optionally narrow by effective cluster, then submit Search.');
  const search = node('input'); search.id = 'filter-name'; search.type = 'search'; search.placeholder = 'Job name contains…'; search.value = filters.name; search.autocomplete = 'off';
  const group = node('input'); group.id = 'filter-group'; group.placeholder = 'Exact group name'; group.value = filters.group; group.autocomplete = 'off';
  const cluster = node('select'); cluster.id = 'filter-cluster'; cluster.append(new Option('All clusters', ''));
  const names = new Set((resources.state.data?.clusters || []).map((item) => item.name)); if (filters.cluster) names.add(filters.cluster);
  [...names].sort().forEach((name) => cluster.append(new Option(name, name))); cluster.value = filters.cluster;
  const submit = node('button', 'button primary', 'Search'); submit.type = 'submit'; submit.id = 'search-jobs'; submit.disabled = !filters.name.trim() && !filters.group.trim();
  const form = node('form', 'filters', filterField('JOB NAME', search), filterField('GROUP · EXACT MATCH', group), filterField('EFFECTIVE CLUSTER', cluster), node('div', 'lookup-actions', submit, button('Clear filters', 'clear-filters', 'button')));
  form.id = 'job-lookup-form'; form.setAttribute('aria-describedby', 'lookup-help lookup-validation');
  const help = node('p', 'table-note', 'A job name or exact group is required; cluster alone is insufficient. Auto-refresh and toolbar Refresh update aggregates only. Submit Search to refresh lookup results.'); help.id = 'lookup-help';
  const validation = node('p', 'lookup-validation', lookupValidation); validation.id = 'lookup-validation'; validation.setAttribute('role', 'alert'); validation.hidden = !lookupValidation;
  section.append(form, help, validation);
  const results = node('div'); results.id = 'jobs-results'; section.append(results); fillJobsResults(results); return section;
}
function updateLookupForm() {
  const cluster = $('filter-cluster');
  if (cluster) {
    const names = new Set((resources.state.data?.clusters || []).map((item) => item.name));
    if (filters.cluster) names.add(filters.cluster);
    const values = ['', ...[...names].sort()];
    if (JSON.stringify([...cluster.options].map((option) => option.value)) !== JSON.stringify(values)) {
      cluster.replaceChildren(...values.map((value) => new Option(value || 'All clusters', value)));
      cluster.value = filters.cluster;
    }
  }
  const submit = $('search-jobs'); if (submit) submit.disabled = !filters.name.trim() && !filters.group.trim();
  const validation = $('lookup-validation'); if (validation) { validation.textContent = lookupValidation; validation.hidden = !lookupValidation; }
}
function updateJobsResults() { const results = $('jobs-results'); if (results) fillJobsResults(results); $('content').setAttribute('aria-busy', String(resources.jobs.loading)); }
function fillJobsResults(container) {
  const resource = resources.jobs; const data = resource.data;
  if (!submittedFilters) { container.replaceChildren(empty('Find a job’s placement', 'Enter a job name or exact group and select Search. No jobs are fetched until you submit.')); return; }
  if (!data) {
    const children = [empty(resource.error ? 'Job observation unavailable' : 'Looking up jobs', resource.error ? 'The job endpoint did not return data. Submit Search to retry, or return to the previous page.' : 'Fetching effective placement decisions for your submitted criteria.')];
    if (pageIndex > 0) { const previous = button('← Previous', 'previous-page', 'button'); previous.disabled = resource.loading; children.push(node('div', 'pagination', `Page ${pageIndex + 1}`, previous)); }
    container.replaceChildren(...children); return;
  }
  const table = createTable(['JOB NAME', 'GROUP', 'EFFECTIVE CLUSTER', 'DEMAND', 'DECISION']);
  (data.jobs || []).forEach((job, index) => table.body.append(node('tr', '', cell(button(job.name, 'job-details', 'link-button job-name', index)), cell(job.group || node('span', 'muted', 'Ungrouped')), cell(node('div', '', node('span', 'cluster-name', job.decision?.cluster || 'Unknown'), job.baselineCluster !== job.decision?.cluster ? node('div', 'subtext', `Baseline: ${job.baselineCluster || 'Unknown'}`) : null)), cell(formatNumber(job.demand), 'numeric'), cell(sourceBadge(job.decision?.source)))));
  if (!data.jobs?.length) { const row = node('tr'); const td = cell(resource.loading ? 'Loading matching jobs…' : 'No jobs match these filters. Try another name, group, or cluster.', 'empty-cell'); td.colSpan = 5; row.append(td); table.body.append(row); }
  const previous = button('← Previous', 'previous-page', 'button'); previous.disabled = pageIndex === 0 || resource.loading;
  const next = button('Next →', 'next-page', 'button'); next.disabled = !data.nextAfter || resource.loading;
  const pagination = node('div', 'pagination', node('span', '', `${formatNumber(data.jobs?.length || 0)} jobs on this page · Generation ${data.policy?.generation ?? '—'}${resource.loading ? ' · Updating…' : ''}`), node('div', 'pagination-actions', previous, node('span', '', `Page ${pageIndex + 1}`), next));
  container.replaceChildren(table.wrapper, pagination);
}
// Preserve original response indexes through every filtered and sorted view.
function indexedOverrides(data) {
  return (data?.overrides || []).map((item, index) => ({ item, index }));
}
function overrideLifecycle(item, observedAt) {
  if (item.servingJobs > 0) return 'current';
  const observed = validDate(observedAt);
  const expiry = validDate(item.record?.spec?.expiresAt);
  if (item.published && (!observed || !expiry)) return 'unknown';
  if (item.published && expiry > observed) return 'current';
  if (observed && expiry && expiry <= observed) return 'history';
  if (['Expired', 'Revoked', 'Rejected', 'Failed'].includes(item.record?.status?.state)) return 'history';
  return observed && expiry ? 'current' : 'unknown';
}
function overrideIsHistory(item, observedAt) {
  return overrideLifecycle(item, observedAt) === 'history';
}
function filterOverrides(entries, criteria, observedAt) {
  const observed = validDate(observedAt);
  return entries.filter(({ item }) => {
    const spec = item.record?.spec || {};
    if (criteria.cluster && spec.cluster !== criteria.cluster) return false;
    if (criteria.kind && spec.kind !== criteria.kind) return false;
    if (criteria.status && item.record?.status?.state !== criteria.status) return false;
    if (criteria.scope === 'all-capabilities' && spec.scope?.capability) return false;
    if (criteria.scope?.startsWith('cap:') && spec.scope?.capability !== criteria.scope.slice(4)) return false;
    const history = overrideIsHistory(item, observedAt);
    if (criteria.lifecycle === 'current' && history || criteria.lifecycle === 'history' && !history) return false;
    const expiry = validDate(spec.expiresAt);
    if (criteria.expiry === 'unrecorded') return !expiry;
    if (!criteria.expiry) return true;
    if (!observed || !expiry) return false;
    const remaining = expiry - observed;
    if (criteria.expiry === 'expired') return remaining <= 0;
    const window = { '1h': 3600000, '24h': 86400000 }[criteria.expiry];
    return remaining > 0 && remaining <= window;
  });
}
function upcomingExpirations(data, limit = 10) {
  const observed = validDate(data?.observedAt);
  if (!data?.ready || !observed) return { available: false, total: 0, entries: [] };
  const entries = indexedOverrides(data).filter(({ item }) => {
    const expiry = validDate(item.record?.spec?.expiresAt);
    return item.published && expiry && expiry > observed;
  });
  entries.sort((a, b) => new Date(a.item.record.spec.expiresAt) - new Date(b.item.record.spec.expiresAt) || a.index - b.index);
  return { available: true, total: entries.length, entries: entries.slice(0, limit) };
}
function policyJoinStatus(stateResource, overrideResource) {
  if (stateResource.error || overrideResource.error) return { joined: false, message: 'An observation request failed. Last successful sources are shown separately; temporary rules are not joined.' };
  if (!stateResource.data || !overrideResource.data) return { joined: false, message: 'Waiting for both observations. Inventory remains available independently; temporary rules are not joined yet.' };
  const state = stateResource.data; const overrides = overrideResource.data;
  if (!state.ready || !overrides.ready) return { joined: false, message: 'A serving policy is not ready. Inventory and override records are shown separately.' };
  const a = state.policy; const b = overrides.policy;
  if (!a?.generation || !a.digest || !b?.generation || !b.digest) return { joined: false, message: 'Policy identity is incomplete. Temporary rules cannot be joined to inventory.' };
  if (a.generation !== b.generation || a.digest !== b.digest) return { joined: false, message: 'Policy generation or digest differs between observations. Refresh to obtain matching sources; temporary rules are not joined.' };
  return { joined: true, message: `Matched policy generation ${a.generation} and input digest ${shortDigest(a.digest)}. Observation times may differ.` };
}
function relativeExpiry(value, observedAt) {
  const expiry = validDate(value); const observed = validDate(observedAt);
  if (!expiry || !observed) return 'Relative expiry unavailable';
  const minutes = Math.ceil((expiry - observed) / 60000);
  if (minutes <= 0) return 'Expired at observation';
  if (minutes < 60) return `In ${minutes} min from observation`;
  if (minutes < 1440) return `In ${formatNumber(minutes / 60)} h from observation`;
  return `In ${formatNumber(minutes / 1440)} days from observation`;
}
function renderUpcomingExpirations() {
  const resource = resources.overrides; const data = resource.data;
  const section = panel('Upcoming expirations', 'Recorded boundaries of published rules, relative to the override observation time.');
  if (resource.error || !data?.ready || !validDate(data?.observedAt)) {
    section.append(node('div', 'table-note', resource.error ? 'Override observation is unavailable or stale. Upcoming expirations cannot be confirmed.' : data ? 'The override observation is not ready or its observation time is missing. Upcoming expirations are unavailable.' : 'Waiting for override observation. Upcoming expirations are not available yet.'));
    return section;
  }
  const summary = upcomingExpirations(data);
  section.querySelector('.panel-heading').append(badge(`${summary.total} future published rules`, 'blue'));
  section.append(node('div', 'table-note', 'Overrides observed ', time(data.observedAt), '. Showing up to the next 10 recorded expirations. Serving counts describe that observation, not predicted future moves.'));
  if (!summary.entries.length) section.append(node('div', 'table-note', 'No published rules have a recorded future expiry at this observation. Expired published records remain visible in the rule history.'));
  if (summary.entries.length) {
    const table = createTable(['RULE / SCOPE', 'CLUSTER', 'RECORDED EXPIRY', 'SERVING JOBS']);
    for (const { item, index } of summary.entries) {
      const spec = item.record?.spec || {};
      table.body.append(node('tr', '', cell(node('div', '', overrideButton(item, index), node('div', 'subtext', `${spec.kind || 'Unknown'} · ${spec.scope?.capability || 'All capabilities'}`))), cell(spec.cluster || 'Unknown'), cell(node('div', '', time(spec.expiresAt), node('div', 'subtext', relativeExpiry(spec.expiresAt, data.observedAt)))), cell(formatNumber(item.servingJobs), 'numeric')));
    }
    section.append(table.wrapper);
  }
  section.append(node('div', 'table-note', 'If no newer policy supersedes a rule, decisions fall back to the baseline after its expiry. A zero serving count is valid; a recorded expiry does not predict exact future placement.'));
  return section;
}
function overrideButton(item, index) {
  const spec = item.record?.spec || {};
  const opener = button(spec.id || item.record?.metadata?.name || 'Unnamed override', 'override-details', 'link-button', index);
  opener.dataset.overrideId = spec.id || item.record?.metadata?.name || `index:${index}`;
  return opener;
}
function renderOverrideTable(entries, observedAt, emptyMessage) {
  const table = createTable(['OVERRIDE / REASON', 'TARGET / SCOPE', 'RECORDED STATUS', 'PUBLISHED', 'SERVING JOBS', 'RECORDED EXPIRY']);
  for (const { item, index } of entries) {
    const spec = item.record?.spec || {};
    table.body.append(node('tr', '', cell(node('div', 'override-reason', overrideButton(item, index), node('div', 'subtext truncate', spec.reason || 'No reason recorded'))), cell(node('div', '', node('span', 'cluster-name', spec.cluster || 'Unknown'), node('div', 'subtext', `${spec.kind || 'Unknown'}${spec.capacity !== undefined ? ` · requested weight ${spec.capacity}` : ''} · ${spec.scope?.capability || 'All capabilities'}`))), cell(stateBadge(item.record?.status?.state)), cell(badge(item.published ? 'Published' : 'Not published', item.published ? 'blue' : '')), cell(node('div', '', formatNumber(item.servingJobs), node('div', 'subtext', { history: 'History', current: 'Current / pending', unknown: 'Timing unknown' }[overrideLifecycle(item, observedAt)])), 'numeric'), cell(node('div', '', time(spec.expiresAt), node('div', 'subtext', relativeExpiry(spec.expiresAt, observedAt))))));
  }
  if (!entries.length) { const row = node('tr'); const td = cell(emptyMessage, 'empty-cell'); td.colSpan = 6; row.append(td); table.body.append(row); }
  return table.wrapper;
}
function overrideFilterOptions(key) {
  const entries = indexedOverrides(resources.overrides.data);
  const selectors = { cluster: (item) => item.record?.spec?.cluster, kind: (item) => item.record?.spec?.kind, status: (item) => item.record?.status?.state, scope: (item) => item.record?.spec?.scope?.capability };
  if (key === 'expiry') return [['', 'Any recorded expiry'], ['1h', 'Within 1 hour'], ['24h', 'Within 24 hours'], ['expired', 'Expired at observation'], ['unrecorded', 'Expiry not recorded']];
  if (key === 'lifecycle') return [['', 'All records'], ['current', 'Current / pending'], ['history', 'History']];
  const values = new Set(entries.map(({ item }) => selectors[key](item)).filter(Boolean));
  if (overrideFilters[key] && key !== 'scope') values.add(overrideFilters[key]);
  if (key === 'scope' && overrideFilters.scope.startsWith('cap:')) values.add(overrideFilters.scope.slice(4));
  return [['', `All ${ {cluster: 'clusters', kind: 'kinds', status: 'statuses', scope: 'scopes'}[key] }`], ...(key === 'scope' ? [['all-capabilities', 'All capabilities (unscoped)']] : []), ...[...values].sort().map((value) => [key === 'scope' ? `cap:${value}` : value, value])];
}
function updateOverrideFilterOptions() {
  for (const key of Object.keys(overrideFilters)) {
    const select = $(`override-filter-${key}`); if (!select) continue;
    const options = overrideFilterOptions(key);
    if (JSON.stringify([...select.options].map((option) => [option.value, option.text])) !== JSON.stringify(options)) select.replaceChildren(...options.map(([value, label]) => new Option(label, value)));
    select.value = overrideFilters[key];
  }
}
function renderOverrides() {
  const fragment = document.createDocumentFragment();
  const summary = node('div'); summary.id = 'override-summary'; fragment.append(summary);
  const upcoming = node('div'); upcoming.id = 'override-upcoming'; fragment.append(upcoming);
  const section = panel('Runtime overrides', 'Filter durable records while keeping publication and current serving counts distinct.');
  section.querySelector('.panel-heading').append(button('Clear filters', 'reset-override-filters', 'link-button'));
  const form = node('form', 'override-filters'); form.id = 'override-filter-form'; form.setAttribute('aria-describedby', 'override-filter-help');
  const labels = { cluster: 'CLUSTER', kind: 'KIND', status: 'RECORDED STATUS', scope: 'CAPABILITY SCOPE', expiry: 'EXPIRY WINDOW', lifecycle: 'CURRENT / HISTORY' };
  for (const key of Object.keys(overrideFilters)) {
    const select = node('select'); select.id = `override-filter-${key}`; select.dataset.overrideFilter = key;
    select.append(...overrideFilterOptions(key).map(([value, label]) => new Option(label, value))); select.value = overrideFilters[key];
    form.append(filterField(labels[key], select));
  }
  section.append(form);
  const help = node('div', 'table-note', 'All records are shown by default. History means elapsed expiry or Expired/Revoked/Rejected/Failed status, except records still serving jobs or published without a confirmed elapsed expiry. Other records, including unknown timing, are conservatively included in Current / pending. This does not mean an override is active; startsAt and expiry remain visible in details. Expiry windows use the override observation time.'); help.id = 'override-filter-help'; section.append(help);
  const results = node('div'); results.id = 'override-results'; section.append(results); fragment.append(section);
  fillOverrideView(summary, upcoming, results); renderedOverrideData = resources.overrides.data; renderedOverrideError = resources.overrides.error; return fragment;
}
function fillOverrideView(summary, upcoming, results) {
  const resource = resources.overrides; const data = resource.data;
  summary.replaceChildren(data ? node('div', 'stats', stat('Override records', formatNumber(data.overrides?.length || 0), 'Durable desired and status records'), stat('Published in snapshot', formatNumber(data.overrides?.filter((item) => item.published).length || 0), 'Presence does not imply effective serving'), stat('Serving jobs', formatNumber(data.overrides?.reduce((sum, item) => sum + (item.servingJobs || 0), 0) || 0), 'Current at the override observation'), stat('Pending', formatNumber(data.overrides?.filter((item) => item.record?.status?.state === 'Pending').length || 0), 'Awaiting policy workflow')) : node('div', 'notice info', resource.error ? 'Override observation unavailable.' : 'Waiting for override observation.'));
  upcoming.replaceChildren(renderUpcomingExpirations());
  if (!data) { results.replaceChildren(empty(resource.error ? 'Override observation unavailable' : 'Loading overrides', 'A missing observation does not mean there are no runtime rules.')); return; }
  const entries = filterOverrides(indexedOverrides(data), overrideFilters, data.observedAt);
  results.replaceChildren(node('div', 'table-note', `${entries.length} of ${data.overrides?.length || 0} records · Overrides observed `, time(data.observedAt), !data.ready ? ' · Serving policy not ready; durable records shown without confirmed publication.' : '', resource.error ? ' · Last successful observation; stale.' : ''), renderOverrideTable(entries, data.observedAt, 'No records match these filters.'));
}
function findOverrideOpener(id) {
  return [...$('content').querySelectorAll('[data-override-id]')].find((opener) => opener.dataset.overrideId === id);
}
function updateOverrideView(force = false) {
  updateOverrideFilterOptions();
  const resource = resources.overrides;
  if (!force && renderedOverrideData === resource.data && renderedOverrideError === resource.error) return;
  const focusedID = document.activeElement?.dataset.overrideId;
  fillOverrideView($('override-summary'), $('override-upcoming'), $('override-results'));
  renderedOverrideData = resource.data; renderedOverrideError = resource.error;
  if (focusedID) findOverrideOpener(focusedID)?.focus();
}
function clusterRuleSummary(entries, joined, observedAt, name) {
  if (!joined) return node('span', 'muted', 'Rules not joined. See the independent override observation below.');
  const published = entries.filter(({ item }) => item.published);
  const summary = node('div', 'cluster-rule-list');
  for (const { item, index } of published) {
    const spec = item.record?.spec || {};
    summary.append(node('div', 'cluster-rule', node('div', 'cluster-rule-heading', overrideButton(item, index), stateBadge(item.record?.status?.state), badge('Published', 'blue')), node('div', 'subtext', `${spec.kind || 'Unknown'}${spec.capacity !== undefined ? ` · requested weight ${spec.capacity}` : ''} · ${spec.scope?.capability || 'All capabilities'} · ${formatNumber(item.servingJobs)} serving jobs`), node('div', 'subtext', spec.reason || 'Reason not recorded'), node('div', 'subtext', 'Expires ', time(spec.expiresAt), ` · ${relativeExpiry(spec.expiresAt, observedAt)}`)));
  }
  if (!published.length) summary.append(node('span', 'muted', 'No published rules in the matching observation.'));
  summary.append(button(`View all ${entries.length} target records`, 'cluster-overrides', 'link-button', name));
  return summary;
}
function renderClusters() {
  const fragment = document.createDocumentFragment();
  const state = resources.state.data; const overrides = resources.overrides.data;
  const join = policyJoinStatus(resources.state, resources.overrides);
  fragment.append(node('div', `notice ${join.joined ? 'info' : ''}`, join.message));
  fragment.append(panel('Observation sources', 'Rules are joined only when both ready policies have the same generation and input digest.', node('div', 'policy-summary', field('INVENTORY OBSERVED', time(state?.observedAt)), field('INVENTORY POLICY', state?.policy ? `${state.policy.generation} · ${shortDigest(state.policy.digest)}` : 'Not ready'), field('OVERRIDES OBSERVED', time(overrides?.observedAt)), field('OVERRIDES POLICY', overrides?.policy ? `${overrides.policy.generation} · ${shortDigest(overrides.policy.digest)}` : 'Not ready')), node('div', 'table-note', resources.state.loading || resources.overrides.loading ? 'Refresh in progress. Displaying the last successful source observations where available.' : 'Source timestamps are observation times, not policy change times.')));
  fragment.append(renderUpcomingExpirations());
  const inventory = new Map((state?.clusters || []).map((cluster) => [cluster.name, cluster]));
  const entries = indexedOverrides(overrides);
  const names = new Set(inventory.keys());
  if (join.joined) for (const { item } of entries) if (item.record?.spec?.cluster) names.add(item.record.spec.cluster);
  if (!names.size) fragment.append(empty('Inventory observation unavailable', 'Inventory will appear once the state endpoint reports a serving policy. Override targets remain listed separately when available.'));
  if (names.size) {
    const section = panel('Cluster policy', 'Baseline inventory and placement with individually scoped published rules.');
    const table = createTable(['CLUSTER / INVENTORY', 'CAPABILITIES', 'BASELINE WEIGHT', 'JOBS', 'ESTIMATED DEMAND', 'PUBLISHED TEMPORARY RULES']);
    table.wrapper.classList.add('cluster-policy-table');
    for (const name of [...names].sort()) {
      const cluster = inventory.get(name);
      const inventoryStatus = cluster?.blocked ? badge('Blocked', 'amber') : !cluster?.inventoryKnown ? badge('Unknown inventory', 'amber') : badge('Not blocked', 'green');
      const identity = node('div', '', button(name, 'cluster-jobs', 'link-button cluster-name', name), node('div', 'subtext', cluster?.provider || 'Provider not recorded'), node('div', 'subtext', inventoryStatus));
      const targeted = join.joined ? entries.filter(({ item }) => item.record?.spec?.cluster === name) : [];
      table.body.append(node('tr', '', cell(identity), cell(cluster ? capabilities(cluster.capabilities) : node('span', 'muted', 'Unknown')), cell(cluster?.inventoryKnown ? formatNumber(cluster.configuredCapacity) : 'Unknown', 'numeric'), cell(cluster ? formatNumber(cluster.jobs) : '—', 'numeric'), cell(cluster ? formatNumber(cluster.demand) : '—', 'numeric'), cell(clusterRuleSummary(targeted, join.joined, overrides?.observedAt, name))));
    }
    section.append(table.wrapper, node('div', 'table-note', 'Jobs and demand use the state observation; rule serving counts use the override observation. Baseline weight is not physical capacity or availability. Scoped or overlapping rules are shown individually; no single effective weight is inferred.'));
    fragment.append(section);
  }
  if (!join.joined) {
    const separate = panel('Override targets · separate observation', 'These durable records are not joined to the inventory above. Unknown-inventory targets remain discoverable.');
    if (overrides) separate.append(node('div', 'table-note', 'Overrides observed ', time(overrides.observedAt), resources.overrides.error ? ' · Last successful observation; stale.' : ''), renderOverrideTable(entries, overrides.observedAt, overrides.ready ? 'No durable override records in this separate observation.' : 'Override policy is not ready; no rules can be confirmed.'));
    if (!overrides) separate.append(node('div', 'table-note', 'Override observation is unavailable or still loading. The absence of rows does not mean no temporary rules exist.'));
    fragment.append(separate);
  }
  return fragment;
}
function isGeneratedFallback(issue) {
  return issue.category === 'dispatcher-fallback' && ['rehearsal', 'aggregated', 'ephemeral'].includes(issue.family);
}
function diagnosticCategoryLabel(issue) {
  return { 'dispatcher-fallback': 'Fallback · cause unknown', 'api-throttling': 'API throttling · informational', communication: 'Communication', 'runtime-error': 'Runtime error' }[issue.category] || issue.category || 'Unclassified';
}
function matchesDiagnosticFilters(issue) {
  if (diagnosticFilters.category && issue.category !== diagnosticFilters.category) return false;
  const text = diagnosticFilters.search.trim().toLowerCase();
  return !text || [issue.message, issue.detail, issue.job, issue.cluster, issue.pod, issue.prowJob, issue.component, issue.level, diagnosticCategoryLabel(issue)].some((value) => typeof value === 'string' && value.toLowerCase().includes(text));
}
function resetDiagnosticDisplay() {
  diagnosticLimits.scheduler = 10; diagnosticLimits.dispatcher = 10;
  updateDiagnosticsView(true);
}
function expandDiagnosticSource(id) {
  const positions = ['scheduler', 'dispatcher'].map((source) => [source, $(`diagnostic-issues-${source}`)?.scrollTop || 0]);
  diagnosticLimits[id] = (diagnosticLimits[id] || 10) + 10;
  updateDiagnosticsView(true);
  for (const [source, top] of positions) { const list = $(`diagnostic-issues-${source}`); if (list) list.scrollTop = top; }
}
function diagnosticIssueButton(source, issue) {
  const opener = button(issue.message || 'Issue details', 'diagnostic-details', 'link-button diagnostic-message', `${source.id}:${issue.id}`);
  opener.dataset.diagnosticId = `${source.id}:${issue.id}`;
  return opener;
}
function findDiagnosticOpener(id) {
  return [...$('content').querySelectorAll('[data-diagnostic-id]')].find((opener) => opener.dataset.diagnosticId === id);
}
function diagnosticSourceCaveat() {
  return 'Collected from Kubernetes, including retained previous-container logs after restarts when available. Deleted pods, older restarts, and rotated-away logs cannot be recovered. The byte budget and request deadline can limit the sample. First/last timestamps do not prove continuous coverage.';
}
function diagnosticPodLabels(pod) {
  return {
    readiness: typeof pod.ready === 'boolean' ? pod.ready ? 'Pod ready' : 'Pod not ready' : 'Readiness unknown',
    restarts: typeof pod.restarts === 'number' && Number.isFinite(pod.restarts) && pod.restarts >= 0 ? `${formatNumber(pod.restarts)} restarts` : 'Restarts unknown',
    previous: { collected: 'Previous logs collected', partial: 'Previous logs partial', unavailable: 'Previous logs unavailable' }[pod.previousLogs] || '',
  };
}
function renderDiagnosticSource(source, title) {
  const section = panel(title, source.id === 'scheduler' ? 'External-lookup fallbacks, communication errors, and client throttling.' : 'Dispatcher runtime and reconcile observations.');
  const status = source.status || 'unavailable';
  section.querySelector('.panel-heading').append(node('div', 'diagnostic-source-badges', badge('Kubernetes pod logs'), badge(status === 'available' ? 'Source available' : status === 'partial' ? 'Partial collection' : 'Source unavailable', status === 'available' ? 'blue' : 'amber')));
  if (source.error) section.append(node('div', 'diagnostic-source-error', source.error));
  const coverage = node('details', 'diagnostic-coverage-details', node('summary', '', `Read ${formatLogBytes(source.bytesRead)} / ${formatLogBytes(source.byteLimit)} · Coverage details`));
  coverage.append(node('div', 'diagnostic-coverage', field('FIRST LOG SEEN', time(source.coveredFrom)), field('LAST LOG SEEN', time(source.coveredTo)), field('SCANNED / MATCHED LINES', `${formatNumber(source.scannedLines)} / ${formatNumber(source.matchedLines)}`)));
  const pods = node('div', 'diagnostic-pods');
  for (const pod of source.pods || []) {
    const labels = diagnosticPodLabels(pod);
    pods.append(node('div', 'diagnostic-pod', node('span', 'mono', pod.name), badge(labels.readiness, pod.ready === false ? 'amber' : ''), node('span', 'muted', labels.restarts), labels.previous ? badge(labels.previous, pod.previousLogs === 'collected' ? '' : 'amber') : null));
  }
  if (!source.pods?.length) pods.append(node('span', 'muted', 'No pod metadata available.'));
  coverage.append(pods);
  if (source.budgetNote) coverage.append(node('div', 'table-note', source.budgetNote));
  coverage.append(node('div', 'table-note', diagnosticSourceCaveat()));
  section.append(coverage);
  const reasons = (source.truncationReasons || []).map((reason) => ({ 'byte-limit': 'Byte limit reached', 'pod-limit': 'Pod limit reached', 'group-limit': 'Issue-group limit reached', 'read-incomplete': 'Log read incomplete', 'previous-logs-unavailable': 'Previous-container logs unavailable' }[reason] || reason));
  if (source.truncated || source.groupsLimited || reasons.length) section.append(node('div', 'diagnostic-source-error', reasons.length ? reasons.join(' · ') : 'The collection was limited; some events may be missing.'));
  const issues = source.issues || [];
  const collapsed = issues.filter(isGeneratedFallback);
  const visible = issues.filter((issue) => (showGeneratedFallbacks || !isGeneratedFallback(issue)) && matchesDiagnosticFilters(issue));
  const collapsedEvents = collapsed.reduce((sum, issue) => sum + (issue.count || 0), 0);
  section.append(node('div', 'table-note', `${visible.length} matching groups of ${issues.length} collected · ${formatNumber(visible.reduce((sum, issue) => sum + (issue.count || 0), 0))} matching events${!showGeneratedFallbacks && collapsed.length ? ` · ${collapsed.length} generated fallback groups (${formatNumber(collapsedEvents)} events) collapsed` : ''}`));
  const list = node('div', 'diagnostic-issues');
  list.id = `diagnostic-issues-${source.id}`; list.tabIndex = 0; list.setAttribute('role', 'region'); list.setAttribute('aria-label', `${title} issue groups`);
  for (const issue of visible.slice(0, diagnosticLimits[source.id] || 10)) {
    const color = issue.category === 'api-throttling' ? 'blue' : issue.category === 'dispatcher-fallback' ? 'amber' : 'red';
    list.append(node('article', 'diagnostic-issue', node('div', 'diagnostic-issue-heading', badge(diagnosticCategoryLabel(issue), color), badge(`${formatNumber(issue.count)} ${issue.count === 1 ? 'event' : 'events'}`)), diagnosticIssueButton(source, issue), node('div', 'subtext', [issue.job, issue.cluster, issue.component, issue.pod].filter(Boolean).join(' · ')), node('div', 'subtext', 'First ', time(issue.firstAt), ' · Last ', time(issue.lastAt)), issue.delaySeconds !== undefined ? node('div', 'subtext', `Maximum recorded client delay: ${formatNumber(issue.delaySeconds)}s. Informational throttling, not a failure.`) : null));
  }
  if (!visible.length) list.append(node('div', 'diagnostic-empty', status === 'unavailable' ? 'Source unavailable; no conclusion about runtime state can be drawn.' : issues.length && (diagnosticFilters.search.trim() || diagnosticFilters.category) ? 'No groups match these filters. Clear filters to inspect the collected groups.' : collapsed.length ? 'Only generated-job fallback groups are hidden. Enable “Show generated-job fallbacks” to inspect them.' : source.scannedLines === 0 ? 'No log lines were collected for this selection. No health conclusion can be drawn.' : 'No matching issue groups in the collected log sample. This is not a health assessment.'));
  section.append(list);
  if (visible.length > (diagnosticLimits[source.id] || 10)) section.append(node('div', 'diagnostic-more', button(`Show 10 more (${visible.length - (diagnosticLimits[source.id] || 10)} remaining)`, 'diagnostic-more', 'button', source.id)));
  return section;
}
function renderDiagnosticsResults() {
  const resource = resources.diagnostics; const data = resource.data;
  if (!data) return empty(resource.loading ? 'Collecting logs' : resource.error ? 'Diagnostics unavailable' : 'Collect logs when you need them', resource.loading ? 'Reading bounded scheduler and dispatcher log observations.' : resource.error ? 'Select Collect logs to retry. Policy observations are not required.' : 'Choose a lookback and select Collect logs. Opening this page, changing the window, or returning to it does not query logs.');
  if (!data.enabled) return empty('Diagnostics are disabled', 'Enable Kubernetes log diagnostics on the dashboard server to collect logs.');
  const fragment = document.createDocumentFragment();
  if (data.demo) fragment.append(node('div', 'notice info', 'DEMO DIAGNOSTICS · Canned log observations. No live log sources were queried.'));
  fragment.append(node('div', 'diagnostic-request-window', 'Requested window: ', diagnosticRequestedFrom(data), ' to ', time(data.requestedTo), '. Collection budget: ', formatLogBytes(data.byteLimit), ', shared between the two components. Kubernetes retention and collection limits can leave gaps.'));
  const grid = node('div', 'diagnostics-grid');
  for (const [id, title] of [['scheduler', 'Scheduler communication'], ['dispatcher', 'Dispatcher runtime']]) grid.append(renderDiagnosticSource(data.sources?.find((source) => source.id === id) || { id, status: 'unavailable', error: 'No observation was returned for this source.', issues: [], pods: [] }, title));
  fragment.append(grid); return fragment;
}
function validDiagnosticWindow(value) {
  return value === '30m' || value === 'available';
}
function diagnosticRequestedFrom(data) {
  return data.window === 'available' ? 'No time cutoff' : time(data.requestedFrom);
}
function clearDiagnosticResults() {
  const resource = resources.diagnostics;
  resource.controller?.abort(); resource.sequence++; resource.data = null; resource.error = ''; resource.fetchedAt = null; resource.loading = false;
  diagnosticLimits.scheduler = 10; diagnosticLimits.dispatcher = 10;
}
async function collectDiagnosticLogs() {
  if (view !== 'diagnostics' || resources.diagnostics.loading || !validDiagnosticWindow(diagnosticsWindow)) return;
  await loadResource('diagnostics', true, true);
}
function onToolbarRefresh() {
  if (view === 'diagnostics') { collectDiagnosticLogs(); return; }
  refresh();
}
function renderDiagnostics() {
  const fragment = document.createDocumentFragment();
  const controls = node('div', 'diagnostics-controls'); controls.id = 'diagnostics-controls';
  const windowSelect = node('select'); windowSelect.id = 'diagnostics-window';
  windowSelect.append(new Option('Last 30 minutes', '30m'), new Option('As much as available within the limit', 'available')); windowSelect.value = diagnosticsWindow;
  const show = node('input'); show.type = 'checkbox'; show.id = 'diagnostics-generated'; show.checked = showGeneratedFallbacks;
  const label = node('label', 'diagnostic-toggle', show, 'Show generated-job fallbacks');
  controls.append(filterField('LOG LOOKBACK', windowSelect), label);
  const filters = node('div', 'diagnostic-filters');
  const search = node('input'); search.id = 'diagnostics-search'; search.type = 'search'; search.placeholder = 'Message, job, cluster or pod'; search.autocomplete = 'off'; search.value = diagnosticFilters.search;
  const searchField = filterField('FILTER COLLECTED RESULTS', search); searchField.classList.add('diagnostic-search');
  const category = node('select'); category.id = 'diagnostics-category';
  category.append(new Option('All categories', ''), new Option('Communication', 'communication'), new Option('Dispatcher fallback', 'dispatcher-fallback'), new Option('API throttling', 'api-throttling'), new Option('Runtime error', 'runtime-error')); category.value = diagnosticFilters.category;
  filters.append(searchField, filterField('CATEGORY', category), button('Clear filters', 'diagnostic-clear', 'button'));
  const help = node('p', 'diagnostic-lookback-help', 'Manual collection only. Choose the last 30 minutes or retained Kubernetes logs without a time cutoff. Both use the same collection budget.');
  const results = node('div'); results.id = 'diagnostics-results'; results.append(renderDiagnosticsResults());
  fragment.append(controls, help, filters, results);
  renderedDiagnosticsData = resources.diagnostics.data; renderedDiagnosticsError = resources.diagnostics.error; renderedDiagnosticsLoading = resources.diagnostics.loading;
  updateDiagnosticControls(); return fragment;
}
function updateDiagnosticControls() {
  $('refresh').disabled = resources.diagnostics.loading || !validDiagnosticWindow(diagnosticsWindow);
}
function updateDiagnosticsView(force = false) {
  updateDiagnosticControls();
  const resource = resources.diagnostics;
  if (!force && renderedDiagnosticsData === resource.data && renderedDiagnosticsError === resource.error && renderedDiagnosticsLoading === resource.loading) return;
  const focused = document.activeElement;
  const diagnosticID = focused?.dataset.diagnosticId;
  const action = focused?.dataset.action; const value = focused?.dataset.value;
  $('diagnostics-results').replaceChildren(renderDiagnosticsResults());
  $('content').setAttribute('aria-busy', String(resource.loading));
  renderedDiagnosticsData = resource.data; renderedDiagnosticsError = resource.error; renderedDiagnosticsLoading = resource.loading;
  if (focused?.isConnected) return;
  if (diagnosticID) (findDiagnosticOpener(diagnosticID) || $('diagnostics-window')).focus();
  else if (action) {
    const opener = [...$('diagnostics-results').querySelectorAll('[data-action]')].find((candidate) => candidate.dataset.action === action && candidate.dataset.value === value);
    const fallback = action === 'diagnostic-more' ? $(`diagnostic-issues-${value}`) : $('diagnostics-window');
    (opener || fallback)?.focus({ preventScroll: action === 'diagnostic-more' });
  }
}
function diagnosticDetails(key) {
  const data = resources.diagnostics.data;
  for (const source of data?.sources || []) {
    const issue = source.issues?.find((item) => `${source.id}:${item.id}` === key);
    if (!issue) continue;
    openDetails(issue.message || 'Diagnostic issue', 'DIAGNOSTIC OBSERVATION', [detailSection('Grouped issue', definitions([['Category', diagnosticCategoryLabel(issue)], ['Source', source.id], ['Component', issue.component], ['Reported level', issue.level], ['Observed count', formatNumber(issue.count)], ['First recorded', time(issue.firstAt)], ['Last recorded', time(issue.lastAt)], ['Job', issue.job], ['Job family', issue.family], ['Cluster', issue.cluster], ['ProwJob', issue.prowJob], ['Pod', issue.pod], ...(issue.delaySeconds !== undefined ? [['Maximum recorded client delay', `${formatNumber(issue.delaySeconds)} seconds`]] : [])])), detailSection('Diagnostic detail', node('p', '', issue.detail || 'No additional detail recorded.')), detailSection('Interpretation and log limits', node('p', '', issue.category === 'dispatcher-fallback' ? 'The scheduler selected Spec.Cluster after an external lookup failure. The underlying cause was not logged; this warning cannot distinguish missing configuration, transport failure, response status, or decoding errors.' : issue.category === 'api-throttling' ? 'This is a client-side informational delay, not a failure by itself.' : 'This event is limited to the collected log sample and does not establish overall service health.'), definitions([['First log seen', time(source.coveredFrom)], ['Last log seen', time(source.coveredTo)], ['Requested from', diagnosticRequestedFrom(data)], ['Requested to', time(data.requestedTo)], ['Observed at', time(data.observedAt)], ['Source status', source.status], ['Log source', 'Kubernetes pod logs'], ['Bytes read', formatLogBytes(source.bytesRead)], ['Component budget', formatLogBytes(source.byteLimit)], ['Collection budget', formatLogBytes(data.byteLimit)]]), node('p', '', diagnosticSourceCaveat()))]);
    return;
  }
}
function detailSection(title, ...children) { return node('section', 'drawer-section', node('h3', '', title), children); }
function definitions(pairs) { return node('dl', 'detail-list', pairs.flatMap(([label, value]) => [node('dt', '', label), node('dd', '', value ?? 'Not recorded')])); }
function policyDetails(policy) {
  if (!policy) return detailSection('Policy unavailable', node('p', '', 'No serving policy has been observed.'));
  return detailSection('Snapshot identity', definitions([['Generation', policy.generation], ['Input digest', node('span', 'mono', policy.digest || 'Not recorded')], ['Checksum', node('span', 'mono', policy.checksum || 'Not recorded')], ['Policy changed', time(policy.generatedAt)], ['Published override IDs', (policy.overrideIDs || []).join(', ') || 'None']]));
}
function openDetails(title, kind, sections) {
  returnFocus = document.activeElement;
  returnOverrideID = returnFocus?.dataset.overrideId || null;
  returnDiagnosticID = returnFocus?.dataset.diagnosticId || null;
  $('detail-title').textContent = title; $('detail-kind').textContent = kind; $('detail-content').replaceChildren(...sections);
  if (!$('details').open) $('details').showModal();
  $('details').scrollTop = 0; $('close-details').focus();
}
function closeDetails() { $('details').close(); }
function jobDetails(index) {
  const data = resources.jobs.data; const job = data?.jobs?.[index]; if (!job) return;
  const decision = job.decision || {};
  openDetails(job.name, 'JOB PLACEMENT', [detailSection('Effective decision', definitions([['Effective cluster', decision.cluster], ['Decision source', sourceBadge(decision.source)], ['Explanation', decision.explanation || 'Not recorded'], ['Override ID', decision.overrideID || 'None'], ['Valid until', time(decision.validUntil)], ['Generation', decision.policyGeneration], ['Policy digest', node('span', 'mono', decision.policyDigest || 'Not recorded')]])), detailSection('Baseline requirements', definitions([['Group', job.group || 'Ungrouped'], ['Baseline cluster', job.baselineCluster], ['Estimated demand', formatNumber(job.demand)], ['Capabilities', capabilities(job.capabilities)], ['Observed at', time(data.observedAt)]]))]);
}
function safeLink(value) {
  try { const url = new URL(value); if (!['https:', 'http:'].includes(url.protocol)) return node('span', '', value); const anchor = node('a', '', value); anchor.href = url.href; anchor.target = '_blank'; anchor.rel = 'noopener noreferrer'; return anchor; } catch { return node('span', '', value); }
}
function overrideDetails(index) {
  const data = resources.overrides.data; const item = data?.overrides?.[index]; if (!item) return;
  const record = item.record || {}; const spec = record.spec || {}; const status = record.status || {};
  const approvals = spec.approvals || [];
  const sections = [detailSection('Publication and effective serving', definitions([['Recorded status', stateBadge(status.state)], ['Status message', status.message || 'Not recorded'], ['In serving snapshot', item.published ? 'Published' : 'Not published'], ['Effective serving jobs', formatNumber(item.servingJobs)], ['Observed at', time(data.observedAt)]])), detailSection('Requested policy change', definitions([['Kind', spec.kind], ['Cluster', spec.cluster], ['Capability scope', spec.scope?.capability || 'All capabilities'], ...(spec.capacity !== undefined ? [['Scheduling weight', spec.capacity]] : []), ['Starts at', time(spec.startsAt)], ['Expires at', time(spec.expiresAt)], ['Created by', spec.createdBy], ['Reason', spec.reason], ...(spec.incidentURL ? [['Incident', safeLink(spec.incidentURL)]] : []), ...(spec.revokedBy ? [['Revoked by', spec.revokedBy], ['Revoked at', time(spec.revokedAt)]] : [])])), detailSection(`Approvals (${approvals.length}/${spec.requiredApprovals ?? '—'})`, approvals.length ? definitions(approvals.map((approval) => [approval.userID, time(approval.at)])) : node('p', '', 'No approvals recorded.'))];
  const history = status.history || [];
  const timeline = node('ol', 'audit', history.map((event) => node('li', '', node('div', '', stateBadge(event.state)), node('div', 'audit-meta', time(event.at), ` · ${event.actor || 'Actor not recorded'}${event.generation ? ` · Generation ${event.generation}` : ''}`), node('p', '', event.message || 'No message recorded.'))));
  sections.push(detailSection('Audit history', history.length ? timeline : node('p', '', 'No audit events recorded.')));
  openDetails(spec.id || record.metadata?.name || 'Override', 'RUNTIME OVERRIDE', sections);
}
function onAction(event) {
  const target = event.target.closest('[data-action]'); if (!target) return;
  const action = target.dataset.action;
  if (action === 'policy-details') openDetails(`Policy generation ${resources.state.data?.policy?.generation ?? '—'}`, 'SERVING POLICY', [policyDetails(resources.state.data?.policy), detailSection('Observation time', definitions([['Observed at', time(resources.state.data?.observedAt)], ['Successful refresh', time(resources.state.fetchedAt)]])), detailSection('Timestamp meaning', node('p', '', 'Policy changed records the time this snapshot was generated. It is not a heartbeat or the time of the last successful reconcile.'))]);
  if (action === 'diagnostic-details') diagnosticDetails(target.dataset.value);
  if (action === 'diagnostic-more') expandDiagnosticSource(target.dataset.value);
  if (action === 'diagnostic-clear') { diagnosticFilters.search = ''; diagnosticFilters.category = ''; $('diagnostics-search').value = ''; $('diagnostics-category').value = ''; resetDiagnosticDisplay(); }
  if (action === 'cluster-jobs') navigate('jobs', target.dataset.value);
  if (action === 'reset-override-filters') { for (const key of Object.keys(overrideFilters)) overrideFilters[key] = ''; updateOverrideView(true); }
  if (action === 'cluster-overrides') { for (const key of Object.keys(overrideFilters)) overrideFilters[key] = ''; overrideFilters.cluster = target.dataset.value; navigate('overrides'); }
  if (action === 'job-details') jobDetails(Number(target.dataset.value));
  if (action === 'override-details') overrideDetails(Number(target.dataset.value));
  if (action === 'clear-filters') { filters = { name: '', group: '', cluster: '' }; submittedFilters = null; lookupValidation = ''; resetPagination(); clearJobResults(); paginationNotice = ''; syncURL(); $('content').replaceChildren(); render(); $('filter-name').focus(); }
  if (action === 'next-page' && submittedFilters && !resources.jobs.loading && resources.jobs.data?.nextAfter) { pages = pages.slice(0, pageIndex + 1); pages.push(resources.jobs.data.nextAfter); pageIndex++; clearJobResults(); render(); loadResource('jobs'); }
  if (action === 'previous-page' && submittedFilters && !resources.jobs.loading && pageIndex > 0) { pageIndex--; clearJobResults(); render(); loadResource('jobs'); }
}
function onFilterChange(event) {
  const diagnosticKey = { 'diagnostics-search': 'search', 'diagnostics-category': 'category' }[event.target.id];
  if (diagnosticKey) { diagnosticFilters[diagnosticKey] = event.target.value; resetDiagnosticDisplay(); return; }
  if (event.target.id === 'diagnostics-generated') { showGeneratedFallbacks = event.target.checked; updateDiagnosticsView(true); return; }
  if (event.target.id === 'diagnostics-window') {
    diagnosticsWindow = event.target.value;
    clearDiagnosticResults(); syncURL(); renderFreshness(); renderNotices(); updateDiagnosticsView(true); return;
  }
  const overrideKey = event.target.dataset.overrideFilter;
  if (overrideKey) { overrideFilters[overrideKey] = event.target.value; updateOverrideView(true); return; }
  const key = { 'filter-name': 'name', 'filter-group': 'group', 'filter-cluster': 'cluster' }[event.target.id]; if (!key) return;
  filters[key] = event.target.value; submittedFilters = null; lookupValidation = ''; resetPagination(); clearJobResults(); paginationNotice = ''; syncURL();
  renderFreshness(); renderNotices(); updateLookupForm(); updateJobsResults();
}
function submitLookup(event) {
  if (event.target.id === 'override-filter-form') { event.preventDefault(); return; }
  if (event.target.id !== 'job-lookup-form') return;
  event.preventDefault();
  const criteria = { name: filters.name.trim(), group: filters.group.trim(), cluster: filters.cluster };
  if (!criteria.name && !criteria.group) { lookupValidation = 'Enter a job name or exact group before searching.'; updateLookupForm(); $('filter-name').focus(); return; }
  filters = criteria; submittedFilters = { ...criteria }; lookupValidation = ''; paginationNotice = ''; resetPagination(); clearJobResults(); syncURL();
  $('filter-name').value = filters.name; $('filter-group').value = filters.group;
  loadResource('jobs');
}
$('content').addEventListener('click', onAction);
$('content').addEventListener('input', onFilterChange);
$('content').addEventListener('submit', submitLookup);
$('refresh').addEventListener('click', onToolbarRefresh);
$('auto-refresh').addEventListener('change', scheduleRefresh);
document.querySelector('nav').addEventListener('click', (event) => { const link = event.target.closest('[data-view]'); if (!link || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return; event.preventDefault(); navigate(link.dataset.view); });
$('close-details').addEventListener('click', closeDetails);
$('done-details').addEventListener('click', closeDetails);
$('details').addEventListener('click', (event) => { if (event.target === $('details') && event.clientX < $('details').getBoundingClientRect().left) closeDetails(); });
$('details').addEventListener('close', () => {
  if (returnFocus?.isConnected) { returnFocus.focus(); return; }
  if (returnOverrideID) { const opener = findOverrideOpener(returnOverrideID); if (opener) { opener.focus(); return; } }
  if (returnDiagnosticID) { const opener = findDiagnosticOpener(returnDiagnosticID); if (opener) { opener.focus(); return; } }
  $('main').focus();
});
document.addEventListener('visibilitychange', () => { renderFreshness(); if (view !== 'diagnostics' && !document.hidden && $('auto-refresh').checked) refresh(); });
window.addEventListener('popstate', () => { const previousView = view; const url = new URL(location.href); view = ['overview', 'clusters', 'jobs', 'overrides', 'diagnostics'].includes(url.searchParams.get('view')) ? url.searchParams.get('view') : 'overview'; filters = { name: url.searchParams.get('name') || '', group: url.searchParams.get('group') || '', cluster: url.searchParams.get('cluster') || '' }; submittedFilters = null; lookupValidation = ''; paginationNotice = ''; resetPagination(); clearJobResults(); diagnosticsWindow = url.searchParams.get('window') === 'available' ? 'available' : '30m'; clearDiagnosticResults(); $('content').replaceChildren(); render(); refreshForViewChange(previousView); });
render(); refresh(); scheduleRefresh();
