const $ = id => document.getElementById(id);
const config = await (await fetch('/config.json', {cache: 'no-store'})).json();
const state = {top10: [], events: 0, movements: [], errors: {}, snapshots: {}};
window.pgderiveDemo = state;
const integer = value => {
  if (typeof value === 'number' && !Number.isSafeInteger(value)) throw Error('Value exceeds exact demo range');
  if (!/^-?\d+$/.test(String(value))) throw Error('Expected an exact integral demo value');
  return BigInt(value);
};
const exact = value => { try { return integer(value).toLocaleString(); } catch { return 'Unavailable'; } };
const nowText = () => new Date().toLocaleTimeString();
const showErrors = () => { $('lbError').textContent = Object.values(state.errors).join(' · '); };
const age = timestamp => typeof timestamp === 'number' && Number.isFinite(timestamp) ? Math.max(0, Math.round(Date.now()/1000-timestamp)) : null;
const measured = timestamp => age(timestamp) === null ? 'unavailable' : `${age(timestamp)}s ago`;
const bytes = value => typeof value !== 'number' || !Number.isFinite(value) || value < 0 ? 'Unavailable' : value < 1024 ? `${value} B` : value < 1048576 ? `${(value/1024).toFixed(1)} KiB` : `${(value/1048576).toFixed(1)} MiB`;
const latency = value => value?.p50_ms == null ? 'Unavailable' : `${value.p50_ms.toLocaleString()} ms`;
const distribution = value => !value?.samples ? 'No observations in this window' : `p95 ${value.p95_ms == null ? 'unavailable' : value.p95_ms.toLocaleString()+' ms'} · ${exact(value.samples)} samples`;

function rows(next) {
  if (state.top10.length) {
    const before = new Set(state.top10.map(row => row.book_id));
    const after = new Set(next.map(row => row.book_id));
    const entered = [...after].filter(id => !before.has(id));
    const exited = [...before].filter(id => !after.has(id));
    if (entered.length || exited.length) state.movements.unshift({entered, exited, at: nowText()});
    state.movements = state.movements.slice(0, 6);
    if (state.movements.length) $('lbMovements').replaceChildren(...state.movements.map(item => {
      const li = document.createElement('li'); li.textContent = `Book ${item.entered.join(', ')} entered · book ${item.exited.join(', ')} left`;
      const time = document.createElement('time'); time.textContent = item.at; li.append(time); return li;
    }));
  }
  state.top10 = next;
  $('lbRows').replaceChildren(...next.map((row, index) => {
    const tr = document.createElement('tr');
    for (const value of [String(index+1).padStart(2, '0'), `Book ${row.book_id}`, exact(row.positions), row.units === null ? 'NULL' : exact(row.units)]) {
      const td = document.createElement('td'); td.textContent = value; tr.append(td);
    }
    return tr;
  }));
}

let running = false, again = false;
async function refresh() {
  if (running) { again = true; return; }
  running = true;
  try {
    const response = await fetch(`/rest/v1/${config.leaderboardTable}?select=book_id::text,positions::text,units::text&order=units.desc.nullslast,book_id.asc`, {
      cache: 'no-store', headers: {apikey: config.anonKey, Authorization: `Bearer ${config.anonKey}`, 'Accept-Profile': config.leaderboardSchema}
    });
    if (!response.ok) throw Error(`Leaderboard snapshot: HTTP ${response.status}`);
    const next = await response.json();
    next.forEach(row => { integer(row.book_id); integer(row.positions); if (row.units !== null) integer(row.units); });
    rows(next); state.snapshots.top10 = Date.now(); $('lbFresh').textContent = `Authoritative top-ten snapshot · ${nowText()}`;
    delete state.errors.snapshot; showErrors();
  } catch (error) { state.errors.snapshot = error.message; showErrors(); }
  finally { running = false; if (again) { again = false; refresh(); } }
}

function metric(title, value, detail) {
  const item = document.createElement('div'); const term = document.createElement('dt'); term.textContent = title;
  const data = document.createElement('dd'); data.textContent = value; const note = document.createElement('small'); note.textContent = detail;
  item.append(term, data, note); return item;
}

async function measurements() {
  try {
    const response = await fetch('/metrics.json', {cache: 'no-store'});
    if (!response.ok) throw Error(`Query measurements: HTTP ${response.status}`);
    const documentMetrics = await response.json();
    $('lbMetrics').replaceChildren(...['totals', 'top10'].map(name => {
      const query = documentMetrics.queries?.[name] || {};
      const card = document.createElement('article'); card.className = 'lb-query-card';
      const title = document.createElement('h3'); title.textContent = name === 'totals' ? 'Complete totals query' : 'Top-ten query';
      const stale = !query.flow || age(query.flow.observed_at) > 60 || !query.worker || age(query.worker.observed_at) > 60 || !query.latency || age(query.latency.observed_at) > 60 || !query.state || age(query.state.observed_at) > 150;
      if (stale) card.classList.add('stale');
      const note = document.createElement('p'); note.textContent = `${stale ? 'Stale or unavailable · ' : ''}Progress measured ${measured(query.flow?.observed_at)} · ${exact(query.flow?.engine_epochs)} epochs`;
      const list = document.createElement('dl'); list.className = 'lb-values';
      list.append(
        metric('Processing p50', latency(query.latency?.engine), distribution(query.latency?.engine)),
        metric('Receipt → sink p50', latency(query.latency?.write_to_sink), distribution(query.latency?.write_to_sink)),
        metric('Source commit → sink', query.latency?.observer_healthy ? latency(query.latency.source_commit_to_sink) : name === 'top10' ? 'Not observed' : 'Unavailable', query.latency?.observer_healthy ? distribution(query.latency.source_commit_to_sink) : name === 'top10' ? 'No independent observer for this query' : 'Configured observer is unavailable or stale'),
        metric('Worker memory', bytes(query.worker?.rss_bytes), `RSS · measured ${measured(query.worker?.observed_at)}`),
        metric('R2 physical state', bytes(query.state?.bytes), `${exact(query.state?.objects)} objects · sampled ${measured(query.state?.observed_at)}`),
        metric('Unconfirmed WAL', bytes(query.flow?.unconfirmed_wal_bytes), `${bytes(query.flow?.replay_bytes)} ahead of object checkpoint`),
        metric(name === 'totals' ? 'All books / positions' : 'Selected books / positions', `${exact(query.flow?.books)} / ${exact(query.flow?.positions)}`, `Measured ${measured(query.flow?.observed_at)}`),
        metric(name === 'totals' ? 'All-book net units' : 'Selected net units', exact(query.flow?.units), 'SUM over this query’s result only')
      );
      card.append(title, note, list); return card;
    }));
    if (documentMetrics.errors?.length) state.errors.metrics = documentMetrics.errors.join(' · ');
    else delete state.errors.metrics;
    showErrors();
  } catch (error) { state.errors.metrics = error.message; showErrors(); }
}

const client = supabase.createClient(location.origin, config.anonKey, {auth: {persistSession: false, autoRefreshToken: false}});
client.channel('portfolio-top-ten').on('postgres_changes', {event: '*', schema: config.leaderboardSchema, table: config.leaderboardTable}, payload => {
  if (payload.errors?.length) { state.errors.realtime = payload.errors.join('; '); showErrors(); return; }
  delete state.errors.realtime; state.events += 1; $('lbEvents').textContent = `${state.events.toLocaleString()} leaderboard events`; refresh();
}).subscribe(status => {
  $('lbConnection').textContent = status === 'SUBSCRIBED' ? 'TOP TEN · CONNECTED' : status;
  if (status === 'SUBSCRIBED') { delete state.errors.realtime; refresh(); }
  else if (['TIMED_OUT', 'CHANNEL_ERROR', 'CLOSED'].includes(status)) state.errors.realtime = `Leaderboard Realtime: ${status}`;
  showErrors();
});
refresh(); measurements(); setInterval(refresh, 3000); setInterval(measurements, 10000);
