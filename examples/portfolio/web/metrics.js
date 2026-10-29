const $=id=>document.getElementById(id);
const state={errors:{}};
const showErrors=()=>{$('lbError').textContent=Object.values(state.errors).join(' · ')};
const exact=value=>{try{if(typeof value==='number'&&!Number.isSafeInteger(value))return 'Unavailable';if(!/^-?\d+$/.test(String(value)))return 'Unavailable';return BigInt(value).toLocaleString()}catch{return 'Unavailable'}};
const age = timestamp => typeof timestamp === 'number' && Number.isFinite(timestamp) ? Math.max(0, Math.round(Date.now()/1000-timestamp)) : null;
const measured = timestamp => age(timestamp) === null ? 'unavailable' : `${age(timestamp)}s ago`;
const bytes = value => typeof value !== 'number' || !Number.isFinite(value) || value < 0 ? 'Unavailable' : value < 1024 ? `${value} B` : value < 1048576 ? `${(value/1024).toFixed(1)} KiB` : `${(value/1048576).toFixed(1)} MiB`;
const latency = value => value?.p50_ms == null ? 'Unavailable' : `${value.p50_ms.toLocaleString()} ms`;
const distribution = value => !value?.samples ? 'No observations in this window' : `p95 ${value.p95_ms == null ? 'unavailable' : value.p95_ms.toLocaleString()+' ms'} · ${exact(value.samples)} samples`;

function metric(title, value, detail) {
  const item = document.createElement('div'); const term = document.createElement('dt'); term.textContent = title;
  const data = document.createElement('dd'); data.textContent = value; const note = document.createElement('small'); note.textContent = detail;
  item.append(term, data, note); return item;
}

async function measurements() {
  if (document.getElementById('metricsPanel').hidden) return;
  try {
    const response = await fetch('/metrics.json', {cache: 'no-store'});
    if (!response.ok) throw Error(`Query measurements: HTTP ${response.status}`);
    const documentMetrics = await response.json();
    $('lbMetrics').replaceChildren(...['totals', 'top10'].map(name => {
      const query = documentMetrics.queries?.[name] || {};
      const card = document.createElement('article'); card.className = 'lb-query-card';
      const title = document.createElement('h3'); title.textContent = name === 'totals' ? 'All-products query' : 'Top-ten query';
      card.dataset.query = name;
      const stale = ['flow','worker','latency','engine','state'].some(key => age(query[key]?.observed_at) === null || age(query[key]?.observed_at) > (key === 'state' ? 150 : 60));
      if (stale) card.classList.add('stale');
      const note = document.createElement('p'); note.textContent = `${stale ? 'Stale or unavailable · ' : ''}Progress measured ${measured(query.flow?.observed_at)} · ${exact(query.flow?.engine_epochs)} epochs · ${query.engine?.phase || 'phase unavailable'}`;
      const list = document.createElement('dl'); list.className = 'lb-values';
      list.append(
        metric('Processing p50', latency(query.latency?.engine), distribution(query.latency?.engine)),
        metric('Receipt → sink p50', latency(query.latency?.write_to_sink), distribution(query.latency?.write_to_sink)),
        metric('Source commit → sink', query.latency?.observer_healthy ? latency(query.latency.source_commit_to_sink) : 'Unavailable', query.latency?.observer_healthy ? distribution(query.latency.source_commit_to_sink) : query.latency?.observer_status === 'unconfigured' ? 'No observer configured for this query' : `Observer ${query.latency?.observer_status || 'unavailable'}`),
        metric('Worker memory', bytes(query.worker?.rss_bytes), `RSS · measured ${measured(query.worker?.observed_at)}`),
        metric('Published source transactions', exact(query.latency?.published_source_transactions), `${exact(query.latency?.published_epochs)} publication epochs · preceding 60 seconds`),
        metric('Checkpoint p50', latency(query.latency?.checkpoint), distribution(query.latency?.checkpoint)),
        metric('Input preparation p50', latency(query.latency?.phases?.inputs), distribution(query.latency?.phases?.inputs)),
        metric('Engine preparation p50', latency(query.latency?.phases?.prepare_including_object_io), distribution(query.latency?.phases?.prepare_including_object_io)),
        metric('Sink publication p50', latency(query.latency?.phases?.publication), distribution(query.latency?.phases?.publication)),
        metric('Memtable', bytes(query.engine?.memtable_bytes), `${bytes(query.engine?.memtable_limit)} limit · ${exact(query.engine?.runs)} arrangement runs`),
        metric('Decoded cache / live allocations', `${bytes(query.engine?.cache?.bytes)} / ${bytes(query.engine?.cache?.live_bytes)}`, `${exact(query.engine?.cache?.entries)} cached entries · measured ${measured(query.engine?.observed_at)}`),
        metric('Index / key / filter caches', `${bytes(query.engine?.cache?.index_bytes)} / ${bytes(query.engine?.cache?.point_bytes)} / ${bytes(query.engine?.cache?.filter_bytes)}`, 'Resident indexes / logical keys / Bloom filters'),
        metric('Cache hits / misses', `${exact(query.engine?.cache?.hits)} / ${exact(query.engine?.cache?.misses)}`, `${exact(query.engine?.cache?.decoded_rows)} decoded rows · ${bytes(query.engine?.cache?.decoded_bytes)} decoded · process counters`),
        ...['foreground','maintenance'].map(scope => metric(`${scope === 'foreground' ? 'Foreground' : 'Maintenance'} object service`, `${exact(query.engine?.object_service?.[scope]?.requests)} requests`, `${bytes(query.engine?.object_service?.[scope]?.read_bytes)} read · ${bytes(query.engine?.object_service?.[scope]?.written_bytes)} written · process counters`)),
        metric('R2 physical state', bytes(query.state?.bytes), `${exact(query.state?.objects)} objects · sampled ${measured(query.state?.observed_at)}`),
        metric('Unconfirmed WAL', bytes(query.flow?.unconfirmed_wal_bytes), `${bytes(query.flow?.replay_bytes)} ahead of object checkpoint`),
        metric('Retained WAL / sink storage', `${bytes(query.flow?.retained_wal_bytes)} / ${bytes(query.flow?.sink_bytes)}`, 'This query’s slot / result relation'),
        metric(name === 'totals' ? 'All products / line items' : 'Selected products / line items', `${exact(query.flow?.products)} / ${exact(query.flow?.order_lines)}`, `Measured ${measured(query.flow?.observed_at)}`),
        metric(name === 'totals' ? 'All-product net revenue cents' : 'Selected net revenue cents', exact(query.flow?.net_revenue_cents), 'SUM over this query’s result only')
      );
      const progress = document.createElement('p'); progress.className = 'lb-progress';
      progress.textContent = `Published LSN ${query.flow?.end_lsn || 'unavailable'} · checkpoint ${query.flow?.checkpoint_lsn || 'unavailable'} · engine diagnostics ${measured(query.engine?.observed_at)}${query.errors?.length ? ' · '+query.errors.join(' · ') : ''}`;
      card.append(title, note, list, progress); return card;
    }));
    if (documentMetrics.errors?.length) state.errors.metrics = documentMetrics.errors.join(' · ');
    else delete state.errors.metrics;
    showErrors();
  } catch (error) { state.errors.metrics = error.message; showErrors(); }
}

measurements();setInterval(measurements,10000);
