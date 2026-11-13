import theme from './theme.css'

const origin = 'https://13.140.231.151.sslip.io'

const publicPaths = new Set([
  '/',
  '/index.html',
  '/storefront.css',
  '/metrics.css',
  '/history.css',
  '/supabase.js',
  '/storefront.js',
  '/metrics.js',
  '/history.js',
  '/config.json',
  '/activity.json',
  '/metrics.json',
  '/metrics-history.json',
  '/metrics-history-top10.json',
  '/rest/v1/product_totals',
  '/rest/v1/product_top10',
  '/realtime/v1/websocket',
])

export default {
  async fetch(request) {
    if (request.method !== 'GET' && request.method !== 'HEAD') {
      return new Response('Method not allowed', { status: 405, headers: { Allow: 'GET, HEAD' } })
    }

    const url = new URL(request.url)
    if (url.pathname === '/pgderive-theme.css') {
      return new Response(request.method === 'HEAD' ? null : theme, {
        headers: { 'Content-Type': 'text/css; charset=utf-8' },
      })
    }
    if (!publicPaths.has(url.pathname)) {
      return new Response('Not found', { status: 404 })
    }

    const upstream = new URL(origin)
    upstream.pathname = url.pathname
    upstream.search = url.search
    const forwarded = url.pathname.startsWith('/rest/')
      ? ['accept', 'accept-profile', 'apikey', 'authorization']
      : ['accept', 'connection', 'upgrade', 'sec-websocket-key', 'sec-websocket-version', 'sec-websocket-protocol']
    const headers = new Headers()
    for (const name of forwarded) {
      const value = request.headers.get(name)
      if (value !== null) headers.set(name, value)
    }
    const response = await fetch(upstream, { method: request.method, headers, redirect: 'manual' })

    if (request.method === 'GET' && response.ok && response.headers.get('content-type')?.includes('text/html')) {
      return new HTMLRewriter()
        .on('head', {
          element(head) {
            head.append('<link rel="stylesheet" href="/pgderive-theme.css">', { html: true })
          },
        })
        .on('footer a[href="https://github.com/jlerche/pgderive"]', {
          element(link) {
            link.setAttribute('href', 'https://pgderive.com/')
          },
        })
        .transform(response)
    }
    return response
  },
}
