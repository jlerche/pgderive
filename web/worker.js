const emailPattern = /^[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$/

export default {
  async fetch(request, env) {
    const url = new URL(request.url)
    if (url.pathname !== '/api/early-access') return env.ASSETS.fetch(request)
    if (request.method !== 'POST') {
      return new Response(null, { status: 405, headers: { Allow: 'POST' } })
    }

    if (request.headers.get('Origin') !== url.origin) return new Response(null, { status: 403 })

    const key = request.headers.get('CF-Connecting-IP') ?? 'local'
    const { success } = await env.SIGNUP_LIMIT.limit({ key })
    if (!success) return new Response(null, { status: 429 })

    const reader = request.body?.getReader()
    if (!reader) return new Response(null, { status: 400 })

    let body = ''
    let size = 0
    const decoder = new TextDecoder()
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      size += value.length
      if (size > 1024) {
        await reader.cancel()
        return new Response(null, { status: 413 })
      }
      body += decoder.decode(value, { stream: true })
    }
    body += decoder.decode()

    const email = new URLSearchParams(body).get('email')?.trim().toLowerCase()
    if (!email || email.length > 254 || !emailPattern.test(email)) {
      return new Response(null, { status: 400 })
    }

    await env.EARLY_ACCESS.prepare(
      'INSERT INTO early_access (email) VALUES (?) ON CONFLICT (email) DO NOTHING',
    ).bind(email).run()
    return new Response(null, { status: 204 })
  },
}
