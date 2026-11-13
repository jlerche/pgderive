import { useState } from 'react'
import { createRoot } from 'react-dom/client'
import './style.css'

const demo = 'https://demo.pgderive.com/'

function Wordmark() {
  return (
    <span className="wordmark">
      <span className="mark" aria-hidden="true"><i /><i /><i /></span>
      pgderive
    </span>
  )
}

function Example() {
  return (
    <figure className="example" id="example">
      <figcaption><span>revenue_by_customer.sql</span><span>PostgreSQL</span></figcaption>
      <pre><code><b>SELECT</b> customer_id,{'\n'}{'  '}<b>SUM</b>(amount_cents){'\n'}{'    '}<b>AS</b> revenue_cents{'\n'}<b>FROM</b> orders{'\n'}<b>GROUP BY</b> customer_id;</code></pre>
      <p className="source-change"><strong>New order</strong><span>Customer 1042 · $25.00</span></p>
      <div className="result">
        <div className="result-label">Result table · Revenue in cents</div>
        <table aria-label="Revenue by customer before and after the order">
          <thead>
            <tr><th scope="col">customer_id</th><th scope="col">Before</th><th scope="col">After</th></tr>
          </thead>
          <tbody>
            <tr className="changed">
              <th scope="row">1042</th><td>18400</td><td>20900 <span className="delta">+2500</span></td>
            </tr>
            <tr><th scope="row">1089</th><td>9600</td><td>9600</td></tr>
          </tbody>
        </table>
      </div>
    </figure>
  )
}

function EarlyAccess() {
  const [status, setStatus] = useState('')

  async function subscribe(event) {
    event.preventDefault()
    const form = event.currentTarget
    const body = new URLSearchParams(new FormData(form))
    setStatus('Submitting…')
    try {
      const response = await fetch('/api/early-access', { method: 'POST', body })
      if (response.status === 204) {
        form.reset()
        setStatus('You’re on the list.')
      } else if (response.status === 429) {
        setStatus('Too many attempts. Try again in a minute.')
      } else if (response.status === 400) {
        setStatus('Enter a valid email address.')
      } else {
        setStatus('Signup failed. Please try again.')
      }
    } catch {
      setStatus('Signup failed. Please try again.')
    }
  }

  return (
    <section id="early-access" className="site early-access" aria-labelledby="early-access-title">
      <div>
        <h2 id="early-access-title">Get early access</h2>
        <p>Join the list for launch updates and early access invitations.</p>
      </div>
      <form onSubmit={subscribe}>
        <label htmlFor="signup-email">Email address</label>
        <div className="signup-fields">
          <input id="signup-email" name="email" type="email" autoComplete="email" maxLength={254} required />
          <button className="button" type="submit" disabled={status === 'Submitting…'}>Join the waitlist</button>
        </div>
        <p role="status">{status}</p>
      </form>
    </section>
  )
}

function App() {
  return (
    <>
      <header className="site-header">
        <div className="site header-content">
          <a href="/" aria-label="Pgderive home"><Wordmark /></a>
          <nav aria-label="Main navigation">
            <a href="#how-it-works">How it works</a>
            <a className="outline" href={demo}>Live demo <span aria-hidden="true">↗</span></a>
          </nav>
        </div>
      </header>
      <main>
        <section className="hero" aria-labelledby="headline">
          <div className="site hero-content">
            <div>
              <p className="kicker">Incremental views for PostgreSQL</p>
              <h1 id="headline">Keep your query <span>results current.</span></h1>
              <p className="intro">Pgderive processes committed changes and updates the affected rows in a PostgreSQL result table. Read it with ordinary SQL.</p>
              <div className="actions">
                <a className="button" href="#early-access">Get early access <span aria-hidden="true">↓</span></a>
                <a className="text-link" href="#how-it-works">How it works <span aria-hidden="true">↘</span></a>
              </div>
            </div>
            <Example />
          </div>
        </section>
        <section className="workflow site" id="how-it-works" aria-labelledby="workflow-title">
          <h2 id="workflow-title">From commit to result table</h2>
          <div className="steps">
            <article><span className="step-label">PostgreSQL WAL</span><h3>Read committed changes</h3><p>Capture source changes from PostgreSQL logical replication in transaction order.</p></article>
            <article><span className="step-label">Pgderive engine</span><h3>Compute what changed</h3><p>Apply each transaction to retained query state and compute the affected result rows.</p></article>
            <article><span className="step-label">PostgreSQL sink</span><h3>Publish results</h3><p>Write result changes and durable progress together in PostgreSQL.</p></article>
          </div>
        </section>
        <EarlyAccess />
      </main>
      <footer className="site"><Wordmark /></footer>
    </>
  )
}

createRoot(document.getElementById('root')).render(<App />)
