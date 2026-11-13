import { useState } from 'react'
import { createRoot } from 'react-dom/client'
import './style.css'

const repository = 'https://github.com/jlerche/pgderive'

function Wordmark() {
  return (
    <span className="wordmark">
      <span className="mark" aria-hidden="true"><i /><i /><i /></span>
      pgderive
    </span>
  )
}

function Example() {
  const [inserted, setInserted] = useState(false)

  return (
    <figure className="example" id="example">
      <figcaption><span>revenue_by_customer.sql</span><span>PostgreSQL</span></figcaption>
      <pre><code><b>SELECT</b> customer_id,{'\n'}{'  '}<b>SUM</b>(amount_cents){'\n'}{'    '}<b>AS</b> revenue_cents{'\n'}<b>FROM</b> orders{'\n'}<b>GROUP BY</b> customer_id;</code></pre>
      <div className="result">
        <div className="result-label"><span aria-hidden="true" /> Result table</div>
        <table aria-label="Revenue by customer in cents">
          <thead><tr><th scope="col">customer_id</th><th scope="col">revenue_cents</th></tr></thead>
          <tbody>
            <tr className={inserted ? 'changed' : undefined}>
              <td>1042</td>
              <td>{inserted ? '20900' : '18400'} {inserted && <span className="delta">+2500</span>}</td>
            </tr>
            <tr><td>1089</td><td>9600</td></tr>
          </tbody>
        </table>
      </div>
      <div className="simulation">
        <p role="status">{inserted ? 'Customer 1042’s revenue is now $209.00.' : 'Simulate a $25 order for customer 1042.'}</p>
        <button type="button" onClick={() => setInserted(!inserted)}>
          {inserted ? 'Reset example' : 'Insert order'}
        </button>
      </div>
    </figure>
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
            <a className="outline" href={repository}>GitHub <span aria-hidden="true">↗</span></a>
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
                <a className="button" href="#example">Try the example <span aria-hidden="true">↓</span></a>
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
      </main>
      <footer className="site"><Wordmark /><span>Hosted service in development</span></footer>
    </>
  )
}

createRoot(document.getElementById('root')).render(<App />)
