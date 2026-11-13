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
                <a className="button" href="#example">See the example <span aria-hidden="true">↓</span></a>
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
