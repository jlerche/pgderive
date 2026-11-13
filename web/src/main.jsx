import { createRoot } from 'react-dom/client'
import './style.css'

const repository = 'https://github.com/jlerche/pgderive'

function Wordmark() {
  return <span className="wordmark"><span className="mark" aria-hidden="true"><i /><i /><i /></span>pgderive</span>
}

function Example() {
  return (
    <figure className="example">
      <figcaption>Example · Revenue by customer</figcaption>
      <pre><code><b>SELECT</b> customer_id,{'\n'}{'       '}<b>SUM</b>(amount_cents) <b>AS</b> revenue_cents{'\n'}<b>FROM</b> orders{'\n'}<b>GROUP BY</b> customer_id;</code></pre>
      <div className="result">
        <div className="result-label">Result table</div>
        <table>
          <thead><tr><th scope="col">customer_id</th><th scope="col">revenue_cents</th></tr></thead>
          <tbody>
            <tr><td>1042</td><td>18400</td></tr>
            <tr><td>1089</td><td>9600</td></tr>
          </tbody>
        </table>
      </div>
    </figure>
  )
}

function App() {
  return (
    <div className="site">
      <header className="site-header">
        <a href="/" aria-label="Pgderive home"><Wordmark /></a>
        <nav aria-label="Main navigation">
          <a href="#how-it-works">How it works</a>
          <a href={repository}>GitHub <span aria-hidden="true">↗</span></a>
        </nav>
      </header>
      <main>
        <section className="hero" aria-labelledby="headline">
          <div>
            <p className="kicker">Incremental views for PostgreSQL</p>
            <h1 id="headline">Keep your query results current.</h1>
            <p className="intro">Pgderive turns committed changes into incremental updates to a PostgreSQL result table. Read the results with ordinary SQL.</p>
            <a className="button" href="#how-it-works">See how it works <span aria-hidden="true">↓</span></a>
          </div>
          <Example />
        </section>
        <section className="workflow" id="how-it-works" aria-labelledby="workflow-title">
          <h2 id="workflow-title">From commit to result table</h2>
          <div className="steps">
            <article><h3>Read committed changes</h3><p>Capture source changes from PostgreSQL logical replication in transaction order.</p></article>
            <article><h3>Compute what changed</h3><p>Apply the transaction to retained query state and compute the affected result rows.</p></article>
            <article><h3>Publish results</h3><p>Write result changes and durable progress together in PostgreSQL.</p></article>
          </div>
        </section>
      </main>
      <footer><Wordmark /><span>Hosted service in development</span></footer>
    </div>
  )
}

createRoot(document.getElementById('root')).render(<App />)
