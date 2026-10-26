"""Same-time protected background publication and failed-publication recovery."""
import json
from worker_harness import Fixture
from worker_crash_harness import wait_for


def case(output, command, failing):
    output.mkdir()
    fixture = Fixture(output, 'consolidation', rows=32, auctions=4)
    original = fixture.config
    def config(maximum=0):
        path = original(maximum)
        path.write_text(path.read_text().replace('maintenance_ticks=4',
            'maintenance_ticks=1\ncheckpoint_interval_ms=60000').replace('retry_attempts=8', 'retry_attempts=1'))
        return path
    fixture.config = config
    with fixture.managed():
        worker = fixture.start(command, 'first')
        worker.event('ready')
        fixture.sql(f'''
          CREATE TABLE {fixture.name}.consolidation_fault(enabled boolean);
          INSERT INTO {fixture.name}.consolidation_fault VALUES ({str(failing).lower()});
          CREATE TABLE {fixture.name}.consolidation_audit(before_progress jsonb,after_progress jsonb,
            before_sink jsonb,after_sink jsonb,readers bigint,uploads bigint);
          CREATE FUNCTION {fixture.name}.observe_consolidation() RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN
            IF NEW.logical_time=OLD.logical_time AND NEW.epoch>OLD.epoch THEN
              IF TG_WHEN='BEFORE' THEN
                IF (SELECT enabled FROM {fixture.name}.consolidation_fault) THEN
                  RAISE EXCEPTION 'injected consolidation publication failure';
                END IF;
                INSERT INTO {fixture.name}.consolidation_audit
                SELECT to_jsonb(p),NULL,
                  (SELECT jsonb_agg(jsonb_build_array(g.ctid::text,g.xmin::text,to_jsonb(g)) ORDER BY g.ctid)
                   FROM {fixture.name}.groups g),NULL,
                  (SELECT count(*) FROM {fixture.name}.pgderive_protections WHERE active AND recovering
                    AND token LIKE '%:consolidation:%'),
                  (SELECT count(*) FROM {fixture.name}.pgderive_protections WHERE active AND uploading
                    AND token LIKE '%:consolidation:%') FROM {fixture.name}.pgderive_progress p;
              ELSE
                UPDATE {fixture.name}.consolidation_audit SET after_progress=(SELECT to_jsonb(p)
                  FROM {fixture.name}.pgderive_progress p),after_sink=(SELECT
                  jsonb_agg(jsonb_build_array(g.ctid::text,g.xmin::text,to_jsonb(g)) ORDER BY g.ctid)
                  FROM {fixture.name}.groups g) WHERE after_progress IS NULL;
              END IF;
            END IF;
            RETURN NEW;
          END $$;
          CREATE TRIGGER consolidation_before BEFORE UPDATE ON {fixture.name}.pgderive_queries
            FOR EACH ROW EXECUTE FUNCTION {fixture.name}.observe_consolidation();
          CREATE TRIGGER consolidation_after AFTER UPDATE ON {fixture.name}.pgderive_queries
            FOR EACH ROW EXECUTE FUNCTION {fixture.name}.observe_consolidation();''')
        failed = False
        for tick in range(16):
            fixture.sql(f'UPDATE {fixture.name}.bid SET price=id+{100+tick}')
            try:
                worker.event('published', minimum_time=tick+2)
            except RuntimeError:
                assert failing, 'unexpected worker failure'
                failed = True
                break
        if failing:
            worker.process.wait(timeout=60)
            worker.abort()
            assert worker.process.returncode != 0
            assert "injected consolidation publication failure" in (output/'first/stderr.log').read_text()
            protections = json.loads(fixture.sql(f'''SELECT json_build_object(
              'readers',count(*) FILTER(WHERE recovering),
              'uploads',count(*) FILTER(WHERE uploading AND NOT quiescent))
              FROM {fixture.name}.pgderive_protections WHERE active AND token LIKE '%:consolidation:%' '''))
            assert protections['readers'] > 0 and protections['uploads'] > 0, protections
            fixture.sql(f'UPDATE {fixture.name}.consolidation_fault SET enabled=false')
            resumed = fixture.start(command, 'recovered')
            resumed.event('ready')
            marker = fixture.sql(f'BEGIN;UPDATE {fixture.name}.bid SET price=id+999;SELECT pg_current_wal_insert_lsn();COMMIT')
            marker = next(line for line in marker.splitlines() if '/' in line)
            # Recovery may still have an unreceived committed transaction. Wait
            # for this explicit source WAL position rather than one publication.
            wait_for(lambda: fixture.sql(f"SELECT end_lsn >= '{marker}'::pg_lsn FROM {fixture.name}.pgderive_progress").strip() == 't',
                     'recovered source marker published')
            resumed.finish(graceful=True)
        else:
            events = [json.loads(line) for line in (output/'first/metrics.jsonl').read_text().splitlines()]
            assert any(event['event']=='consolidation' for event in events), 'urgent checkpoints starved ready consolidation'
            worker.finish(graceful=True)
            protections = None
        fixture.verify()
        audits = json.loads(fixture.sql(f'SELECT coalesce(json_agg(a),\'[]\') FROM {fixture.name}.consolidation_audit a'))
        assert audits, 'no background physical publication was witnessed'
        for audit in audits:
            assert audit['before_progress'] == audit['after_progress'], audit
            assert audit['before_sink'] == audit['after_sink'], audit
            assert audit['readers'] == 1 and audit['uploads'] == 1, audit
        assert fixture.sql(f'''SELECT count(*) FROM {fixture.name}.pgderive_protections
          WHERE active AND token LIKE '%:consolidation:%' ''').strip() == '0'
        (output/'result.json').write_text(json.dumps({'passed': True,
            'injected_failure':failing,'failed_during_input':failed,
            'physical_publications':len(audits),'failed_upload_protections':protections,
            'source_and_sink_unchanged_by_physical_publication':True},indent=2)+'\n')
