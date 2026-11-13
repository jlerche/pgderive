"""Exercise the actual gate shell's overlap, join and failure propagation."""
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest


STUB = '''import os, pathlib, sys, time
root=pathlib.Path(os.environ['PHASE_EVIDENCE'])
role=sys.argv[1]
if role=='llvm-cov':
    if sys.argv[2]=='report':
        assert (root/'unit.done').exists() and (root/'live.done').exists()
        (root/'reported').touch()
    sys.exit(0)
role='unit' if role=='test' else role
(root/(role+'.started')).touch()
if role in ('unit','live','integration'):
    deadline=time.monotonic()+3
    peers=('live','integration') if role=='unit' else ('unit',)
    while not any((root/(p+'.started')).exists() for p in peers):
        if time.monotonic()>deadline:sys.exit(99)
        time.sleep(.005)
    # Make oracle/unit failures finish before the live phase. The shell must
    # still await live cleanup rather than returning with a background runner.
    if role in ('live','integration'):time.sleep(.1)
(root/(role+'.done')).touch()
sys.exit(int(os.environ.get('FAIL_'+role.upper(),'0')))
'''


class CoveragePhasesTests(unittest.TestCase):
    def run_gate(self, root, profile, failure):
        scripts=root/'scripts';scripts.mkdir()
        source=pathlib.Path(__file__).with_name('check_coverage.sh')
        shutil.copy(source,scripts/source.name)
        stub=root/'stub.py';stub.write_text(STUB)
        cargo=root/'cargo'
        cargo.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{stub}" "$@"\n')
        cargo.chmod(0o755)
        for filename,role in (('check_worker.sh','live'),('check_integration.sh','integration')):
            path=scripts/filename
            path.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{stub}" {role}\n')
            path.chmod(0o755)
        build=scripts/'build_gate.sh'
        build.write_text('#!/bin/sh\nmkdir -p "$1"\n: > "$1/coverage.env"\n')
        build.chmod(0o755)
        (scripts/'check_semantics_report.py').write_text(STUB.replace("role=sys.argv[1]", "role='verify'"))
        environment={**os.environ,'PATH':str(root)+os.pathsep+os.environ['PATH'],
                     'PHASE_EVIDENCE':str(root),'PGDERIVE_CHECK_PROFILE':profile}
        if failure:environment['FAIL_'+failure.upper()]='7'
        return subprocess.run(['bash',str(scripts/source.name)],env=environment,
                              capture_output=True,text=True,timeout=10)

    def test_phases_overlap_and_both_must_pass_before_reporting(self):
        for profile,failure in (('core',None),('full',None),('core','unit'),
                                ('core','verify'),('core','live'),('full','integration')):
            with self.subTest(profile=profile,failure=failure), tempfile.TemporaryDirectory() as directory:
                root=pathlib.Path(directory)
                result=self.run_gate(root,profile,failure)
                self.assertEqual(result.returncode,7 if failure else 0,result.stdout+result.stderr)
                self.assertTrue((root/'unit.done').exists())
                # A failed full integration phase must not start worker tests.
                last='integration' if failure=='integration' else 'live'
                self.assertTrue((root/(last+'.done')).exists())
                self.assertEqual((root/'live.started').exists(),failure!='integration')
                self.assertEqual((root/'reported').exists(),failure is None)
