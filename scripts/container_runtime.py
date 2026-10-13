#!/usr/bin/env python3
"""Owned direct-PID1 container runner; preserve all logs and scratch on failure."""
import json
import pathlib
import queue
import subprocess
import threading
import time


class ContainerWorker:
    def __init__(self,image,name,config,secrets,scratch,output,action,password,existing=False):
        self.name=name; self.output=output; self.image=image
        output.mkdir()
        self.events=queue.Queue(); self.latest_cache=None
        self.metrics=(output/'metrics.jsonl').open('w')
        self.stderr=(output/'stderr.log').open('w')
        if not existing:
            subprocess.run(['docker','create','--name',name,'--network=host','--read-only',
                '--cap-drop=ALL','--security-opt=no-new-privileges','--memory=512m','--cpus=2',
                '--mount',f'type=bind,src={config.resolve()},dst=/etc/pgderive/query.toml,readonly',
                '--mount',f'type=bind,src={secrets.resolve()},dst=/run/secrets,readonly',
                '--mount',f'type=volume,src={scratch},dst=/var/lib/pgderive/scratch',
                '-e','PGDERIVE__POSTGRES__PASSWORD='+password,
                image,'query',action,'/etc/pgderive/query.toml'],check=True,capture_output=True)
        self.process=subprocess.Popen(['docker','start','--attach',name],stdout=subprocess.PIPE,stderr=self.stderr,text=True)
        self.reader=threading.Thread(target=self.read,daemon=True); self.reader.start()

    def read(self):
        for line in self.process.stdout:
            self.metrics.write(line); self.metrics.flush()
            value=json.loads(line)
            if 'cache' in value: self.latest_cache=value['cache']
            self.events.put(value)
        self.events.put(None)

    def event(self,kind,seconds=240,minimum_time=None):
        deadline=time.monotonic()+seconds
        while time.monotonic()<deadline:
            value=self.events.get(timeout=max(.01,deadline-time.monotonic()))
            if value is None: raise RuntimeError(f'container ended before {kind}; {self.output}')
            if minimum_time is not None:
                if value['event'] in ('published','ready') and value['time']>=minimum_time: return value
            elif value['event']==kind: return value
        raise TimeoutError(kind)

    def inspect(self):
        value=json.loads(subprocess.check_output(['docker','inspect',self.name]))[0]
        evidence=json.loads(json.dumps(value))
        evidence['Config']['Env']=[item.split('=',1)[0]+'=<redacted>' if 'PASSWORD=' in item else item for item in evidence['Config']['Env']]
        (self.output/'container.json').write_text(json.dumps(evidence,indent=2)+'\n')
        assert value['Image']==self.image, 'container image identity drift'
        assert value['Config']['User']=='10001:10001'
        assert value['Config']['StopSignal']=='SIGINT'
        assert value['HostConfig']['ReadonlyRootfs']
        assert value['HostConfig']['Memory']==512*1024*1024
        assert value['Config']['Entrypoint']==['/usr/local/bin/pgderive']
        mounts={item['Destination']:item for item in value['Mounts']}
        assert not mounts['/etc/pgderive/query.toml']['RW']
        assert not mounts['/run/secrets']['RW']
        assert mounts['/var/lib/pgderive/scratch']['RW']
        assert mounts['/var/lib/pgderive/scratch']['Type']=='volume'
        return value

    def finish(self,graceful=True):
        subprocess.run(['docker','stop','--timeout','120',self.name] if graceful else ['docker','kill','--signal','KILL',self.name],check=True,capture_output=True)
        self.process.wait(timeout=130); self.reader.join(timeout=10)
        assert not self.reader.is_alive()
        state=self.inspect()['State']
        if graceful: assert state['ExitCode']==0,state
        self.metrics.close(); self.stderr.close()

    def abort(self):
        subprocess.run(['docker','kill','--signal','KILL',self.name],capture_output=True)
        if self.process.poll() is None: self.process.wait(timeout=20)
        self.reader.join(timeout=10)
        if not self.metrics.closed: self.metrics.close(); self.stderr.close()


def cli(image,config,secrets,password,action):
    result=subprocess.run(['docker','run','--rm','--network=host','--read-only','--cap-drop=ALL',
        '--security-opt=no-new-privileges',
        '--mount',f'type=bind,src={config.resolve()},dst=/etc/pgderive/query.toml,readonly',
        '--mount',f'type=bind,src={secrets.resolve()},dst=/run/secrets,readonly',
        '-e','PGDERIVE__POSTGRES__PASSWORD='+password,image,'query',action,'/etc/pgderive/query.toml'],text=True,capture_output=True,timeout=240)
    assert result.returncode==0,result.stderr
    return json.loads(result.stdout.splitlines()[-1])
