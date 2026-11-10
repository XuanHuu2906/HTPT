from pathlib import Path
import subprocess
root = Path(__file__).resolve().parents[4]
evidence = Path(__file__).resolve().parent
build = Path('/tmp/raft-d11-classes')
build.mkdir(exist_ok=True)
jars = []
for p in (Path.home()/'.m2/repository').rglob('*.jar'):
    s = str(p)
    if '/org/junit/' in s and '/6.0.3/' not in s: continue
    if any(x in s for x in ['/org/junit/', '/org/opentest4j/', '/org/apiguardian/', '/com/fasterxml/', '/org/slf4j/', '/ch/qos/logback/', '/org/postgresql/']): jars.append(s)
cp = ':'.join([str(build)] + jars)
sources = [str(p) for module in ['shared', 'backend-node'] for p in (root/module/'src/main/java').rglob('*.java')]
sources += [str(p) for p in (root/'backend-node/src/test/java/vn/nhom6/railway/raft/election').glob('*.java')]
sources.append(str(root/'backend-node/src/test/java/vn/nhom6/railway/raft/replication/FakeRaftStorage.java'))
sources.append(str(evidence/'RaftD10Runner.java'))
with (evidence/'direct-junit.log').open('w') as log:
    command = ['javac', '--release', '25', '-cp', cp, '-d', str(build)] + sources
    log.write('Compile shared/backend sources and election tests with cached JARs; JUnit 6.0.3\n'); log.flush()
    result = subprocess.run(command, stdout=log, stderr=log)
    if result.returncode: raise SystemExit(result.returncode)
    tests = ['vn.nhom6.railway.raft.election.'+p.stem for p in (root/'backend-node/src/test/java/vn/nhom6/railway/raft/election').glob('*Test.java')]
    result = subprocess.run(['java', '-cp', cp, 'RaftD10Runner']+tests, stdout=log, stderr=log)
    log.write('Exit code: '+str(result.returncode)+'\n')
    raise SystemExit(result.returncode)
