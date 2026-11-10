from pathlib import Path
import subprocess
jars=[]
for p in Path('/home/nguyenhuu/.m2/repository').rglob('*.jar'):
 s=str(p)
 if '/org/junit/' in s and '/6.0.3/' not in s: continue
 if any(x in s for x in ['/org/junit/','/org/opentest4j/','/org/apiguardian/','/com/fasterxml/','/org/slf4j/','/ch/qos/logback/','/org/postgresql/']): jars.append(s)
cp=':'.join(['/tmp','backend-node/target/classes','backend-node/target/test-classes','shared/target/classes']+jars)
with open('/tmp/raft-d10-unit.log','w') as log:
 result=subprocess.run(['java','-cp',cp,'RaftD10Runner']+['vn.nhom6.railway.raft.election.'+x for x in ['RaftElectionComponentTest','ElectionTest','VotePersistenceTest','TimerTest']],stdout=log,stderr=log)
 print('JUnit exit code:',result.returncode)
