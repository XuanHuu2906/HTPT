import subprocess,pathlib,os,uuid,time
root=pathlib.Path.cwd()
jars=[*pathlib.Path.home().glob('.m2/repository/org/postgresql/postgresql/42.7.7/*.jar')]
for artifact in ['jackson-databind','jackson-core','jackson-annotations']:
 jars.extend(pathlib.Path.home().glob(f'.m2/repository/com/fasterxml/jackson/core/{artifact}/2.19.2/*.jar'))
cp=os.pathsep.join(map(str,[root/'backend-node/target/classes',root/'shared/target/classes',*jars]))
probe=['java','-cp',cp,'artifacts/evidence/2026-11-11-persistence-fix/JdbcDurabilityProbe.java']
schema='railway_test_'+uuid.uuid4().hex
compose=['docker','compose','-f','docker/compose.dev.yml']
def run(command):
 print('COMMAND:',command,flush=True)
 result=subprocess.run(command)
 print('EXIT_CODE:',result.returncode,flush=True)
 result.check_returncode()
run(probe+['prepare',schema])
run(compose+['restart','postgres-1'])
for _ in range(30):
 r=subprocess.run(compose+['exec','-T','postgres-1','sh','-c','pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"'],capture_output=True,text=True)
 if r.returncode==0:break
 time.sleep(1)
else:raise RuntimeError('PostgreSQL not ready after restart')
run(probe+['verify',schema])
print('PASS JDBC data survives graceful PostgreSQL restart plus a fresh JVM. Not a Raft/crash durability claim.')
