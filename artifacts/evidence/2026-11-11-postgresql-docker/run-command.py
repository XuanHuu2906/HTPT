import subprocess,pathlib,sys,os,datetime,json
root=pathlib.Path.cwd(); evidence=root/'artifacts/evidence/2026-11-11-postgresql-docker'
name=sys.argv[1]; cmd=sys.argv[2:]; env=os.environ.copy()
if name=='jdbc':
    r=subprocess.run(['docker','compose','-f','docker/compose.dev.yml','exec','-T','postgres-1','sh','-c','printf "%s\\n%s" "$POSTGRES_USER" "$POSTGRES_PASSWORD"'],capture_output=True,text=True,check=True)
    user,password=r.stdout.split('\n',1)
    env.update(RAILWAY_TEST_JDBC_URL='jdbc:postgresql://127.0.0.1:5433/railway_node1',RAILWAY_TEST_DB_USER=user,RAILWAY_TEST_DB_PASSWORD=password,NODE1_DB_USER=user,NODE1_DB_PASSWORD=password)
start=datetime.datetime.now(datetime.timezone.utc).isoformat()
with (evidence/(name+'.log')).open('w') as f:
    r=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,env=env)
meta={'command':cmd,'exit_code':r.returncode,'started_utc':start,'finished_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()}
(evidence/(name+'.json')).write_text(json.dumps(meta,indent=2)+'\n')
print(json.dumps(meta)); print((evidence/(name+'.log')).read_text()[-5000:])
sys.exit(r.returncode)
