import subprocess,pathlib,sys,os,datetime,json,hashlib,shutil,xml.etree.ElementTree as ET
root=pathlib.Path.cwd(); evidence=pathlib.Path(__file__).resolve().parent
name=sys.argv[1]; cmd=sys.argv[2:]; env=os.environ.copy()
if name.startswith('jdbc'):
    r=subprocess.run(['docker','compose','-f','docker/compose.dev.yml','exec','-T','postgres-1','sh','-c','printf "%s\\n%s" "$POSTGRES_USER" "$POSTGRES_PASSWORD"'],capture_output=True,text=True,check=True)
    user,password=r.stdout.split('\n',1)
    env.update(RAILWAY_TEST_JDBC_URL='jdbc:postgresql://127.0.0.1:5433/railway_node1',RAILWAY_TEST_DB_USER=user,RAILWAY_TEST_DB_PASSWORD=password,NODE1_DB_USER=user,NODE1_DB_PASSWORD=password)
patch=subprocess.check_output(['git','diff','--','backend-node','shared','integration-tests','pom.xml','docker'],text=True)
(evidence/(name+'.patch')).write_text(patch)
start=datetime.datetime.now(datetime.timezone.utc).isoformat()
with (evidence/(name+'.log')).open('w') as f:
    r=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,env=env)
meta={'command':cmd,'exit_code':r.returncode,'started_utc':start,'finished_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()}
(evidence/(name+'.json')).write_text(json.dumps(meta,indent=2)+'\n')
if name.startswith('jdbc') and cmd[0]=='mvn':
    reports=evidence/(name+'-reports'); reports.mkdir(exist_ok=True)
    suites=[]
    selected=next((a.split('=',1)[1].split(',') for a in cmd if a.startswith('-Dtest=')),None)
    for module in ['backend-node','shared','integration-tests']:
        for report in (root/module/'target/surefire-reports').glob('TEST-*.xml'):
            suite=ET.parse(report).getroot()
            if selected and suite.attrib['name'].split('.')[-1] not in selected: continue
            suites.append({k:suite.attrib[k] for k in ['name','tests','failures','errors','skipped']})
            shutil.copy2(report,reports/report.name)
    meta['suites']=suites
    if name=='jdbc-final':
        valid = sum(int(s['tests']) for s in suites)==39 and all(
                int(s['skipped'])==0 and int(s['failures'])==0 and int(s['errors'])==0 for s in suites)
        meta['required_39_no_skip_gate'] = 'PASS' if valid else 'FAIL'
        if not valid:
            meta['maven_exit_code']=r.returncode
            meta['exit_code']=2
meta['patch_sha256']=hashlib.sha256(patch.encode()).hexdigest()
(evidence/(name+'.json')).write_text(json.dumps(meta,indent=2)+'\n')
print(json.dumps(meta)); print((evidence/(name+'.log')).read_text()[-5000:])
sys.exit(meta['exit_code'])
