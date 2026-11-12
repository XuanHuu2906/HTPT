import subprocess,json,pathlib,time,hashlib
base=pathlib.Path(__file__).resolve().parent
compose=['docker','compose','-f','docker/compose.dev.yml']
tables=['raft_meta','raft_log','raft_apply_progress','trips','seats','tickets','request_results','business_events']
def sql(n,query):
    cmd=compose+['exec','-T',f'postgres-{n}','sh','-c','exec psql -X -A -t -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"']
    r=subprocess.run(cmd,input=query,text=True,capture_output=True)
    print(json.dumps({'node':n,'command':cmd,'sql':query,'exit_code':r.returncode}),flush=True)
    if r.returncode: raise RuntimeError(r.stderr)
    return r.stdout.strip()
def snapshot(phase):
    result={}
    for n in range(1,6):
        query='\n'.join(f"SELECT json_build_object('table','{t}','rows',coalesce(json_agg(row_to_json(r) ORDER BY row_to_json(r)::text),'[]'::json)) FROM public.{t} r;" for t in tables)
        data=[json.loads(x) for x in sql(n,query).splitlines()]
        seats=next(t['rows'] for t in data if t['table']=='seats')
        assert sorted(s['seat_id'] for s in seats)==[f'G{i:02}' for i in range(1,11)]
        assert all(s['trip_id']=='SE-001' and s['status']=='AVAILABLE' and s['current_ticket_id'] is None for s in seats)
        result[str(n)]=data
        (base/f'postgres-{n}-{phase}.json').write_text(json.dumps(data,indent=2)+'\n')
        print(f'Node {n} {phase}: 8 tables queried, 10 AVAILABLE seats; sha256='+hashlib.sha256(json.dumps(data,sort_keys=True).encode()).hexdigest(),flush=True)
    return result
before=snapshot('before-restart')
r=subprocess.run(compose+['restart']+[f'postgres-{n}' for n in range(1,6)],capture_output=True,text=True)
print(r.stdout,r.stderr,'restart exit_code=',r.returncode,flush=True); assert r.returncode==0
for attempt in range(30):
    r=subprocess.run(compose+['ps','--format','json'],capture_output=True,text=True,check=True)
    rows=[json.loads(x) for x in r.stdout.splitlines() if x.strip()]
    pg=[x for x in rows if x['Service'].startswith('postgres-')]
    if len(pg)==5 and all(x.get('Health')=='healthy' for x in pg): break
    time.sleep(1)
else: raise RuntimeError('PostgreSQL did not become healthy')
after=snapshot('after-restart'); assert before==after
print('PASS: all 8 public tables on all 5 DBs unchanged after restart. Seed-data durability only; no confirmed booking durability claim.')
