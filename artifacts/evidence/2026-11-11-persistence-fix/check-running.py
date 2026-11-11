import socket,struct,json,uuid,datetime,subprocess

def read(s,n):
 data=b''
 while len(data)<n:
  part=s.recv(n-len(data))
  if not part:raise EOFError('incomplete frame')
  data+=part
 return data
for n in range(1,6):
 for kind,payload in [('BOOK_TICKET',dict(requestId=str(uuid.uuid4()),tripId='SE-001',seatId='G02',passengerName='Fail closed demo')),('GET_SEATS',dict(tripId='SE-001'))]:
  request=dict(protocolVersion=1,messageId=str(uuid.uuid4()),correlationId=None,type=kind,senderId='client-evidence',sentAt=datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z'),payload=payload)
  wire=json.dumps(request).encode()
  with socket.create_connection(('127.0.0.1',9100+n),timeout=5) as s:
   s.sendall(struct.pack('>I',len(wire))+wire);size=struct.unpack('>I',read(s,4))[0];assert size<=1048576
   response=json.loads(read(s,size))
  assert response['correlationId']==request['messageId']
  assert response['type']=='ERROR_RESPONSE'
  assert response['payload']['errorCode']==('NO_QUORUM' if kind=='BOOK_TICKET' else 'READ_UNAVAILABLE')
  print(json.dumps(dict(node=n,request=request,response=response)),flush=True)
 cmd=['docker','compose','-f','docker/compose.dev.yml','exec','-T',f'postgres-{n}','sh','-c','exec psql -X -A -t -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"']
 sql="SELECT (SELECT count(*) FROM public.tickets),(SELECT count(*) FROM public.request_results),(SELECT count(*) FROM public.raft_log),(SELECT last_applied FROM public.raft_apply_progress);"
 r=subprocess.run(cmd,input=sql,capture_output=True,text=True)
 print(json.dumps(dict(command=cmd,sql=sql,node=n,exit_code=r.returncode,result=r.stdout)),flush=True)
 assert r.returncode==0 and r.stdout.strip()=='0|0|0|0'
print('PASS: all 5 default runtimes fail closed and do not mutate public booking/Raft state. This is not distributed booking acceptance.')
