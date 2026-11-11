import socket,struct,json,uuid,datetime

def receive(s,n):
    data=b''
    while len(data)<n:
        part=s.recv(n-len(data))
        if not part: raise EOFError('connection closed mid-frame')
        data+=part
    return data

def exchange(port,payload):
    with socket.create_connection(('127.0.0.1',port),timeout=5) as s:
        s.sendall(struct.pack('>I',len(payload))+payload)
        size=struct.unpack('>I',receive(s,4))[0]
        assert 0<size<=1048576
        return receive(s,size)
for n in range(1,6):
    assert exchange(9100+n,b'PING')==b'PONG'
    with socket.create_connection(('127.0.0.1',9200+n),timeout=5): pass
    request=dict(protocolVersion=1,messageId=str(uuid.uuid4()),correlationId=None,type='NODE_STATUS',senderId='monitor-evidence',sentAt=datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z'),payload={})
    response=json.loads(exchange(9100+n,json.dumps(request).encode()))
    assert response['type']=='NODE_STATUS_RESPONSE' and response['correlationId']==request['messageId']
    assert response['payload']['nodeId']==f'node-{n}'
    print(json.dumps({'node':n,'client_port':9100+n,'peer_port':9200+n,'ping':'PONG','peer_tcp_connect':'PASS','request':request,'response':response}),flush=True)
print('PASS TCP transport only. NODE_STATUS alone does not establish Raft membership, election or quorum.')
