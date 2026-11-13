#!/usr/bin/env python3
"""Disposable five-Backend/five-PostgreSQL election test over the v1 TCP protocol."""
import concurrent.futures
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import socket
import struct
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
OUT = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else ROOT / 'artifacts/evidence/D13/election-runtime/docker'
OUT.mkdir(parents=True, exist_ok=True)
PROJECT = 'railway-election-' + uuid.uuid4().hex[:8]
ENV = dict(os.environ, ELECTION_TEST_DB_PASSWORD=secrets.token_hex(16))
COMPOSE = ['docker', 'compose', '-p', PROJECT, '-f', str(ROOT / 'docker/compose.election-test.yml')]
SAMPLES = []


def compose(*args):
    print('compose', *args, flush=True)
    return subprocess.run(COMPOSE + list(args), cwd=ROOT, env=ENV, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True).stdout


def receive(sock, size):
    data = b''
    while len(data) < size:
        block = sock.recv(size - len(data))
        if not block:
            raise ConnectionError('incomplete TCP frame')
        data += block
    return data


def rpc(node, kind='NODE_STATUS', payload=None, sender='election-test', peer=False):
    mid = str(uuid.uuid4())
    request = dict(protocolVersion=1, messageId=mid, correlationId=None, type=kind,
                   senderId=sender, sentAt=datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
                   payload={} if payload is None else payload)
    data = json.dumps(request, separators=(',', ':')).encode()
    with socket.create_connection(('127.0.0.1', (19200 if peer else 19100) + node), timeout=2) as sock:
        sock.sendall(struct.pack('>I', len(data)) + data)
        size = struct.unpack('>I', receive(sock, 4))[0]
        assert 0 < size <= 1_048_576
        response = json.loads(receive(sock, size))
    assert response['correlationId'] == mid and response['senderId'] == f'node-{node}'
    return response


def statuses(nodes):
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        result = dict(zip(nodes, pool.map(lambda n: rpc(n)['payload'], nodes)))
    assert all(s['storageHealthy'] for s in result.values()), result
    SAMPLES.append(dict(at=datetime.now(timezone.utc).isoformat(), nodes=result))
    return result


def await_leader(nodes, label):
    deadline = time.monotonic() + 25
    stable = None
    count = 0
    while time.monotonic() < deadline:
        try:
            state = statuses(nodes)
            leaders = [n for n, s in state.items() if s['role'] == 'LEADER']
            terms = {s['currentTerm'] for s in state.values()}
            if len(leaders) == 1 and len(terms) == 1:
                current = (leaders[0], next(iter(terms)))
                count = count + 1 if stable == current else 1
                stable = current
                if count >= 4:
                    print(label, 'PASS', 'leader/term=', current, flush=True)
                    return leaders[0]
            else:
                count = 0
        except (OSError, AssertionError):
            count = 0
        time.sleep(0.3)
    raise AssertionError(f'{label}: no stable unique Leader, latest={SAMPLES[-1:]}')


try:
    print('project=', PROJECT, flush=True)
    print(compose('up', '-d'), flush=True)
    nodes = list(range(1, 6))
    first = await_leader(nodes, '5/5 election')
    other = next(n for n in nodes if n != first)
    compose('stop', '-t', '1', f'node-{first}', f'node-{other}')
    three = [n for n in nodes if n not in (first, other)]
    second = await_leader(three, '3/5 reelection after old Leader stops')
    compose('stop', '-t', '1', f'node-{second}')
    two = [n for n in three if n != second]
    time.sleep(2)
    initial_terms = {n: s['currentTerm'] for n, s in statuses(two).items()}
    until = time.monotonic() + 5
    observations = 0
    while time.monotonic() < until:
        state = statuses(two)
        assert all(s['role'] != 'LEADER' for s in state.values()), state
        observations += 1
        time.sleep(0.25)
    final = statuses(two)
    assert all(final[n]['currentTerm'] > initial_terms[n] for n in two), final
    print(f'2/5 PASS: {observations} samples, repeated campaigns, no newly elected Leader', flush=True)
    compose('start', f'node-{first}')
    third = await_leader(two + [first], '3/5 restored')
    compose('start', f'node-{other}', f'node-{second}')
    leader = await_leader(nodes, '5/5 restored')
    before = statuses(nodes)[leader]
    sender = next(n for n in nodes if n != leader)
    higher = before['currentTerm'] + 100
    vote = rpc(leader, 'REQUEST_VOTE', dict(term=higher, candidateId=f'node-{sender}',
               lastLogIndex=0, lastLogTerm=0), sender=f'node-{sender}', peer=True)
    assert vote['type'] == 'REQUEST_VOTE_RESPONSE' and vote['payload']['term'] == higher, vote
    stepped = rpc(leader)['payload']
    assert stepped['role'] == 'FOLLOWER' and stepped['currentTerm'] == higher, stepped
    sql = compose('exec', '-T', f'pg-{leader}', 'psql', '-U', 'railway_test', '-d',
                  f'railway_node{leader}', '-Atc', 'SELECT current_term, voted_for FROM raft_meta WHERE singleton=1')
    assert sql.strip() == f'{higher}|node-{sender}', sql
    (OUT / 'higher-term-postgresql.txt').write_text(sql)
    print('higher-term RequestVote PASS: durable term/vote and FOLLOWER status', flush=True)
    await_leader(nodes, '5/5 reelection after higher-term injection')
    for n in nodes:
        sql = compose('exec', '-T', f'pg-{n}', 'psql', '-U', 'railway_test', '-d', f'railway_node{n}',
                      '-Atc', 'SELECT current_term, voted_for FROM raft_meta; SELECT count(*) FROM tickets; SELECT last_applied FROM raft_apply_progress;')
        (OUT / f'pg-{n}.txt').write_text(sql)
    (OUT / 'RESULT.txt').write_text('PASS: 5/5, 3/5 reelection, 2/5 no new Leader, restore, higher-term step-down; election only.\n')
    print('PASS: real TCP election + PostgreSQL. No distributed booking/commit acceptance claim.', flush=True)
finally:
    (OUT / 'status-samples.json').write_text(json.dumps(SAMPLES, indent=2))
    try:
        (OUT / 'containers.txt').write_text(compose('ps', '-a'))
        for n in range(1, 6):
            (OUT / f'node-{n}.log').write_text(compose('logs', '--no-color', f'node-{n}'))
    finally:
        print(compose('down', '-v', '--remove-orphans'), flush=True)
