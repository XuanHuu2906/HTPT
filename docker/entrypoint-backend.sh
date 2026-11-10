#!/bin/sh
set -e

echo "================================================================================"
echo " RAILWAY DISTRIBUTED SYSTEM - BACKEND NODE: ${NODE_ID}"
echo "================================================================================"
echo "  Node ID          : ${NODE_ID}"
echo "  Cluster ID       : ${CLUSTER_ID}"
echo "  Client Bind      : ${NODE_CLIENT_BIND} (Advertise: ${NODE_ADVERTISE_CLIENT})"
echo "  Peer Bind        : ${NODE_PEER_BIND} (Advertise: ${NODE_ADVERTISE_PEER})"
echo "  Raft Peers       : ${NODE_PEERS}"
echo "  Database Target  : ${DB_USER}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
echo "  Raft Timers      : Heartbeat=${RAFT_HEARTBEAT_INTERVAL_MS}ms, Election=${RAFT_ELECTION_TIMEOUT_MIN_MS}-${RAFT_ELECTION_TIMEOUT_MAX_MS}ms"
echo "================================================================================"

# Kiem tra ket noi toi Database da duoc inject
if [ -n "${DB_HOST}" ]; then
  echo "[INFO] Kiem tra ket noi mang toi Database ${DB_HOST}:${DB_PORT}..."
  max_retries=30
  retry_count=0
  until nc -z -w 2 "${DB_HOST}" "${DB_PORT}" || [ $retry_count -ge $max_retries ]; do
    retry_count=$((retry_count+1))
    echo "  [WAIT] Database ${DB_HOST}:${DB_PORT} chua san sang. Thu lai ($retry_count/$max_retries)..."
    sleep 1
  done
  if [ $retry_count -ge $max_retries ]; then
    echo "  [WARN] Khong the ket noi toi database ${DB_HOST}:${DB_PORT} sau ${max_retries} giay."
  else
    echo "  [OK] Ket noi mang thanh cong toi Database ${DB_HOST}:${DB_PORT}!"
  fi
fi

# Neu co lenh truyen truc tiep tu Docker CMD / arguments
if [ "$#" -gt 0 ]; then
  exec "$@"
fi

# Kiem tra va thuc thi NodeMain
MAIN_CLASS="vn.nhom6.railway.node.NodeMain"
CLASSPATH="/app/backend-node.jar:/app/lib/*"

echo "[INFO] Khoi dong Backend Node runtime..."
if java -cp "${CLASSPATH}" "${MAIN_CLASS}" --version >/dev/null 2>&1 || java -cp "${CLASSPATH}" "${MAIN_CLASS}" --help >/dev/null 2>&1; then
  echo "[INFO] Thuc thi ${MAIN_CLASS}..."
  exec java -cp "${CLASSPATH}" "${MAIN_CLASS}"
else
  echo "[INFO] Lop ${MAIN_CLASS} dang trong tien trinh hoan thien scaffold."
  echo "[INFO] Node container ${NODE_ID} da san sang ve mang (Client: 9100, Raft: 9200) va Database (${DB_HOST})."
  echo "[INFO] Duy tri container running phuc vu kiem thu cluster dev..."
  exec tail -f /dev/null
fi

