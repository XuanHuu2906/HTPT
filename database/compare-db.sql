-- ==============================================================================
-- Railway Distributed System — Nhóm 6
-- File: database/compare-db.sql
-- Mục tiêu: Đối chiếu tính nhất quán giữa các cơ sở dữ liệu PostgreSQL trước mốc G5.
-- Owner: TV2 (Phạm Minh Tuấn)
--
-- NGUYÊN TẮC THIẾT KẾ:
-- 1. CHỈ ĐỌC: Chạy trong transaction READ ONLY, không thay đổi dữ liệu, không ghi log.
--    Không đụng raft_meta và raft_log (được phép khác nhau giữa các node).
-- 2. ĐỘC LẬP MỖI NODE: Chạy trực tiếp trên từng PostgreSQL instance của từng node,
--    không dùng dblink, không dùng postgres_fdw, không cần quyền superuser/ghi.
-- 3. ĐẦU RA ỔN ĐỊNH: Định dạng chuẩn hóa `section|key|value` với ORDER BY khóa chính;
--    timestamp chuẩn hóa UTC; NULL chuẩn hóa thành chuỗi 'NULL';
--    không phụ thuộc OID, thời gian máy cục bộ hay thứ tự vật lý của row.
--
-- QUY TẮC ĐỐI CHIẾU GIỮA CÁC NODE (CROSS-NODE CONSISTENCY RULES):
-- 1. CÙNG lastApplied:
--    Nếu Node A và Node B có `last_applied` BẰNG NHAU, thì `state_hash` BẮT BUỘC
--    phải BẰNG NHAU.
--    - Nếu `state_hash` khác nhau: DIVERGENCE (Lỗi phân kỳ dữ liệu nghiêm trọng).
-- 2. KHÁC lastApplied:
--    Nếu Node A có `last_applied` THẤP HƠN Node B:
--    - Đây là LAG hợp lệ (Node A đang catch-up log).
--    - KHÔNG ĐƯỢC so sánh state_hash của node lag với node đi trước rồi kết luận DIVERGENCE.
--    - Chờ Node A catch-up đến cùng commitIndex / lastApplied rồi đối chiếu lại.
-- 3. BẤT BIẾN CỤC BỘ:
--    Tất cả các dòng `CHECK|*` trên mỗi node phải là `PASS`. Nếu có `FAIL` thì node
--    đó vi phạm bất biến dữ liệu (INVARIANT VIOLATION).
--
-- CÁCH CHẠY BẰNG PSQL:
--   psql -h <host> -p <port> -U <user> -d <dbname> -q -t -A -F '|' -f database/compare-db.sql
--
-- Chạy trên schema cụ thể (ví dụ fixture test):
--   psql -h <host> -p <port> -U <user> -d <dbname> -v target_schema=<schema_name> -q -t -A -F '|' -f database/compare-db.sql
-- ==============================================================================

\set ON_ERROR_STOP on

\if :{?target_schema}
  SET search_path TO :"target_schema";
\endif

BEGIN TRANSACTION READ ONLY;

-- ------------------------------------------------------------------------------
-- 1. TIẾN ĐỘ APPLY (RAFT APPLY PROGRESS)
-- ------------------------------------------------------------------------------
SELECT 'PROGRESS', 'last_applied', COALESCE(last_applied::text, '0')
FROM raft_apply_progress
WHERE singleton = 1;

SELECT 'PROGRESS', 'applied_results_count', COUNT(*)::text
FROM request_results;

SELECT 'PROGRESS', 'max_applied_log_index', COALESCE(MAX(applied_log_index)::text, '0')
FROM request_results;

-- ------------------------------------------------------------------------------
-- 2. CHI TIẾT TRẠNG THÁI GHẾ (SEATS DIGEST)
-- ------------------------------------------------------------------------------
SELECT 'SEAT',
       trip_id || '/' || seat_id,
       status || '|' || COALESCE(current_ticket_id, 'NULL')
FROM seats
ORDER BY trip_id, seat_id;

SELECT 'SUMMARY', 'seats_count', COUNT(*)::text
FROM seats;

SELECT 'SUMMARY', 'seats_hash',
       COALESCE(encode(sha256(string_agg(
         trip_id || '|' || seat_id || '|' || status || '|' || COALESCE(current_ticket_id, 'NULL'),
         E'\n' ORDER BY trip_id, seat_id
       )::bytea), 'hex'), 'EMPTY')
FROM seats;

-- ------------------------------------------------------------------------------
-- 3. CHI TIẾT DANH SÁCH VÉ (TICKETS DIGEST)
-- ------------------------------------------------------------------------------
SELECT 'TICKET',
       t.ticket_id,
       t.trip_id || '|' || t.seat_id || '|' || t.status || '|' ||
       COALESCE(r.request_id::text, 'NULL') || '|' ||
       t.passenger_name || '|' ||
       replace(to_char(t.issued_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.MS'), ' ', 'T') || 'Z|' ||
       COALESCE(replace(to_char(t.cancelled_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.MS'), ' ', 'T') || 'Z', 'NULL')
FROM tickets t
LEFT JOIN request_results r ON r.ticket_id = t.ticket_id AND r.state = 'APPLIED_SUCCESS'
ORDER BY t.ticket_id;

SELECT 'SUMMARY', 'tickets_count', COUNT(*)::text
FROM tickets;

SELECT 'SUMMARY', 'tickets_hash',
       COALESCE(encode(sha256(string_agg(
         t.ticket_id || '|' || t.trip_id || '|' || t.seat_id || '|' || t.status || '|' ||
         COALESCE(r.request_id::text, 'NULL') || '|' || t.passenger_name || '|' ||
         replace(to_char(t.issued_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.MS'), ' ', 'T') || 'Z|' ||
         COALESCE(replace(to_char(t.cancelled_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.MS'), ' ', 'T') || 'Z', 'NULL'),
         E'\n' ORDER BY t.ticket_id
       )::bytea), 'hex'), 'EMPTY')
FROM tickets t
LEFT JOIN request_results r ON r.ticket_id = t.ticket_id AND r.state = 'APPLIED_SUCCESS';

-- ------------------------------------------------------------------------------
-- 4. KẾT QUẢ YÊU CẦU ĐÃ APPLY (REQUEST RESULTS DIGEST)
-- ------------------------------------------------------------------------------
SELECT 'RESULT',
       request_id::text,
       command_type || '|' || intent_fingerprint || '|' || state || '|' ||
       COALESCE(ticket_id, 'NULL') || '|' || COALESCE(reason_code, 'NULL') || '|' ||
       applied_log_index::text
FROM request_results
ORDER BY request_id;

SELECT 'SUMMARY', 'results_count', COUNT(*)::text
FROM request_results;

SELECT 'SUMMARY', 'results_hash',
       COALESCE(encode(sha256(string_agg(
         request_id::text || '|' || command_type || '|' || intent_fingerprint || '|' || state || '|' ||
         COALESCE(ticket_id, 'NULL') || '|' || COALESCE(reason_code, 'NULL') || '|' ||
         applied_log_index::text,
         E'\n' ORDER BY request_id
       )::bytea), 'hex'), 'EMPTY')
FROM request_results;

-- ------------------------------------------------------------------------------
-- 5. HASH TỔNG TOÀN BỘ TRẠNG THÁI (STATE HASH - KHÔNG BAO GỒM lastApplied)
-- ------------------------------------------------------------------------------
WITH hashes AS (
  SELECT
    (SELECT COALESCE(encode(sha256(string_agg(
      trip_id || '|' || seat_id || '|' || status || '|' || COALESCE(current_ticket_id, 'NULL'),
      E'\n' ORDER BY trip_id, seat_id
    )::bytea), 'hex'), 'EMPTY') FROM seats) AS s_hash,
    (SELECT COALESCE(encode(sha256(string_agg(
      t.ticket_id || '|' || t.trip_id || '|' || t.seat_id || '|' || t.status || '|' ||
      COALESCE(r.request_id::text, 'NULL') || '|' || t.passenger_name || '|' ||
      replace(to_char(t.issued_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.MS'), ' ', 'T') || 'Z|' ||
      COALESCE(replace(to_char(t.cancelled_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.MS'), ' ', 'T') || 'Z', 'NULL'),
      E'\n' ORDER BY t.ticket_id
    )::bytea), 'hex'), 'EMPTY') FROM tickets t LEFT JOIN request_results r ON r.ticket_id = t.ticket_id AND r.state = 'APPLIED_SUCCESS') AS t_hash,
    (SELECT COALESCE(encode(sha256(string_agg(
      request_id::text || '|' || command_type || '|' || intent_fingerprint || '|' || state || '|' ||
      COALESCE(ticket_id, 'NULL') || '|' || COALESCE(reason_code, 'NULL') || '|' ||
      applied_log_index::text,
      E'\n' ORDER BY request_id
    )::bytea), 'hex'), 'EMPTY') FROM request_results) AS r_hash
)
SELECT 'SUMMARY', 'state_hash',
       encode(sha256((s_hash || '|' || t_hash || '|' || r_hash)::bytea), 'hex')
FROM hashes;

-- ------------------------------------------------------------------------------
-- 6. KIỂM TRA BẤT BIẾN CỤC BỘ (LOCAL INVARIANT CHECKS)
-- ------------------------------------------------------------------------------

-- Check 1: Tối đa 1 vé ACTIVE cho mỗi (trip, seat)
SELECT 'CHECK', 'ACTIVE_TICKETS_PER_SEAT',
       CASE
         WHEN COUNT(*) = 0 THEN 'PASS|At most one ACTIVE ticket per seat'
         ELSE 'FAIL|Found ' || COUNT(*) || ' seats with multiple ACTIVE tickets'
       END
FROM (
  SELECT trip_id, seat_id, COUNT(*)
  FROM tickets
  WHERE status = 'ACTIVE'
  GROUP BY trip_id, seat_id
  HAVING COUNT(*) > 1
) dup;

-- Check 2: Trạng thái seats khớp hoàn hảo với tickets ACTIVE
WITH seat_issues AS (
  -- Ghế BOOKED nhưng không có vé ACTIVE hoặc current_ticket_id sai
  SELECT s.trip_id, s.seat_id, 'BOOKED seat has no matching ACTIVE ticket' AS issue
  FROM seats s
  LEFT JOIN tickets t ON s.trip_id = t.trip_id AND s.seat_id = t.seat_id AND t.status = 'ACTIVE'
  WHERE s.status = 'BOOKED' AND (t.ticket_id IS NULL OR s.current_ticket_id != t.ticket_id)
  UNION ALL
  -- Ghế AVAILABLE nhưng current_ticket_id NOT NULL hoặc tồn tại vé ACTIVE
  SELECT s.trip_id, s.seat_id, 'AVAILABLE seat has non-null ticket or ACTIVE ticket exists' AS issue
  FROM seats s
  LEFT JOIN tickets t ON s.trip_id = t.trip_id AND s.seat_id = t.seat_id AND t.status = 'ACTIVE'
  WHERE s.status = 'AVAILABLE' AND (s.current_ticket_id IS NOT NULL OR t.ticket_id IS NOT NULL)
  UNION ALL
  -- Vé ACTIVE nhưng ghế không phải BOOKED
  SELECT t.trip_id, t.seat_id, 'ACTIVE ticket exists but seat is not BOOKED' AS issue
  FROM tickets t
  JOIN seats s ON s.trip_id = t.trip_id AND s.seat_id = t.seat_id
  WHERE t.status = 'ACTIVE' AND (s.status != 'BOOKED' OR s.current_ticket_id != t.ticket_id)
)
SELECT 'CHECK', 'SEATS_MATCH_ACTIVE_TICKETS',
       CASE
         WHEN COUNT(*) = 0 THEN 'PASS|Seats table exactly matches active tickets'
         ELSE 'FAIL|Found ' || COUNT(*) || ' mismatch issues between seats and tickets'
       END
FROM seat_issues;

-- Check 3: Mọi vé có bản ghi request_results tương ứng và ngược lại
WITH missing_results AS (
  -- Vé ACTIVE nhưng không có request_results APPLIED_SUCCESS
  SELECT t.ticket_id, 'ACTIVE ticket has no APPLIED_SUCCESS result' AS issue
  FROM tickets t
  LEFT JOIN request_results r ON r.ticket_id = t.ticket_id AND r.state = 'APPLIED_SUCCESS'
  WHERE t.status = 'ACTIVE' AND r.request_id IS NULL
  UNION ALL
  -- request_results APPLIED_SUCCESS có ticket_id nhưng không có trong tickets
  SELECT r.ticket_id, 'APPLIED_SUCCESS result has no ticket in tickets table' AS issue
  FROM request_results r
  LEFT JOIN tickets t ON t.ticket_id = r.ticket_id
  WHERE r.state = 'APPLIED_SUCCESS' AND r.ticket_id IS NOT NULL AND t.ticket_id IS NULL
)
SELECT 'CHECK', 'TICKETS_HAVE_REQUEST_RESULTS',
       CASE
         WHEN COUNT(*) = 0 THEN 'PASS|All tickets have corresponding request_results'
         ELSE 'FAIL|Found ' || COUNT(*) || ' tickets without valid request_results'
       END
FROM missing_results;

-- Check 4: Không có request_results ở trạng thái UNKNOWN hoặc không hợp lệ
SELECT 'CHECK', 'NO_UNKNOWN_IN_RESULTS',
       CASE
         WHEN COUNT(*) = 0 THEN 'PASS|No UNKNOWN or invalid state in request_results'
         ELSE 'FAIL|Found ' || COUNT(*) || ' invalid request_results states'
       END
FROM request_results
WHERE state NOT IN ('APPLIED_SUCCESS', 'APPLIED_REJECTED');

-- Check 5: lastApplied >= max(applied_log_index) của request_results
WITH progress_check AS (
  SELECT COALESCE((SELECT last_applied FROM raft_apply_progress WHERE singleton = 1), 0) AS la,
         COALESCE((SELECT MAX(applied_log_index) FROM request_results), 0) AS max_res
)
SELECT 'CHECK', 'LAST_APPLIED_GE_MAX_RESULT_INDEX',
       CASE
         WHEN la >= max_res THEN 'PASS|lastApplied (' || la || ') >= max appliedLogIndex (' || max_res || ')'
         ELSE 'FAIL|lastApplied (' || la || ') < max appliedLogIndex (' || max_res || ')'
       END
FROM progress_check;

-- Check 6: Đúng 10 ghế G01-G10 cho SE-001
WITH seat_check AS (
  SELECT COUNT(*) AS total_seats,
         COUNT(*) FILTER (WHERE trip_id = 'SE-001' AND seat_id ~ '^G(0[1-9]|10)$') AS valid_seats
  FROM seats
)
SELECT 'CHECK', 'EXACT_10_SEATS_SE001',
       CASE
         WHEN total_seats = 10 AND valid_seats = 10 THEN 'PASS|Exactly 10 seats G01-G10 present for SE-001'
         ELSE 'FAIL|Expected exactly 10 seats G01-G10 for SE-001, found total=' || total_seats || ', valid=' || valid_seats
       END
FROM seat_check;

COMMIT;

