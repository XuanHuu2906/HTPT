-- Seed chỉ dành cho cụm mới đồng nhất, không chạy/reset riêng một Node đang tham gia consensus.
INSERT INTO trips(trip_id) VALUES ('SE-001');
INSERT INTO seats(trip_id, seat_id, status, current_ticket_id)
SELECT 'SE-001', 'G' || LPAD(n::text, 2, '0'), 'AVAILABLE', NULL
FROM generate_series(1,10) AS n;
