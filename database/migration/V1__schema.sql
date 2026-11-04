CREATE TABLE raft_meta (
  singleton SMALLINT PRIMARY KEY DEFAULT 1 CHECK (singleton = 1),
  current_term BIGINT NOT NULL DEFAULT 0 CHECK (current_term >= 0),
  voted_for VARCHAR(20)
);

INSERT INTO raft_meta(singleton, current_term, voted_for) VALUES (1, 0, NULL);

CREATE TABLE raft_log (
  log_index BIGINT PRIMARY KEY CHECK (log_index >= 1),
  term BIGINT NOT NULL CHECK (term >= 1),
  command_type VARCHAR(32) NOT NULL
    CHECK (command_type IN ('BOOK_TICKET', 'CANCEL_TICKET', 'NO_OP')),
  command_json JSONB NOT NULL
);

CREATE TABLE raft_apply_progress (
  singleton SMALLINT PRIMARY KEY DEFAULT 1 CHECK (singleton = 1),
  last_applied BIGINT NOT NULL DEFAULT 0 CHECK (last_applied >= 0)
);

INSERT INTO raft_apply_progress(singleton, last_applied) VALUES (1, 0);

CREATE TABLE trips (
  trip_id VARCHAR(30) PRIMARY KEY
);

CREATE TABLE seats (
  trip_id VARCHAR(30) NOT NULL REFERENCES trips(trip_id),
  seat_id VARCHAR(3) NOT NULL,
  status VARCHAR(12) NOT NULL CHECK (status IN ('AVAILABLE', 'BOOKED')),
  current_ticket_id VARCHAR(64),
  PRIMARY KEY (trip_id, seat_id),
  CHECK (seat_id ~ '^G(0[1-9]|10)$'),
  CHECK ((status = 'AVAILABLE' AND current_ticket_id IS NULL)
      OR (status = 'BOOKED' AND current_ticket_id IS NOT NULL))
);

CREATE TABLE tickets (
  ticket_id VARCHAR(64) PRIMARY KEY,
  trip_id VARCHAR(30) NOT NULL,
  seat_id VARCHAR(3) NOT NULL,
  passenger_name VARCHAR(120) NOT NULL,
  status VARCHAR(12) NOT NULL CHECK (status IN ('ACTIVE', 'CANCELLED')),
  issued_at TIMESTAMPTZ NOT NULL,
  cancelled_at TIMESTAMPTZ,
  FOREIGN KEY (trip_id, seat_id) REFERENCES seats(trip_id, seat_id)
);

CREATE UNIQUE INDEX ux_one_active_ticket_per_seat
  ON tickets(trip_id, seat_id) WHERE status = 'ACTIVE';

CREATE TABLE request_results (
  request_id UUID PRIMARY KEY,
  command_type VARCHAR(32) NOT NULL
    CHECK (command_type IN ('BOOK_TICKET', 'CANCEL_TICKET')),
  intent_fingerprint CHAR(64) NOT NULL,
  state VARCHAR(20) NOT NULL
     CHECK (state IN ('APPLIED_SUCCESS', 'APPLIED_REJECTED')),
  reason_code VARCHAR(40),
  ticket_id VARCHAR(64),
  applied_log_index BIGINT NOT NULL CHECK (applied_log_index >= 1),
  result_json JSONB NOT NULL
);

CREATE TABLE business_events (
  log_index BIGINT PRIMARY KEY CHECK (log_index >= 1),
  request_id UUID NOT NULL,
  event_type VARCHAR(40) NOT NULL,
  event_json JSONB NOT NULL
);

-- Seed chỉ dành cho cụm mới đồng nhất, không chạy lại/không reset riêng một Node.
INSERT INTO trips(trip_id) VALUES ('SE-001');

INSERT INTO seats(trip_id, seat_id, status, current_ticket_id) VALUES
  ('SE-001', 'G01', 'AVAILABLE', NULL),
  ('SE-001', 'G02', 'AVAILABLE', NULL),
  ('SE-001', 'G03', 'AVAILABLE', NULL),
  ('SE-001', 'G04', 'AVAILABLE', NULL),
  ('SE-001', 'G05', 'AVAILABLE', NULL),
  ('SE-001', 'G06', 'AVAILABLE', NULL),
  ('SE-001', 'G07', 'AVAILABLE', NULL),
  ('SE-001', 'G08', 'AVAILABLE', NULL),
  ('SE-001', 'G09', 'AVAILABLE', NULL),
  ('SE-001', 'G10', 'AVAILABLE', NULL);
