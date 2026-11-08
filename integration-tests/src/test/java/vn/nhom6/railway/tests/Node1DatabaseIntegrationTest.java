package vn.nhom6.railway.tests;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.booking.ConnectionFactory;
import vn.nhom6.railway.persistence.DbConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * TV6-D07: Kiểm thử DB một Node (Node 1 - postgres-1).
 *
 * Yêu cầu đặc tả (SPEC-05 §1, §4, §4.1; SPEC-08 §3; AGENTS.md §0, §5):
 * 1. Chạy migration V1__schema.sql và V2__seed.sql lên pg1 (railway_node1).
 * 2. Đọc và kiểm tra chính xác 10 ghế (G01 đến G10) của chuyến SE-001 qua JDBC.
 * 3. BẢO MẬT & CÔ LẬP: Tuyệt đối KHÔNG đăng nhập bằng credentials của node khác.
 * 4. Tất cả 10 ghế ban đầu phải có trạng thái AVAILABLE và current_ticket_id là NULL.
 * 5. Khởi tạo Raft metadata (current_term = 0, voted_for = NULL) và apply progress (last_applied = 0).
 */
public class Node1DatabaseIntegrationTest {

    public static final String NODE1_DEFAULT_HOST = "127.0.0.1";
    public static final int NODE1_DEFAULT_PORT = 5433;
    public static final String NODE1_DEFAULT_DB = "railway_node1";
    public static final String NODE1_DEFAULT_USER = "railway";
    public static final String NODE1_DEFAULT_PASSWORD = "railway_secret_local_only";
    public static final String EXPECTED_TRIP_ID = "SE-001";

    public static final Set<String> FORBIDDEN_DATABASES = Set.of(
            "railway_node2", "railway_node3", "railway_node4", "railway_node5"
    );

    public static final Set<Integer> FORBIDDEN_PORTS = Set.of(
            5434, 5435, 5436, 5437
    );

    public static final Set<String> REQUIRED_TABLES = Set.of(
            "raft_meta", "raft_log", "raft_apply_progress", "trips",
            "seats", "tickets", "request_results", "business_events"
    );

    public static final List<String> EXPECTED_SEAT_IDS = List.of(
            "G01", "G02", "G03", "G04", "G05", "G06", "G07", "G08", "G09", "G10"
    );

    public record SeatRecord(String tripId, String seatId, String status, String currentTicketId) {}

    /**
     * Tạo DbConfig cho Node 1, kiểm tra nghiêm ngặt không vi phạm ranh giới cô lập.
     */
    public static DbConfig createNode1Config() {
        String host = getEnvOrDefault("NODE1_DB_HOST", NODE1_DEFAULT_HOST);
        int port = Integer.parseInt(getEnvOrDefault("NODE1_DB_PORT", String.valueOf(NODE1_DEFAULT_PORT)));
        String dbName = getEnvOrDefault("NODE1_DB_NAME", NODE1_DEFAULT_DB);
        String user = getEnvOrDefault("NODE1_DB_USER", NODE1_DEFAULT_USER);
        String password = getEnvOrDefault("NODE1_DB_PASSWORD", NODE1_DEFAULT_PASSWORD);

        // Quy tắc bất biến: Không đăng nhập bằng credentials hoặc database của node khác
        validateNode1Isolation(dbName, port);

        return new DbConfig(host, port, dbName, user, password);
    }

    /**
     * Xác thực tính cô lập của Node 1 theo AGENTS.md và SPEC-08.
     */
    public static void validateNode1Isolation(String dbName, int port) {
        if (FORBIDDEN_DATABASES.contains(dbName.toLowerCase())) {
            throw new SecurityException("VI PHAM AN TOAN: Khong duoc phep su dung database cua node khac ("
                    + dbName + ") trong test Node 1!");
        }
        if (FORBIDDEN_PORTS.contains(port)) {
            throw new SecurityException("VI PHAM AN TOAN: Khong duoc phep ket noi vao cong host cua node khac ("
                    + port + ") trong test Node 1!");
        }
    }

    /**
     * Kiểm tra sự tồn tại của 8 bảng bắt buộc trong PostgreSQL schema.
     */
    public static void verifyTables(Connection connection) throws SQLException {
        Set<String> foundTables = new HashSet<>();
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")) {
            while (rs.next()) {
                foundTables.add(rs.getString("table_name").toLowerCase());
            }
        }

        for (String required : REQUIRED_TABLES) {
            if (!foundTables.contains(required.toLowerCase())) {
                throw new AssertionError("Thieu bang bat buoc trong database: " + required);
            }
        }
    }

    /**
     * Đọc danh sách ghế của chuyến đi SE-001.
     */
    public static List<SeatRecord> querySeats(Connection connection, String tripId) throws SQLException {
        List<SeatRecord> seats = new ArrayList<>();
        String sql = "SELECT trip_id, seat_id, status, current_ticket_id FROM seats WHERE trip_id = ? ORDER BY seat_id ASC";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tripId);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    seats.add(new SeatRecord(
                            rs.getString("trip_id"),
                            rs.getString("seat_id"),
                            rs.getString("status"),
                            rs.getString("current_ticket_id")
                    ));
                }
            }
        }
        return seats;
    }

    /**
     * Xác thực danh sách 10 ghế theo SPEC-05 và AC01.
     */
    public static void verifySeats(List<SeatRecord> seats) {
        Assertions.assertEquals(10, seats.size(), "Tong so ghe tren chuyen SE-001 phai chinh xac la 10 ghe");

        List<String> actualSeatIds = seats.stream().map(SeatRecord::seatId).toList();
        Assertions.assertEquals(EXPECTED_SEAT_IDS, actualSeatIds,
                "Danh sach ma ghe phai dung 10 ghe tu G01 den G10 theo thu tu tang dan");

        for (SeatRecord seat : seats) {
            Assertions.assertEquals(EXPECTED_TRIP_ID, seat.tripId(), "Trip ID phai la " + EXPECTED_TRIP_ID);
            Assertions.assertEquals("AVAILABLE", seat.status(),
                    "Trang thai ban dau cua ghe " + seat.seatId() + " phai la AVAILABLE");
            Assertions.assertNull(seat.currentTicketId(),
                    "current_ticket_id ban dau cua ghe " + seat.seatId() + " phai la NULL");
        }
    }

    /**
     * Kiểm tra trạng thái khởi tạo của Raft Metadata và Apply Progress.
     */
    public static void verifyRaftMetadata(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(
                    "SELECT singleton, current_term, voted_for FROM raft_meta WHERE singleton = 1")) {
                Assertions.assertTrue(rs.next(), "raft_meta phai co singleton row (singleton=1)");
                Assertions.assertEquals(0L, rs.getLong("current_term"), "current_term ban dau phai la 0");
                Assertions.assertNull(rs.getString("voted_for"), "voted_for ban dau phai la NULL");
            }

            try (ResultSet rs = statement.executeQuery(
                    "SELECT singleton, last_applied FROM raft_apply_progress WHERE singleton = 1")) {
                Assertions.assertTrue(rs.next(), "raft_apply_progress phai co singleton row (singleton=1)");
                Assertions.assertEquals(0L, rs.getLong("last_applied"), "last_applied ban dau phai la 0");
            }
        }
    }

    /**
     * Kiểm tra rằng không có vé ACTIVE nào được tạo trước khi có lệnh đặt vé.
     */
    public static void verifyNoActiveTickets(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM tickets WHERE status = 'ACTIVE'")) {
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals(0, rs.getInt(1), "Khong duoc co ve ACTIVE nao trong database ban dau");
        }
    }

    @Test
    @DisplayName("TV6-D07: Kiem thu ket noi, schema va 10 ghe cua Node 1 qua JDBC ConnectionFactory")
    void testNode1DatabaseThroughJdbc() {
        DbConfig config = createNode1Config();
        ConnectionFactory factory = new ConnectionFactory(config, 2, 2);

        boolean isReachable = false;
        try {
            isReachable = factory.isHealthy();
        } catch (Exception ignored) {
            // Container chua khoi dong tren host
        }

        Assumptions.assumeTrue(isReachable,
                "Node 1 PostgreSQL (127.0.0.1:5433) chua khoi dong. Bo qua JUnit test de bao ve pipeline.");

        try (Connection connection = factory.getConnection()) {
            verifyTables(connection);
            verifyRaftMetadata(connection);
            List<SeatRecord> seats = querySeats(connection, EXPECTED_TRIP_ID);
            verifySeats(seats);
            verifyNoActiveTickets(connection);
        } catch (SQLException e) {
            Assertions.fail("Loi SQL trong qua trinh kiem thu Node 1: " + e.getMessage(), e);
        }
    }

    /**
     * Entry point CLI de chay truc tiep tu script PowerShell hoac dong lenh.
     */
    public static void main(String[] args) {
        System.out.println("================================================================================");
        System.out.println(" TV6-D07: KIEM THU CO SO DU LIEU NODE 1 (NODE 1 DATABASE VERIFICATION)");
        System.out.println("================================================================================");

        DbConfig config;
        try {
            config = createNode1Config();
            System.out.println("[CONFIG] Host     : " + config.host() + ":" + config.port());
            System.out.println("[CONFIG] Database : " + config.dbName());
            System.out.println("[CONFIG] User     : " + config.user());
            System.out.println("[STATUS] Kiem tra co lap: [PASS] Dung Node 1 credentials, khong dung credentials node khac.");
        } catch (Exception e) {
            System.err.println("[FAIL] Loi cau hinh / vi pham co lap an toan: " + e.getMessage());
            System.exit(1);
            return;
        }

        ConnectionFactory factory = new ConnectionFactory(config, 3, 3);
        try (Connection connection = factory.getConnection()) {
            System.out.println("[STATUS] Ket noi JDBC thanh cong toi PostgreSQL Node 1!");

            // 1. Kiem tra bang
            verifyTables(connection);
            System.out.println("[PASS] Kiem tra 8 bang bat buoc: DAY DU");

            // 2. Kiem tra Raft Meta
            verifyRaftMetadata(connection);
            System.out.println("[PASS] Kiem tra Raft metadata (current_term=0, last_applied=0): CHINH XAC");

            // 3. Doc 10 ghe
            List<SeatRecord> seats = querySeats(connection, EXPECTED_TRIP_ID);
            verifySeats(seats);

            System.out.println("\n[SQL EVIDENCE] Bang 10 ghe doc duoc tu Node 1 Database (trip_id: " + EXPECTED_TRIP_ID + "):");
            System.out.println("+---------+---------+-----------+-------------------+");
            System.out.println("| TRIP_ID | SEAT_ID | STATUS    | CURRENT_TICKET_ID |");
            System.out.println("+---------+---------+-----------+-------------------+");
            for (SeatRecord s : seats) {
                System.out.printf("| %-7s | %-7s | %-9s | %-17s |\n",
                        s.tripId(), s.seatId(), s.status(), s.currentTicketId() == null ? "NULL" : s.currentTicketId());
            }
            System.out.println("+---------+---------+-----------+-------------------+");

            verifyNoActiveTickets(connection);
            System.out.println("[PASS] Kiem tra so ve ACTIVE ban dau: 0 ve");

            System.out.println("\n--------------------------------------------------------------------------------");
            System.out.println(" KET LUAN: KIEM THU DB NODE 1 HOAN TAT VA DAT TAT CA BAT BIEN AN TOAN!");
            System.out.println("--------------------------------------------------------------------------------");
            System.exit(0);

        } catch (Exception e) {
            System.err.println("[ERROR] Kiem thu that bai: " + e.getMessage());
            System.exit(2);
        }
    }

    private static String getEnvOrDefault(String name, String defaultValue) {
        String val = System.getenv(name);
        return (val != null && !val.isBlank()) ? val.trim() : defaultValue;
    }
}

