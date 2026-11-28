package vn.nhom6.railway.client.network;

import javafx.collections.ObservableList;
import javafx.scene.layout.VBox;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;
import vn.nhom6.railway.client.viewmodel.TicketViewModel;

import java.util.Map;
import java.util.function.Consumer;

/**
 * Giao diện cầu nối cô lập hoàn toàn các thành phần giả lập chỉ-dùng-cho-dev/test
 * khỏi luồng thực thi Release (TASK TV1-D28).
 * <p>
 * Bất biến đóng băng:
 * <ul>
 *   <li>Chữ ký giao diện này KHÔNG chứa bất kỳ kiểu {@code Mock*} hay {@code Fake*} nào.</li>
 *   <li>Ở chế độ {@code release.mode=true}, {@link DevSupportBridge} chặn cứng và ném ngoại lệ cấu hình
 *       ngay lập tức, đồng thời bản đóng gói {@code -Prelease} loại bỏ toàn bộ gói {@code client.dev}
 *       cũng như các lớp {@code Mock*}/{@code Fake*} khỏi tệp JAR.</li>
 * </ul>
 */
public interface DevSupportSpi {

    BookingClient createDevBookingClient();

    ClusterStatusClient createDevClusterStatusClient();

    ReadAdapter createDevReadAdapter();

    boolean isDevMockBookingClient(BookingClient client);

    String getMockSeatBadgeLabel();

    String getMockTicketNoticeLabel();

    ObservableList<SeatViewModel> createDevMockSeats();

    Map<String, SeatStatus> createInitialMockSnapshot();

    Map<String, SeatStatus> createRefreshedMockSnapshot();

    ObservableList<TicketViewModel> getSharedDevTickets();

    void addDevTicket(TicketViewModel ticket);

    void syncDevSeatStatusChange(String seatId, SeatStatus status);

    void syncDevSnapshot(Map<String, SeatStatus> snapshot);

    void resetDevTicketsToDefault();

    void attachDevScenarioRow(VBox mockPanel, Consumer<String> statusUpdater);

    void configureNextDevCancelOutcome(String outcomeKind);
}
