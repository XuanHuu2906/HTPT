package vn.nhom6.railway.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.nhom6.railway.client.harness.LanFaultObserver;
import vn.nhom6.railway.client.network.ClusterStatusClient.DataSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm thử bộ giám sát sự cố LanFaultObserver (TASK TV1-D26).
 */
public class LanFaultObserverTest {

    @Test
    @DisplayName("TC_OBSERVER_01: LanFaultObserver ghi nhận đầy đủ chuỗi sự kiện Leader và chuyển đổi UI")
    void testObserverLifecycleAndTimelineExport(@TempDir Path tempDir) throws IOException {
        LanFaultObserver observer = new LanFaultObserver();

        observer.onLeaderLost("node-1", "Socket connection dropped");
        observer.onSearchingLeader();
        observer.onLeaderDiscovered("node-2", DataSource.REAL);
        observer.onNoLeaderAvailable();
        observer.onBookingStateTransition("REQ-OBS-01", "SUBMITTING", "NO_QUORUM", "AVAILABLE", "Không đủ đa số");

        assertEquals(5, observer.getEventLog().size(), "Phải ghi nhận đúng 5 sự kiện");
        assertEquals(5, observer.getTimeline().getEntries().size(), "Timeline phải có 5 bản ghi chuyển đổi trạng thái");

        Path csvPath = tempDir.resolve("observer-test.csv");
        observer.exportTimelineCsv(csvPath);

        assertTrue(Files.exists(csvPath), "Tệp CSV timeline phải được xuất ra thành công");
        List<String> lines = Files.readAllLines(csvPath);
        assertEquals(6, lines.size(), "Tệp CSV phải có dòng tiêu đề + 5 bản ghi");
    }
}
