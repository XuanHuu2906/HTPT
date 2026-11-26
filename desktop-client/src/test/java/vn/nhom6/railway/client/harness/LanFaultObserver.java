package vn.nhom6.railway.client.harness;

import vn.nhom6.railway.client.network.ClusterStatusClient.DataSource;
import vn.nhom6.railway.client.network.LeaderStateListener;
import vn.nhom6.railway.client.support.UiFaultTimeline;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * BỘ GIÁM SÁT SỰ CỐ LAN (LAN FAULT OBSERVER) CHO G6 NGHIỆM THU (TASK TV1-D26).
 * <p>
 * Lắng nghe toàn bộ sự kiện mất kết nối, thay đổi Leader, mất Quorum và chuyển đổi trạng thái UI.
 * Được thiết kế để cắm trực tiếp vào ứng dụng Desktop Client khi TV6 và TV5 ráp mạng 5 laptop thật tại G6.
 */
public class LanFaultObserver implements LeaderStateListener {

    private final UiFaultTimeline timeline;
    private final String sourceLabel;
    private final List<String> eventLog = new CopyOnWriteArrayList<>();

    public LanFaultObserver(UiFaultTimeline timeline, String sourceLabel) {
        this.timeline = timeline != null ? timeline : new UiFaultTimeline();
        this.sourceLabel = sourceLabel != null ? sourceLabel : "FAKE";
    }

    public LanFaultObserver() {
        this(new UiFaultTimeline(), "FAKE");
    }

    public UiFaultTimeline getTimeline() {
        return timeline;
    }

    public List<String> getEventLog() {
        return Collections.unmodifiableList(new ArrayList<>(eventLog));
    }

    @Override
    public void onLeaderLost(String oldLeaderId, String reason) {
        String msg = String.format("Leader %s mất kết nối: %s", oldLeaderId, reason);
        eventLog.add(Instant.now() + " | LEADER_LOST | " + msg);
        timeline.recordTransition("CLUSTER", "LEADER_ACTIVE", "LEADER_LOST", "RECONNECTING", msg, sourceLabel);
    }

    @Override
    public void onSearchingLeader() {
        String msg = "Đang thăm dò trạng thái các node trong cụm để tìm Leader...";
        eventLog.add(Instant.now() + " | SEARCHING_LEADER | " + msg);
        timeline.recordTransition("CLUSTER", "RECONNECTING", "SEARCH_START", "SEARCHING", msg, sourceLabel);
    }

    @Override
    public void onLeaderDiscovered(String newLeaderId, DataSource source) {
        String msg = String.format("Đã phát hiện Leader mới: %s (nguồn: %s)", newLeaderId, source);
        eventLog.add(Instant.now() + " | LEADER_DISCOVERED | " + msg);
        timeline.recordTransition("CLUSTER", "SEARCHING", "LEADER_FOUND", "LEADER_ACTIVE", msg, sourceLabel);
    }

    @Override
    public void onNoLeaderAvailable() {
        String msg = "Cụm hiện không có Leader khả dụng (đang bầu cử hoặc mất Quorum 3/5)";
        eventLog.add(Instant.now() + " | NO_LEADER | " + msg);
        timeline.recordTransition("CLUSTER", "SEARCHING", "NO_LEADER", "NO_QUORUM", msg, sourceLabel);
    }

    public void onBookingStateTransition(String requestId, String stateBefore, String event, String stateAfter, String uiMessage) {
        String logEntry = String.format("reqId=%s | %s --(%s)--> %s | %s", requestId, stateBefore, event, stateAfter, uiMessage);
        eventLog.add(Instant.now() + " | UI_TRANSITION | " + logEntry);
        timeline.recordTransition(requestId, stateBefore, event, stateAfter, uiMessage, sourceLabel);
    }

    public void exportTimelineCsv(Path targetCsvPath) throws IOException {
        timeline.exportCsv(targetCsvPath);
    }
}
