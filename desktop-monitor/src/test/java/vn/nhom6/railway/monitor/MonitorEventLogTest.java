package vn.nhom6.railway.monitor;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.monitor.MonitorController.NodeRow;
import vn.nhom6.railway.monitor.MonitorEventLog.EventType;
import vn.nhom6.railway.monitor.MonitorEventLog.MonitorEvent;
import vn.nhom6.railway.monitor.MonitorEventLog.RecoveryRow;
import vn.nhom6.railway.monitor.MonitorEventLog.SyncState;
import vn.nhom6.railway.protocol.dto.NodeStatusDto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV6-D23: Monitor recovery event history and lastApplied vs commitIndex UI fixture test suite.
 * Replays recorded/fake snapshots and TV3-D15 NodeStatusDto payloads without waiting for TV5-D23.
 * Real 5-node leader rejoin visualization is verified end-to-end at Gate G6.
 */
@DisplayName("TV6-D23: Monitor Recovery Event Log & Catch-Up Sync State Fixture Suite")
class MonitorEventLogTest {

    private static final Clock FIXED_CLOCK =
            Clock.fixed(Instant.parse("2026-11-23T04:20:00.000Z"), ZoneOffset.UTC);

    // -------------------------------------------------------------------------------------------
    // Test 1: Initial Snapshot — Lagging & Unhealthy Nodes Never Labeled SYNCED
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("1. Snapshot ban đầu: Hiển thị lastApplied/commitIndex và KHÔNG gắn nhãn SYNCED cho node đang lag/lỗi/offline")
    void initialSnapshot_recordsLeaderAndNeverMarksLaggingOrUnhealthyNodesAsSynced() {
        MonitorEventLog log = new MonitorEventLog(FIXED_CLOCK, 100);
        List<MonitorEvent> emitted = log.recordSnapshot(List.of(
                new NodeRow("node-1", "FOLLOWER", 8L, "ONLINE", 43L, 41L),
                new NodeRow("node-2", "FOLLOWER", 8L, "ONLINE", 43L, 43L),
                new NodeRow("node-3", "LEADER", 8L, "ONLINE", 43L, 43L),
                new NodeRow("node-4", null, null, "STORAGE_ERROR", null, null),
                new NodeRow("node-5", null, null, "OFFLINE", null, null)));

        RecoveryRow n1 = log.recoveryRowOf("node-1");
        RecoveryRow n2 = log.recoveryRowOf("node-2");
        RecoveryRow n3 = log.recoveryRowOf("node-3");
        RecoveryRow n4 = log.recoveryRowOf("node-4");
        RecoveryRow n5 = log.recoveryRowOf("node-5");

        assertEquals(SyncState.CATCHING_UP, n1.syncState());
        assertFalse(n1.isSynced(), "Node có lastApplied=41 < commitIndex=43 không được gắn nhãn SYNCED");
        assertEquals(2L, n1.lag());
        assertEquals("41 / 43 (lag -2)", n1.applyVsCommitDisplay());

        assertEquals(SyncState.SYNCED, n2.syncState());
        assertTrue(n2.isSynced());
        assertEquals(0L, n2.lag());
        assertEquals("43 / 43 (SYNCED)", n2.applyVsCommitDisplay());

        assertEquals(SyncState.SYNCED, n3.syncState());
        assertEquals(SyncState.OFFLINE, n4.syncState());
        assertEquals("— / —", n4.applyVsCommitDisplay());
        assertEquals(SyncState.OFFLINE, n5.syncState());

        assertTrue(emitted.stream().anyMatch(e -> e.type() == EventType.LEADER_CHANGE && "node-3".equals(e.nodeId())));
        assertTrue(emitted.stream().anyMatch(e -> e.type() == EventType.CATCH_UP_PROGRESS && "node-1".equals(e.nodeId())));
    }

    // -------------------------------------------------------------------------------------------
    // Test 2: Rejoined Node Behind Leader CommitIndex NEVER Labeled SYNCED
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("2. Node quay lại có local lastApplied==commitIndex (43/43) nhưng kém Leader (50/50) -> CATCHING_UP, KHÔNG SYNCED")
    void rejoinedNodeBehindLeaderCommit_neverLabeledSyncedEvenWhenLocalAppliedEqualsLocalCommit() {
        MonitorEventLog log = new MonitorEventLog(FIXED_CLOCK, 100);
        log.recordSnapshot(List.of(
                new NodeRow("node-1", "FOLLOWER", 9L, "ONLINE", 43L, 43L),
                new NodeRow("node-2", "FOLLOWER", 9L, "ONLINE", 50L, 50L),
                new NodeRow("node-3", "LEADER", 9L, "ONLINE", 50L, 50L),
                new NodeRow("node-4", "FOLLOWER", 9L, "ONLINE", 50L, 50L),
                new NodeRow("node-5", "FOLLOWER", 9L, "ONLINE", 50L, 50L)));

        RecoveryRow n1 = log.recoveryRowOf("node-1");
        assertEquals(SyncState.CATCHING_UP, n1.syncState(),
                "Node kém Leader commitIndex 50 không được gắn nhãn SYNCED dù local lastApplied==commitIndex==43");
        assertFalse(n1.isSynced());
        assertEquals(7L, n1.lag());
        assertEquals(50L, n1.referenceCommitIndex());
        assertEquals("43 / 43 (lag -7 vs leader 50)", n1.applyVsCommitDisplay());
    }

    // -------------------------------------------------------------------------------------------
    // Test 3: Stale-Term Old Leader Rejoin -> Catch-Up -> Only SYNCED After Full Catch-Up
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("3. Leader cũ quay lại (term cũ -> step-down -> catch-up từng bước): chỉ gắn SYNCED khi đạt đủ term & commitIndex")
    void staleTermOldLeaderRejoining_labeledCatchingUpUntilStepsDownAndCatchesUp() {
        MonitorEventLog log = new MonitorEventLog(FIXED_CLOCK, 100);

        // Snapshot 1: node-1 offline, node-3 is new Leader at term 9, commit=50
        log.recordSnapshot(List.of(
                new NodeRow("node-1", null, null, "OFFLINE", null, null),
                new NodeRow("node-2", "FOLLOWER", 9L, "ONLINE", 50L, 50L),
                new NodeRow("node-3", "LEADER", 9L, "ONLINE", 50L, 50L),
                new NodeRow("node-4", "FOLLOWER", 9L, "ONLINE", 50L, 50L),
                new NodeRow("node-5", "FOLLOWER", 9L, "ONLINE", 50L, 50L)));
        assertEquals(SyncState.OFFLINE, log.recoveryRowOf("node-1").syncState());

        // Snapshot 2: node-1 comes ONLINE with stale term 8 and old log 43/43
        List<MonitorEvent> step2 = log.recordSnapshot(List.of(
                new NodeRow("node-1", "FOLLOWER", 8L, "ONLINE", 43L, 43L),
                new NodeRow("node-2", "FOLLOWER", 9L, "ONLINE", 50L, 50L),
                new NodeRow("node-3", "LEADER", 9L, "ONLINE", 50L, 50L),
                new NodeRow("node-4", "FOLLOWER", 9L, "ONLINE", 50L, 50L),
                new NodeRow("node-5", "FOLLOWER", 9L, "ONLINE", 50L, 50L)));
        assertEquals(SyncState.CATCHING_UP, log.recoveryRowOf("node-1").syncState());
        assertFalse(log.recoveryRowOf("node-1").isSynced());
        assertTrue(step2.stream().anyMatch(e -> e.type() == EventType.NODE_ONLINE && "node-1".equals(e.nodeId())));
        assertTrue(step2.stream().anyMatch(e -> e.type() == EventType.CATCH_UP_PROGRESS && "node-1".equals(e.nodeId())));
        assertFalse(step2.stream().anyMatch(e -> e.type() == EventType.CATCH_UP_COMPLETE));

        // Snapshot 3: node-1 stepped down to term 9, commit=50, lastApplied=47 (still applying!)
        List<MonitorEvent> step3 = log.recordSnapshot(List.of(
                new NodeRow("node-1", "FOLLOWER", 9L, "ONLINE", 50L, 47L),
                new NodeRow("node-2", "FOLLOWER", 9L, "ONLINE", 50L, 50L),
                new NodeRow("node-3", "LEADER", 9L, "ONLINE", 50L, 50L),
                new NodeRow("node-4", "FOLLOWER", 9L, "ONLINE", 50L, 50L),
                new NodeRow("node-5", "FOLLOWER", 9L, "ONLINE", 50L, 50L)));
        assertEquals(SyncState.CATCHING_UP, log.recoveryRowOf("node-1").syncState());
        assertEquals(3L, log.recoveryRowOf("node-1").lag());
        assertEquals("47 / 50 (lag -3)", log.recoveryRowOf("node-1").applyVsCommitDisplay());
        assertTrue(step3.stream().anyMatch(e -> e.type() == EventType.CATCH_UP_PROGRESS && "node-1".equals(e.nodeId())));
        assertFalse(step3.stream().anyMatch(e -> e.type() == EventType.CATCH_UP_COMPLETE));

        // Snapshot 4: node-1 completes apply 50/50 at term 9 -> NOW truthfully SYNCED!
        List<MonitorEvent> step4 = log.recordSnapshot(List.of(
                new NodeRow("node-1", "FOLLOWER", 9L, "ONLINE", 50L, 50L),
                new NodeRow("node-2", "FOLLOWER", 9L, "ONLINE", 50L, 50L),
                new NodeRow("node-3", "LEADER", 9L, "ONLINE", 50L, 50L),
                new NodeRow("node-4", "FOLLOWER", 9L, "ONLINE", 50L, 50L),
                new NodeRow("node-5", "FOLLOWER", 9L, "ONLINE", 50L, 50L)));
        assertEquals(SyncState.SYNCED, log.recoveryRowOf("node-1").syncState());
        assertTrue(log.recoveryRowOf("node-1").isSynced());
        assertEquals(0L, log.recoveryRowOf("node-1").lag());
        assertEquals("50 / 50 (SYNCED)", log.recoveryRowOf("node-1").applyVsCommitDisplay());
        assertTrue(step4.stream().anyMatch(e -> e.type() == EventType.CATCH_UP_COMPLETE && "node-1".equals(e.nodeId())));
    }

    // -------------------------------------------------------------------------------------------
    // Test 4: Full 6-Step Failover + Rejoin + Catch-Up Replay Fixture
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("4. Replay chuỗi 6 bước: Leader fail -> bầu Leader mới -> node cũ online -> catch-up tiến dần -> SYNCED")
    void fullFailoverAndRecoveryReplay_emitsLeaderChangeOfflineOnlineCatchUpProgressAndComplete() {
        MonitorEventLog log = new MonitorEventLog(FIXED_CLOCK, 100);
        List<List<NodeRow>> recorded = List.of(
                // Step 1: Healthy cluster, node-1 Leader term 1, 10/10
                List.of(
                        new NodeRow("node-1", "LEADER", 1L, "ONLINE", 10L, 10L),
                        new NodeRow("node-2", "FOLLOWER", 1L, "ONLINE", 10L, 10L),
                        new NodeRow("node-3", "FOLLOWER", 1L, "ONLINE", 10L, 10L),
                        new NodeRow("node-4", "FOLLOWER", 1L, "ONLINE", 10L, 10L),
                        new NodeRow("node-5", "FOLLOWER", 1L, "ONLINE", 10L, 10L)),
                // Step 2: node-1 goes OFFLINE, cluster temporarily has no leader
                List.of(
                        new NodeRow("node-1", null, null, "OFFLINE", null, null),
                        new NodeRow("node-2", "FOLLOWER", 1L, "ONLINE", 10L, 10L),
                        new NodeRow("node-3", "CANDIDATE", 2L, "ONLINE", 10L, 10L),
                        new NodeRow("node-4", "FOLLOWER", 2L, "ONLINE", 10L, 10L),
                        new NodeRow("node-5", "FOLLOWER", 2L, "ONLINE", 10L, 10L)),
                // Step 3: node-3 elected Leader term 2, commits advance to 15
                List.of(
                        new NodeRow("node-1", null, null, "OFFLINE", null, null),
                        new NodeRow("node-2", "FOLLOWER", 2L, "ONLINE", 15L, 15L),
                        new NodeRow("node-3", "LEADER", 2L, "ONLINE", 15L, 15L),
                        new NodeRow("node-4", "FOLLOWER", 2L, "ONLINE", 15L, 15L),
                        new NodeRow("node-5", "FOLLOWER", 2L, "ONLINE", 15L, 15L)),
                // Step 4: node-1 rejoins ONLINE at 10/15 (still lagging)
                List.of(
                        new NodeRow("node-1", "FOLLOWER", 2L, "ONLINE", 15L, 10L),
                        new NodeRow("node-2", "FOLLOWER", 2L, "ONLINE", 15L, 15L),
                        new NodeRow("node-3", "LEADER", 2L, "ONLINE", 15L, 15L),
                        new NodeRow("node-4", "FOLLOWER", 2L, "ONLINE", 15L, 15L),
                        new NodeRow("node-5", "FOLLOWER", 2L, "ONLINE", 15L, 15L)),
                // Step 5: node-1 applies through 13/15 (still catching up)
                List.of(
                        new NodeRow("node-1", "FOLLOWER", 2L, "ONLINE", 15L, 13L),
                        new NodeRow("node-2", "FOLLOWER", 2L, "ONLINE", 15L, 15L),
                        new NodeRow("node-3", "LEADER", 2L, "ONLINE", 15L, 15L),
                        new NodeRow("node-4", "FOLLOWER", 2L, "ONLINE", 15L, 15L),
                        new NodeRow("node-5", "FOLLOWER", 2L, "ONLINE", 15L, 15L)),
                // Step 6: node-1 catches up to 15/15 -> SYNCED
                List.of(
                        new NodeRow("node-1", "FOLLOWER", 2L, "ONLINE", 15L, 15L),
                        new NodeRow("node-2", "FOLLOWER", 2L, "ONLINE", 15L, 15L),
                        new NodeRow("node-3", "LEADER", 2L, "ONLINE", 15L, 15L),
                        new NodeRow("node-4", "FOLLOWER", 2L, "ONLINE", 15L, 15L),
                        new NodeRow("node-5", "FOLLOWER", 2L, "ONLINE", 15L, 15L)));

        List<MonitorEvent> all = log.replay(recorded);
        assertEquals(all, log.events());

        // Monotonic local sequence numbers
        for (int i = 0; i < all.size(); i++) {
            assertEquals(i + 1L, all.get(i).sequence());
        }

        // Verify all 5 event types appeared in the recovery timeline
        assertEquals(3, log.eventsByType(EventType.LEADER_CHANGE).size(), "Initial node-1 -> NONE -> node-3");
        assertEquals(1, log.eventsByType(EventType.NODE_OFFLINE).size());
        assertEquals(1, log.eventsByType(EventType.NODE_ONLINE).size());
        assertEquals(2, log.eventsByType(EventType.CATCH_UP_PROGRESS).size(), "10/15 và 13/15");
        assertEquals(1, log.eventsByType(EventType.CATCH_UP_COMPLETE).size());

        // Verify per-node filter for node-1
        List<EventType> node1Types = log.eventsForNode("node-1").stream().map(MonitorEvent::type).toList();
        assertEquals(List.of(
                EventType.LEADER_CHANGE,
                EventType.NODE_OFFLINE,
                EventType.NODE_ONLINE,
                EventType.CATCH_UP_PROGRESS,
                EventType.CATCH_UP_PROGRESS,
                EventType.CATCH_UP_COMPLETE), node1Types);

        // Final banner shows 5/5 online & 5 synced
        String banner = log.summaryBanner();
        assertTrue(banner.contains("Leader: node-3"));
        assertTrue(banner.contains("Online: 5/5"));
        assertTrue(banner.contains("Synced: 5"));
        assertTrue(banner.contains("Catching-up: 0"));
        System.out.println("REPLAY_SUMMARY " + banner);
    }

    // -------------------------------------------------------------------------------------------
    // Test 5: No Leader or Split-Brain -> NEVER Labels Any Node SYNCED
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("5. Không có Leader hoặc phát hiện 2 Leader (split-brain): KHÔNG gắn nhãn SYNCED cho bất kỳ node nào")
    void noLeaderOrSplitBrain_neverLabelsAnyNodeSynced() {
        MonitorEventLog log = new MonitorEventLog(FIXED_CLOCK, 100);

        // 0 leaders (mid-election)
        log.recordSnapshot(List.of(
                new NodeRow("node-1", "FOLLOWER", 4L, "ONLINE", 20L, 20L),
                new NodeRow("node-2", "CANDIDATE", 5L, "ONLINE", 20L, 20L),
                new NodeRow("node-3", "FOLLOWER", 5L, "ONLINE", 20L, 20L),
                new NodeRow("node-4", "FOLLOWER", 5L, "ONLINE", 20L, 20L),
                new NodeRow("node-5", "FOLLOWER", 5L, "ONLINE", 20L, 20L)));
        for (RecoveryRow r : log.currentRecoveryRows()) {
            assertEquals(SyncState.UNKNOWN_PROGRESS, r.syncState());
            assertFalse(r.isSynced());
        }

        // 2 leaders observed (stale leader + new leader during partition)
        List<MonitorEvent> splitEvents = log.recordSnapshot(List.of(
                new NodeRow("node-1", "LEADER", 4L, "ONLINE", 20L, 20L),
                new NodeRow("node-2", "FOLLOWER", 4L, "ONLINE", 20L, 20L),
                new NodeRow("node-3", "LEADER", 5L, "ONLINE", 22L, 22L),
                new NodeRow("node-4", "FOLLOWER", 5L, "ONLINE", 22L, 22L),
                new NodeRow("node-5", "FOLLOWER", 5L, "ONLINE", 22L, 22L)));
        for (RecoveryRow r : log.currentRecoveryRows()) {
            assertEquals(SyncState.UNKNOWN_PROGRESS, r.syncState(),
                    "Khi có >1 Leader không được gắn nhãn SYNCED cho " + r.nodeId());
            assertFalse(r.isSynced());
        }
        assertTrue(splitEvents.stream().anyMatch(e -> e.type() == EventType.LEADER_CHANGE
                && "cluster".equals(e.nodeId())));
    }

    // -------------------------------------------------------------------------------------------
    // Test 6: Invalid Observation (lastApplied > commitIndex) Fails Closed to UNKNOWN_PROGRESS
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("6. Quan sát bất thường (lastApplied > commitIndex) không bao giờ được gắn nhãn SYNCED")
    void invalidLastAppliedGreaterThanCommitIndex_neverLabeledSynced() {
        NodeRow corrupted = new NodeRow("node-2", "FOLLOWER", 8L, "ONLINE", 40L, 45L);
        assertEquals(SyncState.UNKNOWN_PROGRESS, MonitorEventLog.evaluateSyncState(corrupted, 8L, 40L));
    }

    // -------------------------------------------------------------------------------------------
    // Test 7: Direct TV3-D15 NodeStatusDto Integration (Shared SPEC-03 Contract)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("7. Tích hợp trực tiếp NodeStatusDto (TV3-D15 / SPEC-03 §5) vào MonitorEventLog")
    void nodeStatusDtoIntegration_tv3D15PayloadsFlowCleanlyThroughMonitorEventLog() {
        MonitorEventLog log = new MonitorEventLog(FIXED_CLOCK, 100);
        String ts = "2026-11-23T04:20:00.000Z";
        List<NodeStatusDto> dtos = List.of(
                new NodeStatusDto("node-1", "FOLLOWER", 8L, "node-3", 45L, 43L, 41L, true,
                        Map.of("node-2", true, "node-3", true, "node-4", false, "node-5", true), ts),
                new NodeStatusDto("node-2", "FOLLOWER", 8L, "node-3", 45L, 43L, 43L, true,
                        Map.of("node-1", true, "node-3", true, "node-4", false, "node-5", true), ts),
                new NodeStatusDto("node-3", "LEADER", 8L, "node-3", 45L, 43L, 43L, true,
                        Map.of("node-1", true, "node-2", true, "node-4", false, "node-5", true), ts),
                new NodeStatusDto("node-4", null, null, null, null, null, null, false,
                        Map.of("node-1", false, "node-2", false, "node-3", false, "node-5", false), ts),
                new NodeStatusDto("node-5", "FOLLOWER", 8L, "node-3", 45L, 43L, 43L, true,
                        Map.of("node-1", true, "node-2", true, "node-3", true, "node-4", false), ts));

        log.recordStatusDtos(dtos);
        assertEquals(SyncState.CATCHING_UP, log.recoveryRowOf("node-1").syncState());
        assertEquals(SyncState.SYNCED, log.recoveryRowOf("node-2").syncState());
        assertEquals(SyncState.SYNCED, log.recoveryRowOf("node-3").syncState());
        assertEquals(SyncState.OFFLINE, log.recoveryRowOf("node-4").syncState());
        assertEquals("STORAGE_ERROR", log.recoveryRowOf("node-4").status());
        assertEquals(SyncState.SYNCED, log.recoveryRowOf("node-5").syncState());
    }

    // -------------------------------------------------------------------------------------------
    // Test 8: Async FakeNodeStatusAdapter + MonitorTcpClient (D14) Replay Into MonitorEventLog
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("8. Tích hợp MonitorTcpClient + FakeNodeStatusAdapter (D14): poll async qua chu kỳ offline -> catch-up -> synced")
    void asyncFakeNodeStatusAdapterReplay_throughMonitorTcpClient_tracksFullRecoveryCycle() throws Exception {
        MonitorTcpClient.FakeNodeStatusAdapter adapter = new MonitorTcpClient.FakeNodeStatusAdapter();
        MonitorEventLog log = new MonitorEventLog(FIXED_CLOCK, 100);

        try (MonitorTcpClient client = new MonitorTcpClient(adapter, Duration.ofMillis(500))) {
            // Round 1: default fixture (node-3 LEADER 43/43, node-1 lagging 41/43)
            log.recordSnapshot(pollAll(client));
            assertEquals(SyncState.CATCHING_UP, log.recoveryRowOf("node-1").syncState());
            assertEquals(SyncState.SYNCED, log.recoveryRowOf("node-3").syncState());

            // Round 2: node-1 goes offline
            adapter.setOffline("node-1");
            log.recordSnapshot(pollAll(client));
            assertEquals(SyncState.OFFLINE, log.recoveryRowOf("node-1").syncState());
            assertEquals(1, log.eventsByType(EventType.NODE_OFFLINE).size());

            // Round 3: node-1 returns online at 42/43 (still catching up!)
            ObjectNode catchingUp = MonitorTcpClient.FakeNodeStatusAdapter.fixture("node-1");
            catchingUp.put("lastApplied", 42L);
            adapter.setFixture("node-1", catchingUp, Duration.ZERO);
            log.recordSnapshot(pollAll(client));
            assertEquals(SyncState.CATCHING_UP, log.recoveryRowOf("node-1").syncState());
            assertEquals(1L, log.recoveryRowOf("node-1").lag());
            assertEquals(1, log.eventsByType(EventType.NODE_ONLINE).size());

            // Round 4: node-1 reaches 43/43 -> SYNCED
            ObjectNode caughtUp = MonitorTcpClient.FakeNodeStatusAdapter.fixture("node-1");
            caughtUp.put("lastApplied", 43L);
            adapter.setFixture("node-1", caughtUp, Duration.ZERO);
            log.recordSnapshot(pollAll(client));
            assertEquals(SyncState.SYNCED, log.recoveryRowOf("node-1").syncState());
            assertEquals(1, log.eventsByType(EventType.CATCH_UP_COMPLETE).size());
        }
    }

    // -------------------------------------------------------------------------------------------
    // Test 9: Bounded Event History Capacity
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("9. Lịch sử sự kiện giới hạn dung lượng tối đa (ring buffer), giữ nguyên thứ tự mới nhất")
    void boundedHistory_evictsOldestEventsWhenMaxCapacityReached() {
        MonitorEventLog log = new MonitorEventLog(FIXED_CLOCK, 3);
        for (long i = 1; i <= 5; i++) {
            log.recordSnapshot(List.of(
                    new NodeRow("node-1", "FOLLOWER", 1L, "ONLINE", 10L, i),
                    new NodeRow("node-2", "LEADER", 1L, "ONLINE", 10L, 10L)));
        }
        List<MonitorEvent> retained = log.events();
        assertEquals(3, retained.size());
        assertTrue(retained.get(0).sequence() < retained.get(1).sequence());
        assertTrue(retained.get(1).sequence() < retained.get(2).sequence());
        assertEquals(5L, retained.get(2).lastApplied());
    }

    // -------------------------------------------------------------------------------------------
    // Test 10: FXML Layout & Controller Binding Verification
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("10. Kiểm tra cấu trúc main.fxml & MonitorController: đủ cột lastApplied/commitIndex, syncState và bảng eventTable")
    void fxmlLayoutAndControllerBinding_containsRecoveryColumnsAndEventTable() throws Exception {
        try (InputStream is = MonitorController.class.getResourceAsStream("/fxml/main.fxml")) {
            assertNotNull(is, "main.fxml phải tồn tại trong classpath");
            String fxml = new String(is.readAllBytes(), StandardCharsets.UTF_8);

            List<String> requiredFxIds = List.of(
                    "nodeTable", "nodeColumn", "roleColumn", "termColumn", "statusColumn",
                    "commitIndexColumn", "lastAppliedColumn", "applyProgressColumn", "syncStateColumn",
                    "recoverySummaryLabel", "eventTable",
                    "eventTimeColumn", "eventTypeColumn", "eventNodeColumn",
                    "eventTermColumn", "eventProgressColumn", "eventDetailColumn");

            for (String fxId : requiredFxIds) {
                assertTrue(fxml.contains("fx:id=\"" + fxId + "\""), "main.fxml thiếu fx:id=" + fxId);
                Field field = MonitorController.class.getField(fxId);
                assertNotNull(field, "MonitorController thiếu public field " + fxId);
            }
        }

        // Controller applySnapshot works cleanly without JavaFX toolkit when controls are unattached
        MonitorController controller = new MonitorController();
        List<MonitorEvent> events = controller.applySnapshot(List.of(
                new NodeRow("node-1", "FOLLOWER", 8L, "ONLINE", 43L, 41L),
                new NodeRow("node-3", "LEADER", 8L, "ONLINE", 43L, 43L)));
        assertFalse(events.isEmpty());
        assertEquals(SyncState.CATCHING_UP, controller.eventLog().recoveryRowOf("node-1").syncState());
        assertEquals(SyncState.SYNCED, controller.eventLog().recoveryRowOf("node-3").syncState());
        assertNull(controller.nodeTable);
    }

    private static List<NodeRow> pollAll(MonitorTcpClient client) throws Exception {
        List<CompletableFuture<NodeRow>> futures = MonitorTcpClient.NODE_IDS.stream()
                .map(client::poll)
                .toList();
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(3, TimeUnit.SECONDS);
        return futures.stream().map(CompletableFuture::join).toList();
    }
}
