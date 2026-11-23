package vn.nhom6.railway.monitor;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import vn.nhom6.railway.monitor.MonitorController.NodeRow;
import vn.nhom6.railway.protocol.dto.NodeStatusDto;

/**
 * TV6-D23: Cluster Monitor recovery event history and catch-up tracker.
 *
 * <p>Consumes observed {@link NodeRow} or SPEC-03 {@link NodeStatusDto} snapshots (from
 * {@link StatusPoller}, TV3-D15 {@code NODE_STATUS}, or recorded/fake replay fixtures) and
 * maintains:
 * <ul>
 *   <li>Chronological event history for {@code LEADER_CHANGE}, {@code NODE_OFFLINE},
 *       {@code NODE_ONLINE}, {@code CATCH_UP_PROGRESS}, and {@code CATCH_UP_COMPLETE}.</li>
 *   <li>Explicit {@code lastApplied} vs {@code commitIndex} comparison per node (and against the
 *       current healthy Leader's {@code commitIndex} reference), never merging the two concepts
 *       (SPEC-06 §7.1).</li>
 *   <li>Strict sync labeling invariant: a node is <strong>never</strong> labeled {@link SyncState#SYNCED}
 *       while offline, unhealthy, lagging behind its local {@code commitIndex}, lagging behind the
 *       Leader's {@code commitIndex}, on a stale term, or while no unique healthy Leader reference
 *       is known.</li>
 * </ul>
 *
 * <p>Monitor is strictly non-voting and observation-only; it never mutates Raft state or confirms
 * bookings. Real 5-node leader rejoin visualization is verified end-to-end at Gate G6.
 */
public final class MonitorEventLog {

    public static final int DEFAULT_MAX_EVENTS = 500;
    public static final String STATUS_ONLINE = "ONLINE";

    /** Cluster recovery &amp; availability event categories displayed in the Monitor event log. */
    public enum EventType {
        LEADER_CHANGE,
        NODE_OFFLINE,
        NODE_ONLINE,
        CATCH_UP_PROGRESS,
        CATCH_UP_COMPLETE
    }

    /**
     * Truthful synchronization state for one node.
     * {@link #SYNCED} is granted strictly after full catch-up against both local and Leader commit indices.
     */
    public enum SyncState {
        OFFLINE,
        CATCHING_UP,
        SYNCED,
        UNKNOWN_PROGRESS
    }

    /** Immutable event entry recorded by the Monitor. */
    public record MonitorEvent(
            long sequence,
            Instant recordedAt,
            EventType type,
            String nodeId,
            Long term,
            Long commitIndex,
            Long lastApplied,
            SyncState syncState,
            String summary) {

        public MonitorEvent {
            Objects.requireNonNull(recordedAt, "recordedAt");
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(nodeId, "nodeId");
            Objects.requireNonNull(syncState, "syncState");
            Objects.requireNonNull(summary, "summary");
        }

        /** Compact display of {@code lastApplied / commitIndex} for UI columns. */
        public String progressText() {
            String applied = lastApplied == null ? "—" : lastApplied.toString();
            String committed = commitIndex == null ? "—" : commitIndex.toString();
            return applied + " / " + committed;
        }
    }

    /**
     * Enriched per-node recovery observation pairing raw {@link NodeRow} fields with explicit
     * {@code lastApplied} vs {@code commitIndex} progress and {@link SyncState}.
     */
    public record RecoveryRow(
            String nodeId,
            String role,
            Long term,
            String status,
            Long commitIndex,
            Long lastApplied,
            Long referenceCommitIndex,
            Long lag,
            SyncState syncState,
            String applyVsCommitDisplay) {

        public boolean isSynced() {
            return syncState == SyncState.SYNCED;
        }
    }

    private final Clock clock;
    private final int maxEvents;
    private final Deque<MonitorEvent> history = new ArrayDeque<>();
    private final Map<String, NodeRow> previousRows = new LinkedHashMap<>();
    private final Map<String, SyncState> previousSyncStates = new LinkedHashMap<>();
    private final Map<String, RecoveryRow> latestRecoveryRows = new LinkedHashMap<>();

    private long nextSequence = 1L;
    private String lastLeaderKey;
    private Long lastLeaderTerm;
    private boolean hasRecordedSnapshot;

    public MonitorEventLog() {
        this(Clock.systemUTC(), DEFAULT_MAX_EVENTS);
    }

    public MonitorEventLog(Clock clock, int maxEvents) {
        this.clock = Objects.requireNonNull(clock, "clock");
        if (maxEvents <= 0) {
            throw new IllegalArgumentException("maxEvents must be positive: " + maxEvents);
        }
        this.maxEvents = maxEvents;
    }

    /**
     * Records a snapshot supplied directly as SPEC-03 {@link NodeStatusDto} payloads (TV3-D15).
     */
    public synchronized List<MonitorEvent> recordStatusDtos(List<NodeStatusDto> statuses) {
        Objects.requireNonNull(statuses, "statuses");
        List<NodeRow> rows = new ArrayList<>(statuses.size());
        for (NodeStatusDto dto : statuses) {
            rows.add(MonitorTcpClient.fromStatusDto(dto));
        }
        return recordSnapshot(rows);
    }

    /**
     * Replays a chronological sequence of recorded or fake snapshots and returns all emitted events.
     */
    public synchronized List<MonitorEvent> replay(List<List<NodeRow>> snapshots) {
        Objects.requireNonNull(snapshots, "snapshots");
        List<MonitorEvent> emitted = new ArrayList<>();
        for (List<NodeRow> snapshot : snapshots) {
            emitted.addAll(recordSnapshot(snapshot));
        }
        return Collections.unmodifiableList(emitted);
    }

    /**
     * Processes one observed snapshot across the cluster, updates per-node {@link RecoveryRow}
     * states, and appends any detected transition events.
     *
     * @return events emitted by this snapshot alone, in chronological order
     */
    public synchronized List<MonitorEvent> recordSnapshot(List<NodeRow> rows) {
        Objects.requireNonNull(rows, "rows");
        Instant now = clock.instant().truncatedTo(ChronoUnit.MILLIS);
        List<MonitorEvent> emitted = new ArrayList<>();

        // 1. Determine current unique healthy Leader reference (term + commitIndex)
        List<NodeRow> onlineLeaders = rows.stream()
                .filter(MonitorEventLog::isOnline)
                .filter(r -> "LEADER".equals(r.role()) && r.term() != null)
                .toList();

        NodeRow uniqueLeader = onlineLeaders.size() == 1 ? onlineLeaders.get(0) : null;
        Long referenceTerm = uniqueLeader != null ? uniqueLeader.term() : null;
        Long referenceCommit = (uniqueLeader != null && isValidIndices(uniqueLeader))
                ? uniqueLeader.commitIndex()
                : null;

        // 2. Detect leader changes (elected, switched, lost, or multiple-leader warning)
        String currentLeaderKey = leaderKeyOf(onlineLeaders);
        if (!hasRecordedSnapshot) {
            if (uniqueLeader != null) {
                emitted.add(appendEvent(now, EventType.LEADER_CHANGE, uniqueLeader.nodeId(),
                        uniqueLeader.term(), uniqueLeader.commitIndex(), uniqueLeader.lastApplied(),
                        evaluateSyncState(uniqueLeader, referenceTerm, referenceCommit),
                        "Leader hiện tại: " + uniqueLeader.nodeId() + " (term " + uniqueLeader.term() + ")"));
            } else if (onlineLeaders.size() > 1) {
                String names = String.join(", ", onlineLeaders.stream()
                        .map(l -> l.nodeId() + "@t" + l.term()).toList());
                emitted.add(appendEvent(now, EventType.LEADER_CHANGE, "cluster",
                        null, null, null, SyncState.UNKNOWN_PROGRESS,
                        "Cảnh báo phát hiện nhiều Leader đồng thời: " + names));
            }
        } else if (!Objects.equals(lastLeaderKey, currentLeaderKey)
                || (uniqueLeader != null && !Objects.equals(lastLeaderTerm, uniqueLeader.term()))) {
            if (uniqueLeader != null) {
                String prevDesc = (lastLeaderKey == null || lastLeaderKey.isEmpty()) ? "NONE" : lastLeaderKey;
                emitted.add(appendEvent(now, EventType.LEADER_CHANGE, uniqueLeader.nodeId(),
                        uniqueLeader.term(), uniqueLeader.commitIndex(), uniqueLeader.lastApplied(),
                        evaluateSyncState(uniqueLeader, referenceTerm, referenceCommit),
                        "Đổi Leader: " + prevDesc + " -> " + uniqueLeader.nodeId()
                                + " (term " + uniqueLeader.term() + ")"));
            } else if (onlineLeaders.isEmpty()) {
                emitted.add(appendEvent(now, EventType.LEADER_CHANGE, "cluster",
                        lastLeaderTerm, null, null, SyncState.UNKNOWN_PROGRESS,
                        "Mất Leader: " + lastLeaderKey + " -> NONE (đang bầu cử hoặc mất quorum)"));
            } else {
                String names = String.join(", ", onlineLeaders.stream()
                        .map(l -> l.nodeId() + "@t" + l.term()).toList());
                emitted.add(appendEvent(now, EventType.LEADER_CHANGE, "cluster",
                        null, null, null, SyncState.UNKNOWN_PROGRESS,
                        "Cảnh báo nhiều Leader: " + names));
            }
        }
        lastLeaderKey = currentLeaderKey;
        lastLeaderTerm = referenceTerm;

        // 3. Evaluate per-node online/offline transitions and catch-up progress
        latestRecoveryRows.clear();
        for (NodeRow row : rows) {
            Objects.requireNonNull(row, "row");
            NodeRow prev = previousRows.get(row.nodeId());
            SyncState prevSync = previousSyncStates.get(row.nodeId());

            SyncState syncState = evaluateSyncState(row, referenceTerm, referenceCommit);
            Long lag = computeLag(row, referenceCommit);
            String progressDisplay = formatApplyVsCommit(row, referenceCommit, syncState, lag);

            RecoveryRow recoveryRow = new RecoveryRow(
                    row.nodeId(),
                    row.role(),
                    row.term(),
                    row.status(),
                    row.commitIndex(),
                    row.lastApplied(),
                    referenceCommit,
                    lag,
                    syncState,
                    progressDisplay);
            latestRecoveryRows.put(row.nodeId(), recoveryRow);

            boolean wasOnline = prev != null && isOnline(prev);
            boolean nowOnline = isOnline(row);

            if (prev != null && wasOnline && !nowOnline) {
                emitted.add(appendEvent(now, EventType.NODE_OFFLINE, row.nodeId(),
                        row.term(), row.commitIndex(), row.lastApplied(), syncState,
                        row.nodeId() + " chuyển " + row.status()
                                + " (trước đó: role=" + prev.role() + ", term=" + prev.term()
                                + ", lastApplied=" + prev.lastApplied() + ")"));
            } else if (prev != null && !wasOnline && nowOnline) {
                String syncNote = syncState == SyncState.SYNCED
                        ? "đã khớp commitIndex"
                        : "chưa bắt kịp (" + progressDisplay + ") — chưa gắn nhãn SYNCED";
                emitted.add(appendEvent(now, EventType.NODE_ONLINE, row.nodeId(),
                        row.term(), row.commitIndex(), row.lastApplied(), syncState,
                        row.nodeId() + " quay lại ONLINE (role=" + row.role() + ", term=" + row.term()
                                + ", " + syncNote + ")"));
            }

            // Catch-up progress & completion tracking
            if (nowOnline) {
                if (syncState == SyncState.CATCHING_UP) {
                    boolean newlyLagging = prevSync != SyncState.CATCHING_UP;
                    boolean appliedAdvanced = prev != null && prev.lastApplied() != null && row.lastApplied() != null
                            && row.lastApplied() > prev.lastApplied();
                    boolean commitAdvancedWhileLagging = prev != null && prev.commitIndex() != null
                            && row.commitIndex() != null && row.commitIndex() > prev.commitIndex();

                    if (newlyLagging || appliedAdvanced || commitAdvancedWhileLagging) {
                        emitted.add(appendEvent(now, EventType.CATCH_UP_PROGRESS, row.nodeId(),
                                row.term(), row.commitIndex(), row.lastApplied(), syncState,
                                row.nodeId() + " đang catch-up: " + progressDisplay + " (chưa SYNCED)"));
                    }
                } else if (syncState == SyncState.SYNCED
                        && (prevSync == SyncState.CATCHING_UP
                        || (prev != null && !wasOnline))) {
                    emitted.add(appendEvent(now, EventType.CATCH_UP_COMPLETE, row.nodeId(),
                            row.term(), row.commitIndex(), row.lastApplied(), syncState,
                            row.nodeId() + " hoàn tất catch-up -> SYNCED (" + progressDisplay + ")"));
                }
            }

            previousRows.put(row.nodeId(), row);
            previousSyncStates.put(row.nodeId(), syncState);
        }

        hasRecordedSnapshot = true;
        return Collections.unmodifiableList(emitted);
    }

    /**
     * Evaluates the truthful {@link SyncState} of a node against its own {@code commitIndex}
     * and the unique healthy Leader's {@code (referenceTerm, referenceCommit)}.
     */
    public static SyncState evaluateSyncState(NodeRow row, Long referenceTerm, Long referenceCommit) {
        Objects.requireNonNull(row, "row");
        if (!isOnline(row)) {
            return SyncState.OFFLINE;
        }
        if (row.commitIndex() == null || row.lastApplied() == null || row.term() == null) {
            return SyncState.UNKNOWN_PROGRESS;
        }
        if (row.lastApplied() > row.commitIndex()) {
            // Invariant violation (lastApplied > commitIndex): never label SYNCED
            return SyncState.UNKNOWN_PROGRESS;
        }
        if (row.lastApplied() < row.commitIndex()) {
            // Local apply still lagging behind local commitIndex
            return SyncState.CATCHING_UP;
        }
        if (referenceTerm == null || referenceCommit == null) {
            // No unique healthy Leader reference: cannot certify SYNCED
            return SyncState.UNKNOWN_PROGRESS;
        }
        if (row.term() < referenceTerm) {
            // Stale term (e.g., old leader before step-down/rejoin)
            return SyncState.CATCHING_UP;
        }
        if (row.commitIndex() < referenceCommit || row.lastApplied() < referenceCommit) {
            // Behind Leader's committed index
            return SyncState.CATCHING_UP;
        }
        if (row.term() > referenceTerm) {
            return SyncState.UNKNOWN_PROGRESS;
        }
        return SyncState.SYNCED;
    }

    /** Computes non-negative lag behind {@code max(local commitIndex, referenceCommitIndex)}. */
    public static Long computeLag(NodeRow row, Long referenceCommit) {
        if (!isOnline(row) || row.commitIndex() == null || row.lastApplied() == null) {
            return null;
        }
        long target = row.commitIndex();
        if (referenceCommit != null && referenceCommit > target) {
            target = referenceCommit;
        }
        long diff = target - row.lastApplied();
        return Math.max(0L, diff);
    }

    /** Formats {@code lastApplied} vs {@code commitIndex} (and Leader reference if lagging). */
    public static String formatApplyVsCommit(
            NodeRow row, Long referenceCommit, SyncState syncState, Long lag) {
        if (!isOnline(row) || row.commitIndex() == null || row.lastApplied() == null) {
            return "— / —";
        }
        String base = row.lastApplied() + " / " + row.commitIndex();
        if (syncState == SyncState.SYNCED) {
            return base + " (SYNCED)";
        }
        if (syncState == SyncState.CATCHING_UP) {
            if (referenceCommit != null && referenceCommit > row.commitIndex()) {
                return base + " (lag -" + lag + " vs leader " + referenceCommit + ")";
            }
            if (lag != null && lag > 0L) {
                return base + " (lag -" + lag + ")";
            }
            return base + " (stale term)";
        }
        return base + " (chưa xác nhận)";
    }

    public synchronized List<MonitorEvent> events() {
        return List.copyOf(history);
    }

    public synchronized List<MonitorEvent> eventsByType(EventType type) {
        Objects.requireNonNull(type, "type");
        return history.stream().filter(e -> e.type() == type).toList();
    }

    public synchronized List<MonitorEvent> eventsForNode(String nodeId) {
        Objects.requireNonNull(nodeId, "nodeId");
        return history.stream().filter(e -> nodeId.equals(e.nodeId())).toList();
    }

    public synchronized List<RecoveryRow> currentRecoveryRows() {
        return List.copyOf(latestRecoveryRows.values());
    }

    public synchronized RecoveryRow recoveryRowOf(String nodeId) {
        return latestRecoveryRows.get(nodeId);
    }

    public synchronized String summaryBanner() {
        if (latestRecoveryRows.isEmpty()) {
            return "Chưa có dữ liệu quan sát";
        }
        long online = latestRecoveryRows.values().stream()
                .filter(r -> STATUS_ONLINE.equals(r.status())).count();
        long synced = latestRecoveryRows.values().stream()
                .filter(RecoveryRow::isSynced).count();
        long catchingUp = latestRecoveryRows.values().stream()
                .filter(r -> r.syncState() == SyncState.CATCHING_UP).count();
        String leader = (lastLeaderKey == null || lastLeaderKey.isEmpty()) ? "NONE" : lastLeaderKey;
        return "Leader: " + leader
                + " | Online: " + online + "/" + latestRecoveryRows.size()
                + " | Synced: " + synced
                + " | Catching-up: " + catchingUp
                + " | Events: " + history.size();
    }

    private MonitorEvent appendEvent(
            Instant at,
            EventType type,
            String nodeId,
            Long term,
            Long commitIndex,
            Long lastApplied,
            SyncState syncState,
            String summary) {
        MonitorEvent event = new MonitorEvent(
                nextSequence++, at, type, nodeId, term, commitIndex, lastApplied, syncState, summary);
        if (history.size() >= maxEvents) {
            history.removeFirst();
        }
        history.addLast(event);
        return event;
    }

    private static boolean isOnline(NodeRow row) {
        return STATUS_ONLINE.equals(row.status());
    }

    private static boolean isValidIndices(NodeRow row) {
        return row.commitIndex() != null
                && row.lastApplied() != null
                && row.commitIndex() >= 0L
                && row.lastApplied() >= 0L
                && row.lastApplied() <= row.commitIndex();
    }

    private static String leaderKeyOf(List<NodeRow> onlineLeaders) {
        if (onlineLeaders.isEmpty()) {
            return "";
        }
        if (onlineLeaders.size() == 1) {
            return onlineLeaders.get(0).nodeId();
        }
        return String.join(",", onlineLeaders.stream().map(NodeRow::nodeId).sorted().toList());
    }
}
