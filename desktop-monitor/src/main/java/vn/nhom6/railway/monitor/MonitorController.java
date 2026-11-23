package vn.nhom6.railway.monitor;

import java.util.List;
import java.util.function.Function;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import vn.nhom6.railway.monitor.MonitorEventLog.MonitorEvent;
import vn.nhom6.railway.monitor.MonitorEventLog.RecoveryRow;

/**
 * TV6-D13 / TV6-D23: FXML controller displaying the 5-node status table, explicit
 * {@code lastApplied} vs {@code commitIndex} sync state, and recovery event history.
 * Observation-only: never mutates Raft state or confirms bookings.
 */
public final class MonitorController {
    // Public FXML injection points keep compatibility with D13/D14 without javafx-fxml dependency.
    public TableView<NodeRow> nodeTable;
    public TableColumn<NodeRow, String> nodeColumn;
    public TableColumn<NodeRow, String> roleColumn;
    public TableColumn<NodeRow, String> termColumn;
    public TableColumn<NodeRow, String> statusColumn;
    public TableColumn<NodeRow, String> commitIndexColumn;
    public TableColumn<NodeRow, String> lastAppliedColumn;
    public TableColumn<NodeRow, String> applyProgressColumn;
    public TableColumn<NodeRow, String> syncStateColumn;

    // TV6-D23 recovery event history controls (optional for backward compatibility with D13 tests)
    public Label recoverySummaryLabel;
    public TableView<MonitorEvent> eventTable;
    public TableColumn<MonitorEvent, String> eventTimeColumn;
    public TableColumn<MonitorEvent, String> eventTypeColumn;
    public TableColumn<MonitorEvent, String> eventNodeColumn;
    public TableColumn<MonitorEvent, String> eventTermColumn;
    public TableColumn<MonitorEvent, String> eventProgressColumn;
    public TableColumn<MonitorEvent, String> eventDetailColumn;

    private final MonitorEventLog eventLog = new MonitorEventLog();

    /** Called by FXMLLoader after injection; all changes occur on the FX thread. */
    public void initialize() {
        bind(nodeColumn, NodeRow::nodeId);
        bind(roleColumn, NodeRow::role);
        bind(termColumn, NodeRow::term);
        bind(statusColumn, NodeRow::status);
        bind(commitIndexColumn, NodeRow::commitIndex);
        bind(lastAppliedColumn, NodeRow::lastApplied);
        if (applyProgressColumn != null) {
            bind(applyProgressColumn, row -> {
                RecoveryRow rec = eventLog.recoveryRowOf(row.nodeId());
                return rec != null ? rec.applyVsCommitDisplay() : "— / —";
            });
        }
        if (syncStateColumn != null) {
            bind(syncStateColumn, row -> {
                RecoveryRow rec = eventLog.recoveryRowOf(row.nodeId());
                return rec != null ? rec.syncState().name() : "UNKNOWN_PROGRESS";
            });
        }
        nodeTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);
        nodeTable.getColumns().forEach(column -> {
            column.setSortable(false);
            column.setReorderable(false);
        });

        if (eventTable != null) {
            bindEvent(eventTimeColumn, e -> e.recordedAt().toString());
            bindEvent(eventTypeColumn, e -> e.type().name());
            bindEvent(eventNodeColumn, MonitorEvent::nodeId);
            bindEvent(eventTermColumn, MonitorEvent::term);
            bindEvent(eventProgressColumn, MonitorEvent::progressText);
            bindEvent(eventDetailColumn, MonitorEvent::summary);
            eventTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);
            eventTable.getColumns().forEach(column -> {
                column.setSortable(false);
                column.setReorderable(false);
            });
        }

        // Fixed presentation fixtures, not real NODE_STATUS responses (TV3/D15).
        applySnapshot(List.of(
                new NodeRow("node-1", "FOLLOWER", 8L, "ONLINE", 43L, 41L),
                new NodeRow("node-2", "FOLLOWER", 8L, "ONLINE", 43L, 43L),
                new NodeRow("node-3", "LEADER", 8L, "ONLINE", 43L, 43L),
                new NodeRow("node-4", null, null, "STORAGE_ERROR", null, null),
                new NodeRow("node-5", null, null, "OFFLINE", null, null)));
    }

    /**
     * Applies one observed 5-node snapshot to the event log and JavaFX controls.
     * Must be invoked on the JavaFX Application Thread when UI controls are attached.
     */
    public List<MonitorEvent> applySnapshot(List<NodeRow> rows) {
        List<MonitorEvent> emitted = eventLog.recordSnapshot(rows);
        if (nodeTable != null) {
            nodeTable.getItems().setAll(rows);
        }
        if (eventTable != null) {
            eventTable.getItems().setAll(eventLog.events());
        }
        if (recoverySummaryLabel != null) {
            recoverySummaryLabel.setText(eventLog.summaryBanner());
        }
        return emitted;
    }

    /** Returns the controller's recovery event log for inspection or fixture replay. */
    public MonitorEventLog eventLog() {
        return eventLog;
    }

    private static void bind(TableColumn<NodeRow, String> column, Function<NodeRow, ?> value) {
        if (column == null) {
            return;
        }
        column.setCellValueFactory(cell -> {
            Object result = value.apply(cell.getValue());
            return new ReadOnlyStringWrapper(result == null ? "—" : result.toString());
        });
    }

    private static void bindEvent(TableColumn<MonitorEvent, String> column, Function<MonitorEvent, ?> value) {
        if (column == null) {
            return;
        }
        column.setCellValueFactory(cell -> {
            Object result = value.apply(cell.getValue());
            return new ReadOnlyStringWrapper(result == null ? "—" : result.toString());
        });
    }

    /** Six display values only; this is not a shared protocol DTO. */
    public record NodeRow(String nodeId, String role, Long term, String status,
                          Long commitIndex, Long lastApplied) { }
}
