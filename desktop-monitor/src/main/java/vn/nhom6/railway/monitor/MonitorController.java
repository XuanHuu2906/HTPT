package vn.nhom6.railway.monitor;

import java.util.function.Function;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;

/** TV6-D13: FXML controller with UI mock fixtures; no TCP/SQL or Raft operations. */
public final class MonitorController {
    // Public FXML injection points avoid adding javafx-fxml to the module POM in D13.
    public TableView<NodeRow> nodeTable;
    public TableColumn<NodeRow, String> nodeColumn;
    public TableColumn<NodeRow, String> roleColumn;
    public TableColumn<NodeRow, String> termColumn;
    public TableColumn<NodeRow, String> statusColumn;
    public TableColumn<NodeRow, String> commitIndexColumn;
    public TableColumn<NodeRow, String> lastAppliedColumn;

    /** Called by FXMLLoader after injection; all changes occur on the FX thread. */
    public void initialize() {
        bind(nodeColumn, NodeRow::nodeId);
        bind(roleColumn, NodeRow::role);
        bind(termColumn, NodeRow::term);
        bind(statusColumn, NodeRow::status);
        bind(commitIndexColumn, NodeRow::commitIndex);
        bind(lastAppliedColumn, NodeRow::lastApplied);
        nodeTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_ALL_COLUMNS);
        nodeTable.getColumns().forEach(column -> {
            column.setSortable(false);
            column.setReorderable(false);
        });
        // Fixed presentation fixtures, not real NODE_STATUS responses (TV3/D15).
        nodeTable.getItems().setAll(
                new NodeRow("node-1", "FOLLOWER", 8L, "ONLINE", 43L, 41L),
                new NodeRow("node-2", "FOLLOWER", 8L, "ONLINE", 43L, 43L),
                new NodeRow("node-3", "LEADER", 8L, "ONLINE", 43L, 43L),
                new NodeRow("node-4", null, null, "STORAGE_ERROR", null, null),
                new NodeRow("node-5", null, null, "OFFLINE", null, null));
    }

    private static void bind(TableColumn<NodeRow, String> column, Function<NodeRow, ?> value) {
        column.setCellValueFactory(cell -> {
            Object result = value.apply(cell.getValue());
            return new ReadOnlyStringWrapper(result == null ? "—" : result.toString());
        });
    }

    /** Six display values only; this is not a shared protocol DTO. */
    public record NodeRow(String nodeId, String role, Long term, String status,
                          Long commitIndex, Long lastApplied) { }
}
