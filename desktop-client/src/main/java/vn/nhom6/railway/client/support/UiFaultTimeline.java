package vn.nhom6.railway.client.support;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Ghi nhận dòng thời gian (timeline) chuyển đổi trạng thái giao diện người dùng
 * trong các kịch bản sự cố mạng phân tán (TASK TV1-D26).
 * <p>
 * Bắt buộc ghi nhãn nguồn dữ liệu: FAKE / RECORDED-FIXTURE / RECORDED-FAKE / REAL theo AGENTS.md.
 */
public class UiFaultTimeline {

    public record TimelineEntry(
            Instant timestamp,
            String requestId,
            String stateBefore,
            String event,
            String stateAfter,
            String uiMessage,
            String source
    ) {
        public String toCsvRow() {
            return String.format("%s,%s,%s,%s,%s,\"%s\",%s",
                    timestamp.toString(),
                    escape(requestId),
                    escape(stateBefore),
                    escape(event),
                    escape(stateAfter),
                    escapeCsvQuotes(uiMessage),
                    escape(source)
            );
        }

        private static String escape(String s) {
            return s == null ? "" : s.replace(",", ";");
        }

        private static String escapeCsvQuotes(String s) {
            return s == null ? "" : s.replace("\"", "\"\"");
        }
    }

    private final List<TimelineEntry> entries = new CopyOnWriteArrayList<>();

    public void recordTransition(String requestId, String stateBefore, String event, String stateAfter, String uiMessage, String source) {
        entries.add(new TimelineEntry(
                Instant.now(),
                requestId != null ? requestId : "N/A",
                stateBefore != null ? stateBefore : "INITIAL",
                event != null ? event : "UNKNOWN_EVENT",
                stateAfter != null ? stateAfter : "UNKNOWN",
                uiMessage != null ? uiMessage : "",
                source != null ? source : "FAKE"
        ));
    }

    public void recordTransition(String requestId, String stateBefore, String event, String stateAfter, String uiMessage) {
        recordTransition(requestId, stateBefore, event, stateAfter, uiMessage, "FAKE");
    }

    public List<TimelineEntry> getEntries() {
        return Collections.unmodifiableList(new ArrayList<>(entries));
    }

    public void clear() {
        entries.clear();
    }

    public void exportCsv(Path csvPath) throws IOException {
        Path parent = csvPath.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }

        try (BufferedWriter writer = Files.newBufferedWriter(csvPath, StandardCharsets.UTF_8)) {
            writer.write("timestamp,requestId,stateBefore,event,stateAfter,uiMessage,source");
            writer.newLine();
            for (TimelineEntry entry : entries) {
                writer.write(entry.toCsvRow());
                writer.newLine();
            }
        }
    }
}
