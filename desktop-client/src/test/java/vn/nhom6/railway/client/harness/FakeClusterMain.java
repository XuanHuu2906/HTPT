package vn.nhom6.railway.client.harness;

import vn.nhom6.railway.client.smoke.FakeNodeServer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Tiến trình chạy {@link FakeNodeServer} độc lập cho kịch bản 6 Client cùng ghế (TASK TV1-D18).
 * <p>
 * <b>NHÃN BẮT BUỘC:</b> FAKE, không phải Node thật, không phải Raft.
 * <ul>
 *   <li>Chỉ dùng để kiểm thử luồng giao thức SPEC-03 (4-byte BE + UTF-8 JSON qua {@code FrameCodec}/{@code MessageCodec})
 *       và giao diện phía Client.</li>
 *   <li>KHÔNG chứng minh Raft consensus hay AC03 thật (AC03 thật nghiệm thu tại G5 sau TV4-D18).</li>
 *   <li>Luật giả lập: đến trước thắng cho cùng 1 ghế; requestId khác cùng ghế -> REJECTED(SEAT_ALREADY_BOOKED);
 *       cùng requestId + cùng payload -> trả lại đúng kết quả cũ; khác payload -> REQUEST_ID_CONFLICT.</li>
 *   <li>Log kiểm toán KHÔNG bao giờ ghi họ tên hành khách.</li>
 * </ul>
 */
public final class FakeClusterMain {

    private static final String FAKE_BANNER = "FAKE, không phải Node thật, không phải Raft";

    private FakeClusterMain() {
    }

    public static void main(String[] args) throws Exception {
        int port = 0;
        Path portFile = Paths.get("fake-cluster.port");
        String nodeId = FakeNodeServer.DEFAULT_NODE_ID;
        long seed = 42L;
        int maxJitterMs = 35;
        String dropResponseFor = null;
        Path logFile = null;
        Path summaryFile = null;
        Path stopFile = null;
        String targetSeat = "G02";

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("--port".equals(arg) && i + 1 < args.length) {
                port = Integer.parseInt(args[++i].trim());
            } else if ("--port-file".equals(arg) && i + 1 < args.length) {
                portFile = Paths.get(args[++i].trim());
            } else if ("--node-id".equals(arg) && i + 1 < args.length) {
                nodeId = args[++i].trim();
            } else if ("--seed".equals(arg) && i + 1 < args.length) {
                seed = Long.parseLong(args[++i].trim());
            } else if ("--max-jitter-ms".equals(arg) && i + 1 < args.length) {
                maxJitterMs = Integer.parseInt(args[++i].trim());
            } else if ("--drop-response-for".equals(arg) && i + 1 < args.length) {
                dropResponseFor = args[++i].trim();
            } else if ("--log-file".equals(arg) && i + 1 < args.length) {
                logFile = Paths.get(args[++i].trim());
            } else if ("--summary-file".equals(arg) && i + 1 < args.length) {
                summaryFile = Paths.get(args[++i].trim());
            } else if ("--stop-file".equals(arg) && i + 1 < args.length) {
                stopFile = Paths.get(args[++i].trim());
            } else if ("--seat".equals(arg) && i + 1 < args.length) {
                targetSeat = args[++i].trim().toUpperCase();
            }
        }

        final Path finalLogFile = logFile;
        final Path finalSummaryFile = summaryFile;
        final String finalTargetSeat = targetSeat;

        if (finalLogFile != null && finalLogFile.getParent() != null) {
            Files.createDirectories(finalLogFile.getParent());
        }
        if (finalSummaryFile != null && finalSummaryFile.getParent() != null) {
            Files.createDirectories(finalSummaryFile.getParent());
        }

        FakeNodeServer server = new FakeNodeServer(nodeId, port);
        server.configureRandomDelay(seed, maxJitterMs);

        if (dropResponseFor != null && !dropResponseFor.isBlank()) {
            String normalized = dropResponseFor.trim();
            if (normalized.matches("\\d+")) {
                server.addDropResponseForSender("client-" + normalized);
            } else {
                server.addDropResponseForSender(normalized);
            }
        }

        Object logLock = new Object();
        server.setAuditLogListener(line -> {
            synchronized (logLock) {
                System.out.println(line);
                appendLine(finalLogFile, line);
                writeSummary(finalSummaryFile, server, finalTargetSeat);
            }
        });

        int actualPort = server.getPort();
        if (portFile.getParent() != null) {
            Files.createDirectories(portFile.getParent());
        }
        Path tmpPortFile = portFile.resolveSibling(portFile.getFileName() + ".tmp");
        Files.writeString(tmpPortFile, String.valueOf(actualPort), StandardCharsets.UTF_8);
        Files.move(tmpPortFile, portFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);

        String startupBanner = String.format(
                "[FAKE-CLUSTER-MAIN] Khởi động (%s) | nodeId=%s | port=%d | seed=%d | maxJitterMs=%d | dropResponseFor=%s | startedAt=%s",
                FAKE_BANNER,
                nodeId,
                actualPort,
                seed,
                maxJitterMs,
                (dropResponseFor != null && !dropResponseFor.isBlank()) ? dropResponseFor : "NONE",
                Instant.now()
        );
        System.out.println(startupBanner);
        appendLine(finalLogFile, startupBanner);
        writeSummary(finalSummaryFile, server, finalTargetSeat);

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            writeSummary(finalSummaryFile, server, finalTargetSeat);
            String footer = String.format(
                    "[FAKE-CLUSTER-MAIN] Dừng (%s) | seat=%s | activeTickets=%d | totalBookingArrivals=%d",
                    FAKE_BANNER,
                    finalTargetSeat,
                    server.getActiveTicketCount(finalTargetSeat),
                    server.getBookingAuditLog().size()
            );
            System.out.println(footer);
            appendLine(finalLogFile, footer);
            server.close();
            shutdownLatch.countDown();
        }, "fake-cluster-shutdown"));

        if (stopFile != null) {
            final Path watchStop = stopFile;
            Thread watcher = new Thread(() -> {
                while (shutdownLatch.getCount() > 0) {
                    if (Files.exists(watchStop)) {
                        shutdownLatch.countDown();
                        break;
                    }
                    try {
                        Thread.sleep(150L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }, "fake-cluster-stop-watcher");
            watcher.setDaemon(true);
            watcher.start();
        }

        shutdownLatch.await();
        writeSummary(finalSummaryFile, server, finalTargetSeat);
        server.close();
    }

    private static void appendLine(Path file, String line) {
        if (file == null) {
            return;
        }
        try (BufferedWriter writer = Files.newBufferedWriter(
                file,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND)) {
            writer.write(line);
            writer.newLine();
        } catch (IOException ignored) {
        }
    }

    private static void writeSummary(Path summaryFile, FakeNodeServer server, String targetSeat) {
        if (summaryFile == null) {
            return;
        }
        List<FakeNodeServer.BookingAuditEntry> entries = server.getBookingAuditLog();
        long confirmed = entries.stream().filter(e -> "CONFIRMED".equals(e.outcome())).count();
        long rejected = entries.stream().filter(e -> "REJECTED".equals(e.outcome())).count();
        long dropped = entries.stream().filter(FakeNodeServer.BookingAuditEntry::responseDropped).count();
        int activeSeatTickets = server.getActiveTicketCount(targetSeat);

        StringBuilder sb = new StringBuilder();
        sb.append("source=FAKE_SERVER\n");
        sb.append("warning=FAKE, khong phai Node that, khong phai Raft\n");
        sb.append("nodeId=").append(server.getNodeId()).append("\n");
        sb.append("port=").append(server.getPort()).append("\n");
        sb.append("targetSeat=").append(targetSeat).append("\n");
        sb.append("activeTicketsTargetSeat=").append(activeSeatTickets).append("\n");
        sb.append("totalArrivals=").append(entries.size()).append("\n");
        sb.append("confirmedCount=").append(confirmed).append("\n");
        sb.append("rejectedCount=").append(rejected).append("\n");
        sb.append("droppedResponseCount=").append(dropped).append("\n");

        try {
            Files.writeString(summaryFile, sb.toString(), StandardCharsets.UTF_8);
        } catch (IOException ignored) {
        }
    }
}
