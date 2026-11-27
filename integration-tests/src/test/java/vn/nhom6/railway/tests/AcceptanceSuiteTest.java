package vn.nhom6.railway.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import vn.nhom6.railway.networking.ElectionRuntime;
import vn.nhom6.railway.networking.NodeConfig;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TV6-D24 inventory/preparation only, never an AC01-AC10 or physical LAN gate.
 * Default tests validate D23 templates and reporting with no Docker/network probes.
 * -Dlan.inventory.local=true performs read-only Windows/Docker inventory of THIS host.
 * Optional -Dlan.laptop=1..6 assigns this host only after operator confirmation.
 * Reports go to stdout/Surefire; no shared config, firewall, image or volume mutation.
 */
public class AcceptanceSuiteTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<String> VOTERS = Set.of("node-1", "node-2", "node-3", "node-4", "node-5");
    private static final Duration COMMAND_DEADLINE = Duration.ofSeconds(20);

    // Each subsection fails independently: access denied must not hide IP/hostname.
    private static final String WINDOWS_INVENTORY = """
            $ErrorActionPreference='Stop'
            [Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
            function Observe([scriptblock]$Read) {
                try { @{status='PASS'; data=@(& $Read)} }
                catch { @{status='BLOCKED'; reason=$_.Exception.Message} }
            }
            $physical=@(Get-NetAdapter -Physical | Where-Object Status -eq 'Up' | ForEach-Object ifIndex)
            $result=[ordered]@{
                hostname=[Environment]::MachineName
                capturedAt=[DateTime]::UtcNow.ToString('o')
                network=Observe {
                    Get-NetIPConfiguration | ForEach-Object {
                        $cfg=$_
                        foreach ($ip in @($cfg.IPv4Address)) {
                            if ($null -ne $ip.IPAddress) {
                                [pscustomobject]@{
                                    interface=$cfg.InterfaceAlias
                                    ipv4=$ip.IPAddress
                                    prefixLength=$ip.PrefixLength
                                    gateway=(@($cfg.IPv4DefaultGateway.NextHop) -join ',')
                                    physicalUp=($cfg.InterfaceIndex -in $physical)
                                }
                            }
                        }
                    }
                }
                connectionProfiles=Observe {
                    Get-NetConnectionProfile | Select-Object InterfaceAlias,
                        @{n='NetworkCategory';e={$_.NetworkCategory.ToString()}},
                        @{n='IPv4Connectivity';e={$_.IPv4Connectivity.ToString()}}
                }
                firewallProfiles=Observe {
                    Get-NetFirewallProfile -PolicyStore ActiveStore | Select-Object Name,
                        @{n='Enabled';e={$_.Enabled.ToString()}},
                        @{n='DefaultInboundAction';e={$_.DefaultInboundAction.ToString()}},
                        @{n='DefaultOutboundAction';e={$_.DefaultOutboundAction.ToString()}}
                }
                firewallPortRules=Observe {
                    Get-NetFirewallPortFilter -PolicyStore ActiveStore |
                        Where-Object { $_.Protocol -in @('TCP','6') -and
                            (@($_.LocalPort) -contains '9100' -or @($_.LocalPort) -contains '9200') } |
                        Get-NetFirewallRule | Select-Object DisplayName,
                            @{n='Enabled';e={$_.Enabled.ToString()}},
                            @{n='Direction';e={$_.Direction.ToString()}},
                            @{n='Action';e={$_.Action.ToString()}},
                            @{n='Profile';e={$_.Profile.ToString()}}
                }
            }
            $result | ConvertTo-Json -Depth 8 -Compress
            """;

    record CommandResult(List<String> command, int exitCode, boolean timedOut, String output) {
        String status() { return !timedOut && exitCode == 0 ? "PASS" : "BLOCKED"; }
        ObjectNode json() {
            ObjectNode node = JSON.createObjectNode();
            node.set("command", JSON.valueToTree(command));
            node.put("status", status()).put("exitCode", exitCode).put("timedOut", timedOut).put("output", output);
            return node;
        }
    }

    static CommandResult execute(List<String> command, Duration timeout, Path temporaryDirectory)
            throws IOException, InterruptedException {
        Path output = Files.createTempFile(temporaryDirectory, "inventory-", ".txt");
        Process process;
        try {
            process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile()).start();
        } catch (IOException unavailable) {
            return new CommandResult(List.copyOf(command), -1, false, unavailable.toString());
        }
        boolean completed = false;
        try {
            completed = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            if (process.isAlive()) {
                process.descendants().forEach(ProcessHandle::destroyForcibly);
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }
        // PowerShell explicitly emits UTF-8; Docker's selected output contains no credentials.
        return new CommandResult(List.copyOf(command), completed ? process.exitValue() : -1,
                !completed, Files.readString(output, StandardCharsets.UTF_8));
    }

    static Path repository() {
        Path path = Path.of("").toAbsolutePath();
        while (path != null && !Files.isRegularFile(path.resolve("AGENTS.md"))) { path = path.getParent(); }
        if (path == null) { throw new IllegalStateException("Cannot find repository root"); }
        return path;
    }

    static ArrayNode slots() {
        ArrayNode result = JSON.createArrayNode();
        for (int i = 1; i <= 6; i++) {
            ObjectNode slot = result.addObject();
            slot.put("laptop", i).put("raftVoter", i <= 5).put("status", "NOT_RUN");
            slot.put("nodeId", i <= 5 ? "node-" + i : "NONE");
            slot.put("hostname", "NOT_RUN").put("ipv4", "NOT_RUN").put("mask", "NOT_RUN");
            slot.put("firewall", "NOT_RUN").put("docker", "NOT_RUN");
            slot.put("reason", "No measurement from this physical laptop; template addresses are not observations.");
        }
        return result;
    }

    static String mask(int prefix) {
        if (prefix < 0 || prefix > 32) { throw new IllegalArgumentException("IPv4 prefix out of range"); }
        long bits = prefix == 0 ? 0 : (0xffffffffL << (32 - prefix)) & 0xffffffffL;
        return String.format("%d.%d.%d.%d", bits >>> 24, (bits >>> 16) & 255, (bits >>> 8) & 255, bits & 255);
    }

    static List<JsonNode> candidates(JsonNode observation) {
        List<JsonNode> result = new ArrayList<>();
        for (JsonNode nic : observation.path("network").path("data")) {
            String ip = nic.path("ipv4").asText();
            if (nic.path("physicalUp").asBoolean() && !nic.path("gateway").asText().isBlank()
                    && !ip.startsWith("169.254.") && !ip.startsWith("127.")) { result.add(nic); }
        }
        return result;
    }

    static int laptopNumber(String value) {
        if (value.isBlank()) { return 0; }
        if (!value.matches("[1-6]")) { throw new IllegalArgumentException("lan.laptop must be 1..6, or absent (UNASSIGNED)"); }
        return Integer.parseInt(value);
    }

    @Test
    void d23TemplatesHaveOneConsistentFiveVoterDirectoryAndSeparateDatabases() throws Exception {
        String directory = null;
        Set<String> databases = new HashSet<>();
        for (int i = 1; i <= 5; i++) {
            Path file = repository().resolve("configs/lan/node-" + i + ".properties");
            NodeConfig config = NodeConfig.load(file.toString());
            assertEquals("node-" + i, config.nodeId());
            assertEquals(9100, config.clientPort());
            assertEquals(9200, config.raftPort());
            assertEquals("railway_node" + i, config.dbConfig().dbName());
            assertTrue(databases.add(config.dbConfig().dbName()));
            Properties properties = new Properties();
            try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) { properties.load(reader); }
            String peers = properties.getProperty("node.peers");
            if (directory == null) { directory = peers; }
            assertEquals(directory, peers, "All nodes need the same membership/address map");
            Map<String, InetSocketAddress> parsed = ElectionRuntime.parsePeers(peers);
            assertEquals(VOTERS, parsed.keySet());
            for (InetSocketAddress peer : parsed.values()) { assertEquals(9200, peer.getPort()); }
            String ownHost = parsed.get(config.nodeId()).getHostString();
            assertEquals(ownHost + ":9100", properties.getProperty("node.advertise.client"));
            assertEquals(ownHost + ":9200", properties.getProperty("node.advertise.peer"));
        }
        System.out.println("TV6-D24 CONFIG_PREFLIGHT=PASS; template syntax only; PHYSICAL_LAN=NOT_RUN");
    }

    @Test
    void sixUnmeasuredSlotsNeverBecomeMeasuredOrSixVoters() {
        ArrayNode pending = slots();
        assertEquals(6, pending.size());
        int voters = 0;
        for (JsonNode slot : pending) {
            if (slot.path("raftVoter").asBoolean()) { voters++; }
            assertEquals("NOT_RUN", slot.path("status").asText());
            assertEquals("NOT_RUN", slot.path("ipv4").asText());
        }
        assertEquals(5, voters);
        assertFalse(pending.get(5).path("raftVoter").asBoolean());
        assertEquals("NONE", pending.get(5).path("nodeId").asText());
    }

    @Test
    void networkChoiceExcludesVirtualAndDisconnectedInterfacesAndDoesNotGuess() throws Exception {
        JsonNode sample = JSON.readTree("""
                {"network":{"data":[
                {"ipv4":"192.168.10.1","physicalUp":false,"gateway":""},
                {"ipv4":"169.254.1.2","physicalUp":true,"gateway":"192.168.1.1"},
                {"ipv4":"192.168.1.50","physicalUp":true,"gateway":"192.168.1.1","prefixLength":24},
                {"ipv4":"10.0.0.2","physicalUp":true,"gateway":"10.0.0.1","prefixLength":24}]}}
                """);
        assertEquals(2, candidates(sample).size(), "Ambiguous physical NICs must remain a candidate list");
        assertEquals("255.255.255.0", mask(24));
        assertEquals("0.0.0.0", mask(0));
        assertEquals("255.255.255.255", mask(32));
        assertThrows(IllegalArgumentException.class, () -> mask(33));
        assertEquals(0, laptopNumber(""));
        assertEquals(6, laptopNumber("6"));
        assertThrows(IllegalArgumentException.class, () -> laptopNumber("7"));
    }

    private static List<String> fixture(String mode) {
        String java = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java").toString();
        return List.of(java, "-cp", System.getProperty("java.class.path"), ProcessFixture.class.getName(), mode);
    }

    public static class ProcessFixture {
        public static void main(String[] args) throws Exception {
            System.out.println("LOCAL_PROCESS_FIXTURE_ONLY");
            if (args[0].equals("timeout")) { Thread.sleep(30_000); }
            System.exit(7);
        }
    }

    @Test
    @Timeout(10)
    void commandFailurePreservesExitAndCannotBecomePass(@TempDir Path temporaryDirectory) throws Exception {
        CommandResult result = execute(fixture("failure"), Duration.ofSeconds(5), temporaryDirectory);
        assertEquals(7, result.exitCode());
        assertTrue(result.output().contains("LOCAL_PROCESS_FIXTURE_ONLY"));
        assertEquals("BLOCKED", result.status());
    }

    @Test
    @Timeout(10)
    void commandDeadlineCannotBecomePass(@TempDir Path temporaryDirectory) throws Exception {
        CommandResult result = execute(fixture("timeout"), Duration.ofMillis(300), temporaryDirectory);
        assertTrue(result.timedOut());
        assertEquals("BLOCKED", result.status());
    }

    @Test
    void missingToolIsReportedAsBlocked(@TempDir Path temporaryDirectory) throws Exception {
        CommandResult result = execute(List.of(temporaryDirectory.resolve("absent-docker-tool").toString()),
                COMMAND_DEADLINE, temporaryDirectory);
        assertEquals(-1, result.exitCode());
        assertEquals("BLOCKED", result.status());
        assertFalse(result.output().isBlank());
    }

    @Test
    @Timeout(140)
    @EnabledIfSystemProperty(named = "lan.inventory.local", matches = "true")
    void collectOnlyThisWindowsLaptopWithoutClaimingLanAcceptance(@TempDir Path temporaryDirectory) throws Exception {
        int laptop = laptopNumber(System.getProperty("lan.laptop", ""));
        ObjectNode report = JSON.createObjectNode();
        report.put("task", "TV6-D24").put("mode", "LOCAL_READ_ONLY_INVENTORY");
        report.put("capturedAt", Instant.now().toString()).put("laptopAssignment", laptop == 0 ? "UNASSIGNED" : "Laptop-" + laptop);
        report.put("physicalLan", "NOT_RUN").put("remoteLaptops", "NOT_RUN");
        report.put("note", "JUnit PASS verifies collection/reporting only, not firewall reachability or cluster readiness.");
        ArrayNode pending = slots();
        report.set("laptops", pending);
        ArrayNode commands = report.putArray("commands");
        if (!System.getProperty("os.name").startsWith("Windows")) {
            report.put("inventoryStatus", "BLOCKED").put("reason", "Windows collector; run on each actual Windows laptop.");
            System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(report));
            return;
        }
        CommandResult windows = execute(List.of("powershell.exe", "-NoProfile", "-NonInteractive", "-Command", WINDOWS_INVENTORY),
                COMMAND_DEADLINE, temporaryDirectory);
        commands.add(windows.json());
        JsonNode observed = JSON.createObjectNode();
        if (windows.status().equals("PASS")) { observed = JSON.readTree(windows.output().strip()); }
        report.set("localObservation", observed);
        ArrayNode addresses = report.putArray("physicalAddressCandidates");
        for (JsonNode nic : candidates(observed)) {
            ObjectNode address = nic.deepCopy();
            address.put("mask", mask(nic.path("prefixLength").asInt()));
            addresses.add(address);
        }
        // Never collect full docker inspect/config/environment, which may expose DB secrets.
        CommandResult client = execute(List.of("docker", "--version"), COMMAND_DEADLINE, temporaryDirectory);
        CommandResult compose = execute(List.of("docker", "compose", "version"), COMMAND_DEADLINE, temporaryDirectory);
        CommandResult server = execute(List.of("docker", "version", "--format", "{{json .Server}}"), COMMAND_DEADLINE, temporaryDirectory);
        CommandResult images = execute(List.of("docker", "image", "ls", "--format", "{{.Repository}}:{{.Tag}} {{.ID}}"),
                COMMAND_DEADLINE, temporaryDirectory);
        for (CommandResult command : List.of(client, compose, server, images)) { commands.add(command.json()); }
        report.put("dockerCli", client.status()).put("dockerServer", server.status());
        report.put("firewallPortReachability", "NOT_RUN");
        report.put("offlineImageExport", server.status().equals("PASS") ? "NOT_RUN" : "BLOCKED");
        report.put("inventoryStatus", "BLOCKED"); // Still missing five other physical observations.
        report.put("reason", "Only this machine was measured; no LAN cluster or complete six-laptop inventory is certified.");
        if (laptop > 0) {
            ObjectNode slot = (ObjectNode) pending.get(laptop - 1);
            slot.put("status", "BLOCKED").put("hostname", observed.path("hostname").asText("BLOCKED"));
            slot.put("firewall", observed.path("firewallPortRules").path("status").asText("BLOCKED"));
            slot.put("docker", server.status());
            if (addresses.size() == 1) {
                slot.put("ipv4", addresses.get(0).path("ipv4").asText()).put("mask", addresses.get(0).path("mask").asText());
            } else { slot.put("ipv4", "BLOCKED").put("mask", "BLOCKED"); }
            slot.put("reason", "Local observation only; port reachability and deployment NOT_RUN; see localObservation.");
        }
        System.out.println("TV6-D24 LOCAL_INVENTORY_BEGIN");
        System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(report));
        System.out.println("TV6-D24 LOCAL_INVENTORY_END; PHYSICAL_LAN=NOT_RUN; AC01-AC10=NOT_RUN");
        // Actual collector failure is not hidden by a successful reporting test.
        assertEquals("PASS", windows.status(), "Local OS collector failed; see command evidence");
        assertFalse(observed.path("hostname").asText().isBlank());
    }
}
