package vn.nhom6.railway.networking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit and structural validation tests for the LAN Preflight Connectivity Script (scripts/lan-connectivity-check.ps1).
 *
 * <p>Requirements:
 * <ul>
 *   <li>Task: TV3-D24 (SPEC-08 §3 & §3.1, §6 and AGENTS.md §0, §5).</li>
 *   <li>Verifies presence, parameters, and syntax rules of {@code scripts/lan-connectivity-check.ps1}.</li>
 *   <li>Verifies port checking for Client 9100 and Raft Peer 9200.</li>
 *   <li>Verifies 5-voter cluster quorum checks (node-1..node-5).</li>
 *   <li>Verifies firewall inspection and guidance logic.</li>
 *   <li>Verifies compliance with AGENTS.md safety invariant (no false claim of physical 5-laptop connectivity).</li>
 * </ul>
 */
public class LanConnectivityCheckScriptTest {

    private static File findFile(String relativePath) {
        File direct = new File(relativePath);
        if (direct.exists()) {
            return direct;
        }
        File parent = new File(".." + File.separator + relativePath);
        if (parent.exists()) {
            return parent;
        }
        return direct;
    }

    @Test
    @DisplayName("TC-SCRIPT-01: scripts/lan-connectivity-check.ps1 exists and is not empty")
    public void testScriptFileExistsAndIsNotEmpty() {
        File script = findFile("scripts/lan-connectivity-check.ps1");
        assertTrue(script.exists(), "scripts/lan-connectivity-check.ps1 must exist");
        assertTrue(script.isFile(), "Must be a regular file");
        assertTrue(script.length() > 0, "Script must not be empty");
    }

    @Test
    @DisplayName("TC-SCRIPT-02: Script defines all required parameters and help metadata")
    public void testScriptHeaderAndParameterDefinitions() throws Exception {
        File script = findFile("scripts/lan-connectivity-check.ps1");
        String content = Files.readString(script.toPath());

        // Help metadata
        assertTrue(content.contains(".SYNOPSIS"), "Script must contain .SYNOPSIS block");
        assertTrue(content.contains(".DESCRIPTION"), "Script must contain .DESCRIPTION block");
        assertTrue(content.contains(".PARAMETER ConfigFile"), "Must document ConfigFile parameter");
        assertTrue(content.contains(".PARAMETER AllNodes"), "Must document AllNodes parameter");
        assertTrue(content.contains(".PARAMETER Mode"), "Must document Mode parameter");
        assertTrue(content.contains(".PARAMETER LogPath"), "Must document LogPath parameter");

        // Parameter definitions
        assertTrue(content.contains("$ConfigFile"), "Script must declare $ConfigFile parameter");
        assertTrue(content.contains("$AllNodes"), "Script must declare $AllNodes parameter");
        assertTrue(content.contains("Preflight") && content.contains("LocalCheck") && content.contains("SelfTest"),
                "Script must declare Preflight, LocalCheck, SelfTest modes");
        assertTrue(content.contains("$TargetHost"), "Script must declare $TargetHost parameter");
        assertTrue(content.contains("$TimeoutMs"), "Script must declare $TimeoutMs parameter");
        assertTrue(content.contains("$CheckFirewall"), "Script must declare $CheckFirewall parameter");
    }

    @Test
    @DisplayName("TC-SCRIPT-03: Script enforces standard ports 9100 (Client) and 9200 (Raft Peer)")
    public void testScriptEnforcesStandardPorts9100And9200() throws Exception {
        File script = findFile("scripts/lan-connectivity-check.ps1");
        String content = Files.readString(script.toPath());

        assertTrue(content.contains("9100"), "Script must reference standard Client port 9100");
        assertTrue(content.contains("9200"), "Script must reference standard Raft Peer port 9200");
        assertTrue(content.contains("Test-PortBindAvailability"), "Script must have port availability check function");
    }

    @Test
    @DisplayName("TC-SCRIPT-04: Script enforces exactly five configured voters (node-1..node-5)")
    public void testScriptEnforcesFiveVoterQuorumRule() throws Exception {
        File script = findFile("scripts/lan-connectivity-check.ps1");
        String content = Files.readString(script.toPath());

        assertTrue(content.contains("Test-NodePeersFormat"), "Script must have peer format check function");
        assertTrue(content.contains("node-1") && content.contains("node-5"), "Script must validate node-1 through node-5");
        assertTrue(content.contains("VotersCount -eq 5") || content.contains("Count -eq 5"),
                "Script must enforce exactly 5 voter peers");
    }

    @Test
    @DisplayName("TC-SCRIPT-05: Script contains firewall inspection and guidance logic")
    public void testScriptContainsFirewallCheckLogic() throws Exception {
        File script = findFile("scripts/lan-connectivity-check.ps1");
        String content = Files.readString(script.toPath());

        assertTrue(content.contains("Test-FirewallPreflight"), "Script must have firewall preflight function");
        assertTrue(content.contains("Get-NetFirewallRule") || content.contains("netsh advfirewall"),
                "Script must query Windows Firewall rules");
        assertTrue(content.contains("New-NetFirewallRule"),
                "Script must provide remediation command for missing firewall rules");
    }

    @Test
    @DisplayName("TC-SCRIPT-06: Script strictly adheres to AGENTS.md (no false claim of physical cluster)")
    public void testScriptCompliesWithNoPhysicalClaimInvariant() throws Exception {
        File script = findFile("scripts/lan-connectivity-check.ps1");
        String content = Files.readString(script.toPath());

        assertTrue(content.contains("PLACEHOLDER_MODE") || content.contains("Placeholder"),
                "Script must flag placeholder IP mode");
        assertTrue(content.contains("NOT_RUN") || content.contains("DEFERRED_TO_D25"),
                "Script must explicitly declare physical cluster test as NOT_RUN / DEFERRED");
    }

    @Test
    @DisplayName("TC-SCRIPT-07: All five LAN node properties files satisfy script's preflight rules")
    public void testAllFiveLanPropertiesValidAgainstScriptRules() throws Exception {
        List<String> nodeIds = List.of("node-1", "node-2", "node-3", "node-4", "node-5");

        for (String nodeId : nodeIds) {
            File propFile = findFile("configs/lan/" + nodeId + ".properties");
            assertTrue(propFile.exists());

            Properties props = new Properties();
            try (InputStream in = new FileInputStream(propFile)) {
                props.load(in);
            }

            assertEquals(nodeId, props.getProperty("node.id"));
            assertEquals("railway-demo-5", props.getProperty("cluster.id"));
            assertEquals("9100", props.getProperty("client.port"), "Client port must be 9100");
            assertEquals("9200", props.getProperty("raft.port"), "Raft port must be 9200");

            String peersStr = props.getProperty("node.peers");
            assertNotNull(peersStr);

            String[] tokens = peersStr.split(",");
            assertEquals(5, tokens.length, "Must have 5 peer tokens");

            Set<String> parsedIds = new HashSet<>();
            for (String token : tokens) {
                String[] parts = token.trim().split("@");
                assertEquals(2, parts.length);
                parsedIds.add(parts[0]);
                assertTrue(parts[1].endsWith(":9200"), "Peer port must be 9200");
            }
            assertEquals(Set.copyOf(nodeIds), parsedIds);
        }
    }
}
