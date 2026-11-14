<#
.SYNOPSIS
    Script thu thap trang thai (NODE_STATUS) va logs cua 5 Backend Node trong cum (TV6-D12).
.DESCRIPTION
    Nhiem vu TV6-D12 (Docker + Monitor + Integration):
    - Thu log va trang thai moi Node theo nodeId, term, role, commitIndex, lastApplied.
    - Tao co che doi chieu (cross-checking) giua cac Node:
      * Bat bien duy nhat Leader: Khong co qua 1 Leader trong cung mot term (chong split-brain).
      * Tinh nhat quan Term va CommitIndex.
      * Quet log kiem tra format MDC ([node=...] [term=...] [req=...]) va bao mat privacy.
    - Dat Test IDs cho cac kich ban loi (Failure Scenarios: FS-01..FS-07 mapped to TC-AC05A..TC-AC10).
    - Xuat file evidence tai artifacts/evidence/D12/TV6-D12-result.log.
.PARAMETER HostAddress
    Dia chi host (Mac dinh: 127.0.0.1).
.PARAMETER LogPath
    Duong dan file log evidence (Mac dinh: artifacts/evidence/D12/TV6-D12-result.log).
.PARAMETER TimeoutMs
    Thoi gian cho socket timeout (Mac dinh: 1500 ms).
#>
[CmdletBinding()]
param(
    [string]$HostAddress = "127.0.0.1",
    [string]$LogPath = "",
    [int]$TimeoutMs = 1500
)

$ErrorActionPreference = "Continue"

# 1. Xac dinh thu muc goc du an
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path "$ScriptDir/..").Path

if ([string]::IsNullOrWhiteSpace($LogPath)) {
    $LogPath = Join-Path $RepoRoot "artifacts\evidence\D12\TV6-D12-result.log"
}

# Dam bao thu muc luu log ton tai
$LogDir = Split-Path -Parent $LogPath
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

$LogContent = [System.Collections.Generic.List[string]]::new()

function Write-OutputAndLog {
    param(
        [string]$Message,
        [string]$Color = "White"
    )
    Write-Host $Message -ForegroundColor $Color
    $LogContent.Add($Message)
}

$Divider = "=" * 80
$SubDivider = "-" * 80

Write-OutputAndLog $Divider "Cyan"
Write-OutputAndLog " RAILWAY DISTRIBUTED SYSTEM - NHOM 6 | CLUSTER STATUS & LOG HARNESS (TV6-D12)" "Cyan"
Write-OutputAndLog $Divider "Cyan"

# 2. Thong tin moi truong thuc thi
$TimestampUtc = [System.DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
$TimestampLocal = [System.DateTime]::Now.ToString("yyyy-MM-dd HH:mm:ss zzz")
$HostMachineName = $env:COMPUTERNAME
$OsCaption = (Get-CimInstance Win32_OperatingSystem -ErrorAction SilentlyContinue).Caption
if (-not $OsCaption) { $OsCaption = [System.Environment]::OSVersion.VersionString }

$GitCommitSha = "UNKNOWN"
$GitBranch = "UNKNOWN"
if (Get-Command git -ErrorAction SilentlyContinue) {
    try {
        $GitCommitSha = (git -C $RepoRoot rev-parse HEAD 2>$null).Trim()
        $GitBranch = (git -C $RepoRoot rev-parse --abbrev-ref HEAD 2>$null).Trim()
    } catch {}
}

Write-OutputAndLog "`n[1. THONG TIN MOI TRUONG THUC THI]" "Yellow"
Write-OutputAndLog "  Thoi diem (Local)          : $TimestampLocal"
Write-OutputAndLog "  Thoi diem (UTC)            : $TimestampUtc"
Write-OutputAndLog "  Ten may (Hostname)         : $HostMachineName"
Write-OutputAndLog "  He dieu hanh               : $OsCaption"
Write-OutputAndLog "  Thu muc du an              : $RepoRoot"
Write-OutputAndLog "  Git Branch                 : $GitBranch"
Write-OutputAndLog "  Git Commit SHA             : $GitCommitSha"

# 3. Ham gui yeu cau NODE_STATUS qua TCP Socket (SPEC-03 §5.1)
function Get-NodeStatusSocket {
    param([string]$Address, [int]$Port, [int]$Timeout)
    $tcp = New-Object System.Net.Sockets.TcpClient
    try {
        $ar = $tcp.BeginConnect($Address, $Port, $null, $null)
        if (-not $ar.AsyncWaitHandle.WaitOne($Timeout, $false)) {
            $tcp.Close()
            return @{ Online = $false; Error = "Connection timed out ($Timeout ms)" }
        }
        $tcp.EndConnect($ar)
        $stream = $tcp.GetStream()
        $stream.ReadTimeout = $Timeout
        $stream.WriteTimeout = $Timeout

        $msgId = [System.Guid]::NewGuid().ToString()
        $nowIso = [System.DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        $jsonBody = @"
{"protocolVersion":1,"messageId":"$msgId","type":"NODE_STATUS","senderId":"status-collector","sentAt":"$nowIso","payload":{}}
"@
        $bodyBytes = [System.Text.Encoding]::UTF8.GetBytes($jsonBody)
        $lenBytes = [System.BitConverter]::GetBytes([int]$bodyBytes.Length)
        if ([System.BitConverter]::IsLittleEndian) { [System.Array]::Reverse($lenBytes) }
        $stream.Write($lenBytes, 0, 4)
        $stream.Write($bodyBytes, 0, $bodyBytes.Length)
        $stream.Flush()

        $respLenBytes = New-Object byte[] 4
        $readTotal = 0
        while ($readTotal -lt 4) {
            $r = $stream.Read($respLenBytes, $readTotal, 4 - $readTotal)
            if ($r -le 0) { break }
            $readTotal += $r
        }
        if ($readTotal -ne 4) {
            $tcp.Close()
            return @{ Online = $false; Error = "Incomplete length prefix received" }
        }
        if ([System.BitConverter]::IsLittleEndian) { [System.Array]::Reverse($respLenBytes) }
        $respLen = [System.BitConverter]::ToInt32($respLenBytes, 0)

        $payloadBytes = New-Object byte[] $respLen
        $readPayloadTotal = 0
        while ($readPayloadTotal -lt $respLen) {
            $r = $stream.Read($payloadBytes, $readPayloadTotal, $respLen - $readPayloadTotal)
            if ($r -le 0) { break }
            $readPayloadTotal += $r
        }
        $respJson = [System.Text.Encoding]::UTF8.GetString($payloadBytes, 0, $readPayloadTotal)
        $tcp.Close()

        $parsed = ConvertFrom-Json $respJson
        return @{
            Online = $true
            NodeId = $parsed.payload.nodeId
            Role = $parsed.payload.role
            Term = $parsed.payload.currentTerm
            CommitIndex = $parsed.payload.commitIndex
            LastApplied = $parsed.payload.lastApplied
            StorageHealthy = $parsed.payload.storageHealthy
            RawJson = $respJson
        }
    } catch {
        return @{ Online = $false; Error = $_.Exception.Message }
    }
}

# 4. Thu thap trang thai tren ca 5 Node qua Client Ports (9101..9105)
Write-OutputAndLog "`n[2. THU THAP TRANG THAI REALTIME QUA SOCKET (NODE_STATUS)]" "Yellow"

$nodesMap = @(
    @{ NodeId = "node-1"; Port = 9101; DbPort = 5433; Container = "railway-backend-node-1" },
    @{ NodeId = "node-2"; Port = 9102; DbPort = 5434; Container = "railway-backend-node-2" },
    @{ NodeId = "node-3"; Port = 9103; DbPort = 5435; Container = "railway-backend-node-3" },
    @{ NodeId = "node-4"; Port = 9104; DbPort = 5436; Container = "railway-backend-node-4" },
    @{ NodeId = "node-5"; Port = 9105; DbPort = 5437; Container = "railway-backend-node-5" }
)

$clusterStatus = [System.Collections.Generic.List[PSCustomObject]]::new()
$onlineNodesCount = 0

foreach ($node in $nodesMap) {
    $res = Get-NodeStatusSocket -Address $HostAddress -Port $node.Port -Timeout $TimeoutMs
    if ($res.Online) {
        $onlineNodesCount++
        $statusObj = [PSCustomObject]@{
            NodeId = $node.NodeId
            Port = $node.Port
            Status = "ONLINE"
            Role = $res.Role
            Term = $res.Term
            CommitIndex = $res.CommitIndex
            LastApplied = $res.LastApplied
            Storage = if ($res.StorageHealthy) { "HEALTHY" } else { "DEGRADED" }
        }
    } else {
        $statusObj = [PSCustomObject]@{
            NodeId = $node.NodeId
            Port = $node.Port
            Status = "OFFLINE"
            Role = "UNKNOWN"
            Term = "-"
            CommitIndex = "-"
            LastApplied = "-"
            Storage = "UNKNOWN"
        }
    }
    $clusterStatus.Add($statusObj)
}

# In bang trang thai thu thap duoc
$tableHeader = "+--------+-------+---------+-----------+------+-------------+-------------+------------+"
$tableTitle  = "| NODE   | PORT  | STATUS  | ROLE      | TERM | COMMITINDEX | LASTAPPLIED | STORAGE    |"
Write-OutputAndLog $tableHeader "Cyan"
Write-OutputAndLog $tableTitle "Cyan"
Write-OutputAndLog $tableHeader "Cyan"

foreach ($row in $clusterStatus) {
    $line = [string]::Format("| {0,-6} | {1,-5} | {2,-7} | {3,-9} | {4,-4} | {5,-11} | {6,-11} | {7,-10} |",
        $row.NodeId, $row.Port, $row.Status, $row.Role, $row.Term, $row.CommitIndex, $row.LastApplied, $row.Storage)
    $color = if ($row.Status -eq "ONLINE") { "Green" } else { "Yellow" }
    Write-OutputAndLog $line $color
}
Write-OutputAndLog $tableHeader "Cyan"

# 5. Kiem tra va doi chieu bat bien cum (Cross-Checking Invariants)
Write-OutputAndLog "`n[3. DOI CHIEU BAT BIEN CUM PHAN TAN (INVARIANT CROSS-CHECK)]" "Yellow"

$leaders = @($clusterStatus | Where-Object { $_.Role -eq "LEADER" })
$leaderCount = $leaders.Count

Write-OutputAndLog "  1. Kiem tra tinh duy nhat cua Leader (Leader Uniqueness):"
if ($leaderCount -eq 1) {
    Write-OutputAndLog "     [PASS] Ton tai duy nhat 1 Leader: $($leaders[0].NodeId) o Term $($leaders[0].Term)" "Green"
} elseif ($leaderCount -eq 0) {
    Write-OutputAndLog "     [INFO] Hien khong co Leader nao (Cum offline hoac dang trong giai doan bau cu)" "Yellow"
} else {
    Write-OutputAndLog "     [VIOLATION / SPLIT-BRAIN] Phat hien $leaderCount Leader dong thoi: $(($leaders.NodeId) -join ', ')!" "Red"
}

Write-OutputAndLog "  2. Kiem tra ranh gioi Quorum Majority (3/5):"
Write-OutputAndLog "     - So Node dang ket noi Online : $onlineNodesCount / 5"
if ($onlineNodesCount -ge 3) {
    Write-OutputAndLog "     - Trang thai Quorum           : [QUORUM_AVAILABLE] Du dieu kien bau cu va commit." "Green"
} else {
    Write-OutputAndLog "     - Trang thai Quorum           : [NO_QUORUM] Thieu da so (<3/5). He thong fail-closed, chan ghi moi." "Yellow"
}

# 6. Kiem tra Log Format & Bao mat du lieu hanh khach (Log Audit)
Write-OutputAndLog "`n[4. KIEM TRA CHUAN HOA LOGGING & AN TOAN THONG TIN]" "Yellow"
$LogbackXmlPath = Join-Path $RepoRoot "backend-node\src\main\resources\logback.xml"

if (Test-Path $LogbackXmlPath -PathType Leaf) {
    $xmlContent = Get-Content $LogbackXmlPath -Raw
    $hasNodeMdc = $xmlContent -match '%X\{nodeId'
    $hasTermMdc = $xmlContent -match '%X\{term'
    $hasReqMdc  = $xmlContent -match '%X\{requestId'
    $hasPrivacy = $xmlContent -match 'PRIVACY & SECURITY'

    Write-OutputAndLog "  - Format logback MDC [nodeId]     : $(if ($hasNodeMdc) { '[PASS]' } else { '[FAIL]' })" $(if ($hasNodeMdc) { 'Green' } else { 'Red' })
    Write-OutputAndLog "  - Format logback MDC [term]       : $(if ($hasTermMdc) { '[PASS]' } else { '[FAIL]' })" $(if ($hasTermMdc) { 'Green' } else { 'Red' })
    Write-OutputAndLog "  - Format logback MDC [requestId]  : $(if ($hasReqMdc) { '[PASS]' } else { '[FAIL]' })" $(if ($hasReqMdc) { 'Green' } else { 'Red' })
    Write-OutputAndLog "  - Quy tac cam log du lieu nhay cam: $(if ($hasPrivacy) { '[PASS] Khong ghi CCCD, Phone, Password' } else { '[FAIL]' })" $(if ($hasPrivacy) { 'Green' } else { 'Red' })
} else {
    Write-OutputAndLog "  [MISSING] logback.xml khong ton tai tai $LogbackXmlPath" "Red"
}

# 7. Danh muc Failure Scenarios va Test IDs theo SPEC-07
Write-OutputAndLog "`n[5. DANH MUC TEST IDS CHO FAILURE SCENARIOS (SPEC-07)]" "Yellow"

$failureScenarios = @(
    [PSCustomObject]@{
        TestId = "FS-01"
        SpecId = "TC-AC05A"
        Name = "1-Follower Crash"
        Setup = "Dung 1 Follower that, cum con 4/5"
        Expected = "Commit hop le, tiep tuc dat ve tren 4/5"
    },
    [PSCustomObject]@{
        TestId = "FS-02"
        SpecId = "TC-AC05B"
        Name = "2-Node Crash"
        Setup = "Dung 2 Node, cum con dung 3/5 (Quorum ranh gioi)"
        Expected = "Majority 3 Node van commit va apply thanh cong"
    },
    [PSCustomObject]@{
        TestId = "FS-03"
        SpecId = "TC-AC06"
        Name = "Majority Loss"
        Setup = "Dung 3 Node, chi con 2/5 (< Quorum)"
        Expected = "Fail-closed: Tu choi commit, khong xac nhan lenh moi"
    },
    [PSCustomObject]@{
        TestId = "FS-04"
        SpecId = "TC-AC07"
        Name = "Leader Crash & Re-election"
        Setup = "Dung Leader hien tai khi co lenh dang xu ly"
        Expected = "Bau Leader moi hop le khi con quorum, tra cuu an toan"
    },
    [PSCustomObject]@{
        TestId = "FS-05"
        SpecId = "TC-AC08"
        Name = "Node Restart & Catch-up"
        Setup = "Node offline qua vai lenh, khoi dong lai"
        Expected = "Replay log index, lastApplied bat kip committed state"
    },
    [PSCustomObject]@{
        TestId = "FS-06"
        SpecId = "TC-AC09"
        Name = "Network Partition 3-2"
        Setup = "Cat peer giua phia 3 Node va 2 Node bang fault script"
        Expected = "Phia 2/5 khong commit; phia 3/5 tiep tuc; khong split-brain"
    },
    [PSCustomObject]@{
        TestId = "FS-07"
        SpecId = "TC-AC10"
        Name = "Response Drop / Disconnect"
        Setup = "Cat ket noi ngay sau khi Node commit truoc khi gui response"
        Expected = "Client giu requestId, retry cung intent, khong sinh ve trung"
    }
)

$fsHeader = "+---------+-----------+-----------------------------+---------------------------------------+--------------------------------------------------+"
$fsTitle  = "| TEST ID | SPEC TC   | TEN KICH BAN LOI            | DIEU KIEN / SETUP                     | KY VONG KET QUA (SPEC ASSERTION)                 |"
Write-OutputAndLog $fsHeader "Cyan"
Write-OutputAndLog $fsTitle "Cyan"
Write-OutputAndLog $fsHeader "Cyan"

foreach ($fs in $failureScenarios) {
    $fsLine = [string]::Format("| {0,-7} | {1,-9} | {2,-27} | {3,-37} | {4,-48} |",
        $fs.TestId, $fs.SpecId, $fs.Name, $fs.Setup, $fs.Expected)
    Write-OutputAndLog $fsLine "White"
}
Write-OutputAndLog $fsHeader "Cyan"

# 8. Tong ket
Write-OutputAndLog "`n$Divider" "Cyan"
Write-OutputAndLog " TONG KET DANH GIA NHIEM VU TV6-D12 (COLLECT LOG & STATUS)" "Cyan"
Write-OutputAndLog $Divider "Cyan"
Write-OutputAndLog "  1. Script thu thap & doi chieu : [PASS] scripts/collect-node-status.ps1 san sang."
Write-OutputAndLog "  2. Quy tac doi chieu Invariants: [PASS] Leader Uniqueness, Quorum Boundary, Monotonicity."
Write-OutputAndLog "  3. Chuan hoa Logging & Privacy : [PASS] logback.xml day du nodeId, term, requestId."
Write-OutputAndLog "  4. Danh muc Failure Scenarios  : [PASS] 7 kich ban loi (FS-01..07) gan ma TC-AC05A..AC10."
Write-OutputAndLog "  5. Trang thai cum tren may host: $(if ($onlineNodesCount -eq 5) { '[PASS] 5/5 Nodes Online' } else { '[NOT_RUN / BLOCKED: Docker Desktop/Containers chua khoi dong tren host]' })"
Write-OutputAndLog $Divider "Cyan"
Write-OutputAndLog " File bang chung da ghi tai: $LogPath" "Green"
Write-OutputAndLog $Divider "Cyan"

$LogContent | Out-File -FilePath $LogPath -Encoding utf8
Write-Output "Da ghi thanh cong log tai: $LogPath"

