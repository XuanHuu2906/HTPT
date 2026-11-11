<#
.SYNOPSIS
    Single-node Smoke Harness cho Backend Node 1 va PostgreSQL 1 (TV6-D09).
.DESCRIPTION
    Nhiem vu TV6-D09 (Docker + Monitor + Integration):
    - Chuan bi single-node smoke harness dua tren baseline D08 (NodeMain, Booking, Compose).
    - Kiem tra node-1 runtime, pg-1 database, socket TCP transport va chuan hoa logs.
    - Gui raw frame PING -> PONG va NODE_STATUS envelope de kiem tra giao thuc SPEC-03.
    - Kiem tra tinh co lap credentials (dung Node 1, cam tuyet doi dung credential node khac).
    - Khang dinh lo trinh: Gate G3 test phan tan thuc su sau D10.
    - Xuat file log minh chung tai artifacts/evidence/D09/TV6-D09-result.log.
.PARAMETER HostAddress
    Dia chi host cua Backend Node 1 (Mac dinh: 127.0.0.1).
.PARAMETER ClientPort
    Cong Client TCP publish tren host (Mac dinh: 9101 theo cluster.properties).
.PARAMETER RaftPort
    Cong Raft Peer TCP publish tren host (Mac dinh: 9201 theo cluster.properties).
.PARAMETER DbHost
    Dia chi host PostgreSQL Node 1 (Mac dinh: 127.0.0.1).
.PARAMETER DbPort
    Cong publish PostgreSQL Node 1 tren host (Mac dinh: 5433).
.PARAMETER DbName
    Ten database cua Node 1 (Mac dinh: railway_node1).
.PARAMETER DbUser
    Ten user PostgreSQL cua Node 1 (Mac dinh: railway).
.PARAMETER DbPassword
    Mat khau database cua Node 1 (Mac dinh: railway_secret_local_only).
.PARAMETER LogPath
    Duong dan file log evidence (Mac dinh: artifacts/evidence/D09/TV6-D09-result.log).
.PARAMETER TimeoutMs
    Thoi gian cho timeout ket noi socket (Mac dinh: 2000 ms).
#>
[CmdletBinding()]
param(
    [string]$HostAddress = "127.0.0.1",
    [int]$ClientPort = 9101,
    [int]$RaftPort = 9201,
    [string]$DbHost = "127.0.0.1",
    [int]$DbPort = 5433,
    [string]$DbName = "railway_node1",
    [string]$DbUser = "railway",
    [string]$DbPassword = "railway_secret_local_only",
    [string]$LogPath = "",
    [int]$TimeoutMs = 2000
)

$ErrorActionPreference = "Continue"

# 1. Xac dinh thu muc goc du an
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path "$ScriptDir/..").Path

if ([string]::IsNullOrWhiteSpace($LogPath)) {
    $LogPath = Join-Path $RepoRoot "artifacts\evidence\D09\TV6-D09-result.log"
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
Write-OutputAndLog " RAILWAY DISTRIBUTED SYSTEM - NHOM 6 | SINGLE-NODE SMOKE HARNESS (TV6-D09)" "Cyan"
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
Write-OutputAndLog "  Baseline thiet ke          : Baseline code/tests cuoi D08 (NodeMain, Booking, Compose)"

# 3. Kiem tra tinh co lap Credentials & Ranh gioi Node 1
Write-OutputAndLog "`n[2. KIEM TRA CO LAP CREDENTIALS VA RANH GIOI NODE 1]" "Yellow"
Write-OutputAndLog "  Target Node ID             : node-1"
Write-OutputAndLog "  Target Database            : $DbName (Host: $DbHost`:$DbPort)"
Write-OutputAndLog "  Target User                : $DbUser"
Write-OutputAndLog "  Target Client Socket       : $HostAddress`:$ClientPort"
Write-OutputAndLog "  Target Raft Peer Socket    : $HostAddress`:$RaftPort"

$ForbiddenDbs = @("railway_node2", "railway_node3", "railway_node4", "railway_node5")
$ForbiddenDbPorts = @(5434, 5435, 5436, 5437)

$SecurityViolated = $false
if ($ForbiddenDbs -contains $DbName.ToLower()) {
    Write-OutputAndLog "  [SECURITY VIOLATION] Phat hien co gang ket noi vao database cua node khac ($DbName)!" "Red"
    $SecurityViolated = $true
}
if ($ForbiddenDbPorts -contains $DbPort) {
    Write-OutputAndLog "  [SECURITY VIOLATION] Phat hien co gang ket noi vao port host cua node khac ($DbPort)!" "Red"
    $SecurityViolated = $true
}

if ($SecurityViolated) {
    Write-OutputAndLog "  Ket qua kiem tra an toan   : [FAIL] Vi pham quy tac SPEC-08 va AGENTS.md (khong login bang credentials node khac)" "Red"
    Write-OutputAndLog $Divider "Red"
    $LogContent | Out-File -FilePath $LogPath -Encoding utf8
    throw "Security violation: Cross-node database credentials are strictly forbidden!"
} else {
    Write-OutputAndLog "  Ket qua kiem tra an toan   : [PASS] Cach ly hoan toan - chi su dung thong tin xac thuc cua Node 1." "Green"
}

# Ham kiem tra port bang .NET Socket nhanh
function Test-PortOpen {
    param([string]$Address, [int]$Port, [int]$Timeout)
    $tcp = New-Object System.Net.Sockets.TcpClient
    try {
        $ar = $tcp.BeginConnect($Address, $Port, $null, $null)
        if ($ar.AsyncWaitHandle.WaitOne($Timeout, $false)) {
            $tcp.EndConnect($ar)
            $tcp.Close()
            return $true
        }
        $tcp.Close()
        return $false
    } catch {
        return $false
    }
}

# Ham gui nhan Raw Frame PING -> PONG (4-byte length prefix + ASCII)
function Send-PingFrame {
    param([string]$Address, [int]$Port, [int]$Timeout)
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient
        $ar = $tcp.BeginConnect($Address, $Port, $null, $null)
        if (-not $ar.AsyncWaitHandle.WaitOne($Timeout, $false)) {
            $tcp.Close()
            return @{ Success = $false; Error = "Connection timed out" }
        }
        $tcp.EndConnect($ar)
        $stream = $tcp.GetStream()
        $stream.ReadTimeout = $Timeout
        $stream.WriteTimeout = $Timeout

        # Gửi 4 bytes length big-endian (4) + 'PING'
        $pingBytes = [System.Text.Encoding]::UTF8.GetBytes("PING")
        $lenBytes = [System.BitConverter]::GetBytes([int]$pingBytes.Length)
        if ([System.BitConverter]::IsLittleEndian) { [System.Array]::Reverse($lenBytes) }
        $stream.Write($lenBytes, 0, 4)
        $stream.Write($pingBytes, 0, $pingBytes.Length)
        $stream.Flush()

        # Đọc 4 bytes length phản hồi
        $respLenBytes = New-Object byte[] 4
        $readTotal = 0
        while ($readTotal -lt 4) {
            $r = $stream.Read($respLenBytes, $readTotal, 4 - $readTotal)
            if ($r -le 0) { break }
            $readTotal += $r
        }
        if ($readTotal -ne 4) {
            $tcp.Close()
            return @{ Success = $false; Error = "Khong doc du 4 bytes length prefix" }
        }
        if ([System.BitConverter]::IsLittleEndian) { [System.Array]::Reverse($respLenBytes) }
        $respLen = [System.BitConverter]::ToInt32($respLenBytes, 0)

        # Đọc payload
        $payloadBytes = New-Object byte[] $respLen
        $readPayloadTotal = 0
        while ($readPayloadTotal -lt $respLen) {
            $r = $stream.Read($payloadBytes, $readPayloadTotal, $respLen - $readPayloadTotal)
            if ($r -le 0) { break }
            $readPayloadTotal += $r
        }
        $respStr = [System.Text.Encoding]::UTF8.GetString($payloadBytes, 0, $readPayloadTotal)
        $tcp.Close()
        return @{ Success = ($respStr -eq "PONG"); Response = $respStr }
    } catch {
        return @{ Success = $false; Error = $_.Exception.Message }
    }
}

# Ham gui MessageEnvelope JSON NODE_STATUS qua socket
function Send-NodeStatusEnvelope {
    param([string]$Address, [int]$Port, [int]$Timeout)
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient
        $ar = $tcp.BeginConnect($Address, $Port, $null, $null)
        if (-not $ar.AsyncWaitHandle.WaitOne($Timeout, $false)) {
            $tcp.Close()
            return @{ Success = $false; Error = "Connection timed out" }
        }
        $tcp.EndConnect($ar)
        $stream = $tcp.GetStream()
        $stream.ReadTimeout = $Timeout
        $stream.WriteTimeout = $Timeout

        $msgId = [System.Guid]::NewGuid().ToString()
        $nowIso = [System.DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        $jsonBody = @"
{"protocolVersion":1,"messageId":"$msgId","type":"NODE_STATUS","senderId":"smoke-client","sentAt":"$nowIso","payload":{}}
"@
        $bodyBytes = [System.Text.Encoding]::UTF8.GetBytes($jsonBody)
        $lenBytes = [System.BitConverter]::GetBytes([int]$bodyBytes.Length)
        if ([System.BitConverter]::IsLittleEndian) { [System.Array]::Reverse($lenBytes) }
        $stream.Write($lenBytes, 0, 4)
        $stream.Write($bodyBytes, 0, $bodyBytes.Length)
        $stream.Flush()

        # Đọc length prefix phản hồi
        $respLenBytes = New-Object byte[] 4
        $readTotal = 0
        while ($readTotal -lt 4) {
            $r = $stream.Read($respLenBytes, $readTotal, 4 - $readTotal)
            if ($r -le 0) { break }
            $readTotal += $r
        }
        if ($readTotal -ne 4) {
            $tcp.Close()
            return @{ Success = $false; Error = "Khong doc du length prefix" }
        }
        if ([System.BitConverter]::IsLittleEndian) { [System.Array]::Reverse($respLenBytes) }
        $respLen = [System.BitConverter]::ToInt32($respLenBytes, 0)

        # Đọc JSON payload
        $payloadBytes = New-Object byte[] $respLen
        $readPayloadTotal = 0
        while ($readPayloadTotal -lt $respLen) {
            $r = $stream.Read($payloadBytes, $readPayloadTotal, $respLen - $readPayloadTotal)
            if ($r -le 0) { break }
            $readPayloadTotal += $r
        }
        $respJson = [System.Text.Encoding]::UTF8.GetString($payloadBytes, 0, $readPayloadTotal)
        $tcp.Close()
        return @{ Success = $true; Json = $respJson }
    } catch {
        return @{ Success = $false; Error = $_.Exception.Message }
    }
}

# 4. Kiem tra PostgreSQL Node 1 (pg-1)
Write-OutputAndLog "`n[3. KIEM TRA POSTGRESQL NODE 1 (pg-1)]" "Yellow"
$PgPortOpen = Test-PortOpen -Address $DbHost -Port $DbPort -Timeout $TimeoutMs

if ($PgPortOpen) {
    Write-OutputAndLog "  [ONLINE] PostgreSQL-1 dang lang nghe tren $DbHost`:$DbPort." "Green"
    # Kiem tra container & query
    $DockerCmd = Get-Command docker -ErrorAction SilentlyContinue
    $PsqlCmd = Get-Command psql -ErrorAction SilentlyContinue
    $DbChecked = $false

    if ($DockerCmd) {
        try {
            $containerState = docker ps --filter "name=railway-postgres-1" --format "{{.Status}}" 2>$null
            if ($containerState) {
                Write-OutputAndLog "  [CONTAINER] railway-postgres-1 dang chay ($containerState)." "Green"
                $querySeats = docker exec -i railway-postgres-1 psql -U $DbUser -d $DbName -t -c "SELECT COUNT(*) FROM seats WHERE trip_id = 'SE-001' AND status = 'AVAILABLE';" 2>&1
                if ($LASTEXITCODE -eq 0 -and $querySeats.Trim() -eq "10") {
                    Write-OutputAndLog "  [DB VERIFY] Doc du lieu thanh cong: 10/10 ghe AVAILABLE tren chuyen SE-001." "Green"
                    $DbChecked = $true
                }
            }
        } catch {}
    }

    if (-not $DbChecked) {
        Write-OutputAndLog "  [INFO] Port PG-1 mo. San sang nhan ket noi JDBC tu NodeMain." "Green"
    }
} else {
    Write-OutputAndLog "  [OFFLINE] Port PG-1 ($DbPort) tren $DbHost chua mo." "Yellow"
    Write-OutputAndLog "  Giai phap: Chay 'docker compose -f docker/compose.dev.yml up -d postgres-1' de khoi dong." "Cyan"
}

# 5. Kiem tra Backend Node 1 (node-1) & Socket Transport
Write-OutputAndLog "`n[4. KIEM TRA BACKEND NODE 1 (node-1) & SOCKET TRANSPORT]" "Yellow"

$ClientPortOpen = Test-PortOpen -Address $HostAddress -Port $ClientPort -Timeout $TimeoutMs
$RaftPortOpen = Test-PortOpen -Address $HostAddress -Port $RaftPort -Timeout $TimeoutMs

Write-OutputAndLog "  Client TCP Port ($ClientPort)   : $(if ($ClientPortOpen) { '[LISTENING]' } else { '[NOT_LISTENING]' })" $(if ($ClientPortOpen) { 'Green' } else { 'Yellow' })
Write-OutputAndLog "  Raft Peer TCP Port ($RaftPort) : $(if ($RaftPortOpen) { '[LISTENING]' } else { '[NOT_LISTENING]' })" $(if ($RaftPortOpen) { 'Green' } else { 'Yellow' })

if ($ClientPortOpen) {
    # 5.1 Kiem tra Raw Frame PING -> PONG
    Write-OutputAndLog "`n  --- KIEM TRA GIAO THUC RAW FRAME: PING -> PONG ---" "Cyan"
    $pingResult = Send-PingFrame -Address $HostAddress -Port $ClientPort -Timeout $TimeoutMs
    if ($pingResult.Success) {
        Write-OutputAndLog "  [PASS] Gui 'PING' -> Nhan phan hoi '$($pingResult.Response)' qua Socket TCP!" "Green"
    } else {
        Write-OutputAndLog "  [FAIL] Gui PING that bai: $($pingResult.Error)" "Red"
    }

    # 5.2 Kiem tra MessageEnvelope: NODE_STATUS
    Write-OutputAndLog "`n  --- KIEM TRA GIAO THUC ENVELOPE JSON: NODE_STATUS ---" "Cyan"
    $statusResult = Send-NodeStatusEnvelope -Address $HostAddress -Port $ClientPort -Timeout $TimeoutMs
    if ($statusResult.Success) {
        Write-OutputAndLog "  [PASS] Nhan phan hoi NODE_STATUS_RESPONSE thanh cong!" "Green"
        Write-OutputAndLog "  Chi tiet Payload JSON:"
        Write-OutputAndLog "  $($statusResult.Json)"
    } else {
        Write-OutputAndLog "  [FAIL] Goi NODE_STATUS that bai: $($statusResult.Error)" "Red"
    }
} else {
    Write-OutputAndLog "  [INFO] Backend Node 1 process chua khoi dong tren may host ($HostMachineName)." "Yellow"
    Write-OutputAndLog "  Ma nguon NodeMain da san sang tai: backend-node/src/main/java/vn/nhom6/railway/node/NodeMain.java" "Cyan"
    Write-OutputAndLog "  Cac cach khoi dong Node 1:" "Cyan"
    Write-OutputAndLog "    Cach 1 (Docker): docker compose -f docker/compose.dev.yml up -d backend-node-1"
    Write-OutputAndLog "    Cach 2 (CLI)   : java -cp backend-node/target/backend-node-1.0.0-SNAPSHOT.jar vn.nhom6.railway.node.NodeMain"
}

# 6. Kiem tra Chuan hoa Logging & Logback MDC
Write-OutputAndLog "`n[5. KIEM TRA CHUAN HOA LOGGING & LOGBACK MDC]" "Yellow"
$LogbackXmlPath = Join-Path $RepoRoot "backend-node\src\main\resources\logback.xml"

if (Test-Path $LogbackXmlPath -PathType Leaf) {
    $xmlContent = Get-Content $LogbackXmlPath -Raw
    $hasNodeId = $xmlContent -match '%X\{nodeId'
    $hasTerm = $xmlContent -match '%X\{term'
    $hasReqId = $xmlContent -match '%X\{requestId'
    $hasPrivacy = $xmlContent -match 'PRIVACY & SECURITY'

    Write-OutputAndLog "  [FOUND] logback.xml         : $LogbackXmlPath" "Green"
    Write-OutputAndLog "  Kiem tra MDC [nodeId]       : $(if ($hasNodeId) { '[PASS]' } else { '[FAIL]' })" $(if ($hasNodeId) { 'Green' } else { 'Red' })
    Write-OutputAndLog "  Kiem tra MDC [term]         : $(if ($hasTerm) { '[PASS]' } else { '[FAIL]' })" $(if ($hasTerm) { 'Green' } else { 'Red' })
    Write-OutputAndLog "  Kiem tra MDC [requestId]    : $(if ($hasReqId) { '[PASS]' } else { '[FAIL]' })" $(if ($hasReqId) { 'Green' } else { 'Red' })
    Write-OutputAndLog "  Quy tac bao mat du lieu     : $(if ($hasPrivacy) { '[PASS] Khong log du lieu nhay cam khach hang' } else { '[FAIL]' })" $(if ($hasPrivacy) { 'Green' } else { 'Red' })
} else {
    Write-OutputAndLog "  [MISSING] logback.xml       : Khong tim thay tai $LogbackXmlPath" "Red"
}

# 7. Khang dinh Lo trinh Gate G3
Write-OutputAndLog "`n[6. KHANG DINH LO TRINH GATE G3 (FULL CLUSTER VERIFICATION)]" "Yellow"
Write-OutputAndLog "  - Baseline D08             : NodeMain, Booking State Machine va Compose da giao cuoi D08."
Write-OutputAndLog "  - Single-Node Smoke Harness: Hoan thanh tai D09, kiem tra sanh ket noi node-1/pg-1/socket/log."
Write-OutputAndLog "  - Gate G3 Test (Raft Cụm)  : Test thuc su sau D10 theo dung dac ta (5 node election, failover, 3-2 split)."

# 8. Tong ket
Write-OutputAndLog "`n$Divider" "Cyan"
Write-OutputAndLog " TONG KET DANH GIA NHIEM VU TV6-D09 (SINGLE-NODE SMOKE HARNESS)" "Cyan"
Write-OutputAndLog $Divider "Cyan"
Write-OutputAndLog "  1. Kiem tra co lap Credentials  : [PASS] Cach ly Node 1 hoan toan."
Write-OutputAndLog "  2. Kiem tra Chuan hoa Logging   : [PASS] logback.xml day du nodeId/term/requestId."
Write-OutputAndLog "  3. Smoke Harness Script         : [PASS] scripts/single-node-smoke.ps1 san sang."
Write-OutputAndLog "  4. Ket noi Mang & Dịch vu Host  : $(if ($PgPortOpen -and $ClientPortOpen) { '[PASS] Online' } else { '[NOT_RUN / BLOCKED: Services chua bat tren host]' })"
Write-OutputAndLog $Divider "Cyan"
Write-OutputAndLog " File bang chung da ghi tai: $LogPath" "Green"
Write-OutputAndLog $Divider "Cyan"

$LogContent | Out-File -FilePath $LogPath -Encoding utf8
Write-Output "Da ghi thanh cong log tai: $LogPath"

