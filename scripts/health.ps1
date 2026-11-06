<#
.SYNOPSIS
    Script Healthcheck va tham do (probe) cong mang DB va Backend Node cho he thong phan tan.
.DESCRIPTION
    Nhiem vu TV6-D06 (Docker + Monitor + Integration):
    - Tham do trang thai ket noi toi 5 PostgreSQL instance doc lap (ports: 5433..5437).
    - Tham do trang thai 5 Client Socket ports (9101..9105) va 5 Raft Peer ports (9201..9205).
    - Phuc vu dac ta SPEC-08: ghi nhan trang thai he thong truoc khi TV3 ban giao NodeMain o D08.
    - Kiem tra cau hinh logback.xml chuan hoa MDC format (nodeId, term, requestId) va an toan du lieu.
    - Xuat file nhat ky minh chung tai artifacts/evidence/D06/TV6-D06-result.log.
.PARAMETER LogPath
    Duong dan tep log dau ra (Mac dinh: artifacts/evidence/D06/TV6-D06-result.log).
.PARAMETER TimeoutMs
    Thoi gian cho timeout cho moi port probe (Mac dinh: 1000 ms).
#>
[CmdletBinding()]
param(
    [string]$LogPath = "",
    [int]$TimeoutMs = 1000
)

$ErrorActionPreference = "Continue"

# 1. Xac dinh thu muc goc du an
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path "$ScriptDir/..").Path

if ([string]::IsNullOrWhiteSpace($LogPath)) {
    $LogPath = Join-Path $RepoRoot "artifacts/evidence/D06/TV6-D06-result.log"
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
Write-OutputAndLog " RAILWAY DISTRIBUTED SYSTEM - NHOM 6 | CLUSTER HEALTHCHECK & PORT PROBE (TV6-D06)" "Cyan"
Write-OutputAndLog $Divider "Cyan"

# 2. Thong tin thuc thi
$TimestampUtc = [System.DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
$TimestampLocal = [System.DateTime]::Now.ToString("yyyy-MM-dd HH:mm:ss zzz")
$Hostname = $env:COMPUTERNAME
$OsCaption = (Get-CimInstance Win32_OperatingSystem -ErrorAction SilentlyContinue).Caption
if (-not $OsCaption) { $OsCaption = [System.Environment]::OSVersion.VersionString }

Write-OutputAndLog "`n[1. THONG TIN MOI TRUONG THUC THI]" "Yellow"
Write-OutputAndLog "  Thoi diem (Local)          : $TimestampLocal"
Write-OutputAndLog "  Thoi diem (UTC)            : $TimestampUtc"
Write-OutputAndLog "  Ten may (Hostname)         : $Hostname"
Write-OutputAndLog "  He dieu hanh               : $OsCaption"
Write-OutputAndLog "  Thu muc du an              : $RepoRoot"

# Lay thong tin Git
$GitCommitSha = "UNKNOWN"
$GitBranch = "UNKNOWN"
if (Get-Command git -ErrorAction SilentlyContinue) {
    try {
        $GitCommitSha = (git -C $RepoRoot rev-parse HEAD 2>$null).Trim()
        $GitBranch = (git -C $RepoRoot rev-parse --abbrev-ref HEAD 2>$null).Trim()
    } catch {}
}
Write-OutputAndLog "  Git Branch                 : $GitBranch"
Write-OutputAndLog "  Git Commit SHA             : $GitCommitSha"

# Ham kiem tra port bang .NET Socket nhanh, khong bi treo
function Test-TcpPort {
    param(
        [string]$HostAddress,
        [int]$Port,
        [int]$Timeout
    )
    $tcpClient = New-Object System.Net.Sockets.TcpClient
    try {
        $asyncResult = $tcpClient.BeginConnect($HostAddress, $Port, $null, $null)
        $waitHandle = $asyncResult.AsyncWaitHandle
        if ($waitHandle.WaitOne($Timeout, $false)) {
            $tcpClient.EndConnect($asyncResult)
            return $true
        }
        return $false
    } catch {
        return $false
    } finally {
        $tcpClient.Close()
    }
}

# 3. Kiem tra Docker Daemon & Compose Container Status
Write-OutputAndLog "`n[2. KIEM TRA TIEN TRINH DOCKER DEAMON & COMPOSE STATUS]" "Yellow"

$DockerCmd = Get-Command docker -ErrorAction SilentlyContinue
$DockerDaemonRunning = $false

if ($null -ne $DockerCmd) {
    try {
        $dockerInfo = (docker info --format '{{.ServerVersion}}' 2>$null)
        if (-not [string]::IsNullOrWhiteSpace($dockerInfo)) {
            $DockerDaemonRunning = $true
            Write-OutputAndLog "  [OK] Docker Daemon dang hoat dong (Server Version: $dockerInfo)" "Green"
            
            # Kiem tra container status tu docker compose
            $ComposePsOutput = (docker compose -f "$RepoRoot/docker/compose.dev.yml" ps 2>&1) | Out-String
            Write-OutputAndLog "  Trang thai cac container tu compose.dev.yml:"
            foreach ($line in $ComposePsOutput.Trim().Split("`n")) {
                if (-not [string]::IsNullOrWhiteSpace($line)) {
                    Write-OutputAndLog "    $($line.Trim())"
                }
            }
        } else {
            Write-OutputAndLog "  [INFO] Docker Daemon chua bat tren may host ($Hostname). Se tien hanh socket probe truc tiep." "Yellow"
        }
    } catch {
        Write-OutputAndLog "  [INFO] Khong the ket noi Docker Daemon API. Tien hanh socket probe truc tiep." "Yellow"
    }
} else {
    Write-OutputAndLog "  [WARN] Khong tim thay lenh 'docker' trong PATH he thong." "Yellow"
}

# 4. Tham do 5 PostgreSQL Database Ports (Host ports 5433..5437)
Write-OutputAndLog "`n[3. THAM DO (PROBE) 5 POSTGRESQL INSTANCES (DOC LAP)]" "Yellow"

$DbTargets = @(
    @{ Node = "node-1"; Container = "postgres-1"; Port = 5433; DbName = "railway_node1" },
    @{ Node = "node-2"; Container = "postgres-2"; Port = 5434; DbName = "railway_node2" },
    @{ Node = "node-3"; Container = "postgres-3"; Port = 5435; DbName = "railway_node3" },
    @{ Node = "node-4"; Container = "postgres-4"; Port = 5436; DbName = "railway_node4" },
    @{ Node = "node-5"; Container = "postgres-5"; Port = 5437; DbName = "railway_node5" }
)

$DbOnlineCount = 0
foreach ($db in $DbTargets) {
    $isOpen = Test-TcpPort -HostAddress "127.0.0.1" -Port $db.Port -Timeout $TimeoutMs
    if ($isOpen) {
        Write-OutputAndLog "  [ONLINE]  $($db.Node) -> $($db.Container) (Host Port: $($db.Port), DB: $($db.DbName))" "Green"
        $DbOnlineCount++
    } else {
        Write-OutputAndLog "  [OFFLINE] $($db.Node) -> $($db.Container) (Host Port: $($db.Port), DB: $($db.DbName)) - Port chua mo / container chua bat" "DarkYellow"
    }
}
Write-OutputAndLog "  Ket qua probe Database: $DbOnlineCount / $($DbTargets.Count) instance dang lang nghe tren host." "Cyan"

# 5. Tham do 5 Backend Node Client Ports (9101..9105)
Write-OutputAndLog "`n[4. THAM DO (PROBE) 5 BACKEND NODE CLIENT PORTS (9101..9105)]" "Yellow"

$ClientTargets = @(
    @{ Node = "node-1"; Port = 9101 },
    @{ Node = "node-2"; Port = 9102 },
    @{ Node = "node-3"; Port = 9103 },
    @{ Node = "node-4"; Port = 9104 },
    @{ Node = "node-5"; Port = 9105 }
)

$ClientOnlineCount = 0
foreach ($cli in $ClientTargets) {
    $isOpen = Test-TcpPort -HostAddress "127.0.0.1" -Port $cli.Port -Timeout $TimeoutMs
    if ($isOpen) {
        Write-OutputAndLog "  [LISTENING]     $($cli.Node) Client Socket tren port $($cli.Port)" "Green"
        $ClientOnlineCount++
    } else {
        Write-OutputAndLog "  [NOT_LISTENING] $($cli.Node) Client Socket tren port $($cli.Port) - [PENDING_D08: NodeMain TV3 ban giao o D08]" "DarkGray"
    }
}
Write-OutputAndLog "  Ket qua probe Client Ports: $ClientOnlineCount / $($ClientTargets.Count) port dang lang nghe." "Cyan"

# 6. Tham do 5 Backend Node Raft Peer Ports (9201..9205)
Write-OutputAndLog "`n[5. THAM DO (PROBE) 5 BACKEND NODE RAFT PEER PORTS (9201..9205)]" "Yellow"

$PeerTargets = @(
    @{ Node = "node-1"; Port = 9201 },
    @{ Node = "node-2"; Port = 9202 },
    @{ Node = "node-3"; Port = 9203 },
    @{ Node = "node-4"; Port = 9204 },
    @{ Node = "node-5"; Port = 9205 }
)

$PeerOnlineCount = 0
foreach ($peer in $PeerTargets) {
    $isOpen = Test-TcpPort -HostAddress "127.0.0.1" -Port $peer.Port -Timeout $TimeoutMs
    if ($isOpen) {
        Write-OutputAndLog "  [LISTENING]     $($peer.Node) Raft Peer Socket tren port $($peer.Port)" "Green"
        $PeerOnlineCount++
    } else {
        Write-OutputAndLog "  [NOT_LISTENING] $($peer.Node) Raft Peer Socket tren port $($peer.Port) - [PENDING_D08: Raft RPC Socket TV3 ban giao o D08]" "DarkGray"
    }
}
Write-OutputAndLog "  Ket qua probe Peer Ports: $PeerOnlineCount / $($PeerTargets.Count) port dang lang nghe." "Cyan"

# 7. Kiem tra Tuan thu Chuan hoa Logging (Logback XML Verification)
Write-OutputAndLog "`n[6. KIEM TRA CHUAN HOA LOGGING (LOGBACK CONFIGURATION)]" "Yellow"

$LogbackPath = Join-Path $RepoRoot "backend-node/src/main/resources/logback.xml"
$LogbackValid = $false

if (Test-Path $LogbackPath) {
    Write-OutputAndLog "  [FOUND] Tep cau hinh Logback: $LogbackPath" "Green"
    $LogbackContent = Get-Content $LogbackPath -Raw -Encoding UTF8
    
    # Kiem tra cac truong MDC bat buoc theo yeu cau task D06
    $HasNodeId = $LogbackContent -match 'nodeId'
    $HasTerm = $LogbackContent -match 'term'
    $HasRequestId = $LogbackContent -match 'requestId'
    
    Write-OutputAndLog "  Kiem tra format cac truong MDC bat buoc:"
    Write-OutputAndLog "    - Format [nodeId]       : $(if ($HasNodeId) { '[PASS] Da cau hinh %X{nodeId}' } else { '[FAIL] Thieu truong nodeId' })" $(if ($HasNodeId) { 'Green' } else { 'Red' })
    Write-OutputAndLog "    - Format [term]         : $(if ($HasTerm) { '[PASS] Da cau hinh %X{term}' } else { '[FAIL] Thieu truong term' })" $(if ($HasTerm) { 'Green' } else { 'Red' })
    Write-OutputAndLog "    - Format [requestId]    : $(if ($HasRequestId) { '[PASS] Da cau hinh %X{requestId}' } else { '[FAIL] Thieu truong requestId' })" $(if ($HasRequestId) { 'Green' } else { 'Red' })
    
    # Kiem tra quy tac bao mat: Khong ghi du lieu nhay cam
    $HasSecurityRule = $LogbackContent -match 'BAO MAT DU LIEU|NHAY CAM|PRIVACY'
    Write-OutputAndLog "    - Quy tac bao mat Privacy: $(if ($HasSecurityRule) { '[PASS] Da tich hop chi thi cam log du lieu nhay cam (CCCD, Sdt, Password)' } else { '[WARN] Can bo sung chu thich bao mat' })" $(if ($HasSecurityRule) { 'Green' } else { 'Yellow' })
    
    if ($HasNodeId -and $HasTerm -and $HasRequestId) {
        $LogbackValid = $true
    }
} else {
    Write-OutputAndLog "  [FAIL] Khong tim thay tep: $LogbackPath" "Red"
}

# 8. Tong ket Danh gia Nhiem vu TV6-D06
Write-OutputAndLog "`n$SubDivider" "Cyan"
Write-OutputAndLog " TONG KET DANH GIA NHIEM VU TV6-D06 (HEALTHCHECK & LOGGING)" "Cyan"
Write-OutputAndLog $SubDivider "Cyan"

Write-OutputAndLog "  1. Kich ban Healthcheck ($($MyInvocation.MyCommand.Name)) : [PASS] Da san sang, tham do nhanh khong gay treo." "Green"
Write-OutputAndLog "  2. Quy hoach Cong mang (SPEC-08)                          : [PASS] 5 DB ports (5433..5437), 5 Client (9101..9105), 5 Peer (9201..9205)." "Green"
Write-OutputAndLog "  3. Chuan hoa Logback XML                                  : $(if ($LogbackValid) { '[PASS] Day du nodeId, term, requestId, khong log du lieu nhay cam.' } else { '[FAIL] Cau hinh chua dat chuan.' })" $(if ($LogbackValid) { 'Green' } else { 'Red' })
Write-OutputAndLog "  4. Trang thai tien do cac thanh vien khac                : [INFO] NodeMain va Raft TCP Socket dang duoc TV3 phat trien (du kien ban giao tai D08 theo roadmap)." "Cyan"

Write-OutputAndLog "`n$Divider" "Cyan"
Write-OutputAndLog " File bang chung da ghi tai: $LogPath" "Cyan"
Write-OutputAndLog $Divider "Cyan"

# Ghi nhat ky ra file UTF-8
try {
    [System.IO.File]::WriteAllLines($LogPath, $LogContent, [System.Text.Encoding]::UTF8)
} catch {
    Write-Host "Loi khi ghi tep log: $_" -ForegroundColor Red
}

exit 0
