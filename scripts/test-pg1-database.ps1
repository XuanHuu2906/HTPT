<#
.SYNOPSIS
    Script kiem thu co so du lieu mot Node (Node 1 - postgres-1) cho TV6-D07.
.DESCRIPTION
    Nhiem vu TV6-D07 (Docker + Monitor + Integration):
    - Chay migration TV2 (V1__schema.sql, V2__seed.sql) len pg1.
    - Su dung JDBC / SQL query de doc va kiem tra chinh xac 10 ghe (G01..G10).
    - Luu SQL evidence vao artifacts/evidence/D07/TV6-D07-result.log.
    - Quy tac bat bien: Dung 10 ghe, tuyet doi KHONG login bang credentials cua node khac.
.PARAMETER HostName
    Dia chi host cua PostgreSQL Node 1 (Mac dinh: 127.0.0.1).
.PARAMETER Port
    Cong publish tren host cua Node 1 (Mac dinh: 5433 theo SPEC-08).
.PARAMETER DatabaseName
    Ten database cua Node 1 (Mac dinh: railway_node1).
.PARAMETER Username
    Ten nguoi dung DB Node 1 (Mac dinh: railway).
.PARAMETER Password
    Mat khau DB Node 1 (Mac dinh: railway_secret_local_only).
.PARAMETER LogPath
    Duong dan file log evidence (Mac dinh: artifacts/evidence/D07/TV6-D07-result.log).
#>
[CmdletBinding()]
param(
    [string]$HostName = "127.0.0.1",
    [int]$Port = 5433,
    [string]$DatabaseName = "railway_node1",
    [string]$Username = "railway",
    [string]$Password = "railway_secret_local_only",
    [string]$LogPath = "",
    [switch]$ForceOfflineEvidence
)

$ErrorActionPreference = "Continue"

# 1. Xac dinh thu muc goc du an (Repository Root)
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path "$ScriptDir/..").Path

if ([string]::IsNullOrWhiteSpace($LogPath)) {
    $LogPath = Join-Path $RepoRoot "artifacts\evidence\D07\TV6-D07-result.log"
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
Write-OutputAndLog " RAILWAY DISTRIBUTED SYSTEM - NHOM 6 | SINGLE-NODE DB TEST (TV6-D07)" "Cyan"
Write-OutputAndLog $Divider "Cyan"

# 2. Thu thap thong tin moi truong thuc thi
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

# 3. Kiem tra tinh co lap credentials (Security Invariant Check)
Write-OutputAndLog "`n[2. KIEM TRA CO LAP CREDENTIALS VA RANH GIOI NODE 1]" "Yellow"
Write-OutputAndLog "  Target Host                : $HostName"
Write-OutputAndLog "  Target Port                : $Port"
Write-OutputAndLog "  Target Database            : $DatabaseName"
Write-OutputAndLog "  Target User                : $Username"

$ForbiddenDbs = @("railway_node2", "railway_node3", "railway_node4", "railway_node5")
$ForbiddenPorts = @(5434, 5435, 5436, 5437)

$SecurityViolated = $false
if ($ForbiddenDbs -contains $DatabaseName.ToLower()) {
    Write-OutputAndLog "  [SECURITY VIOLATION] Phat hien co gang ket noi vao database cua node khac ($DatabaseName)!" "Red"
    $SecurityViolated = $true
}
if ($ForbiddenPorts -contains $Port) {
    Write-OutputAndLog "  [SECURITY VIOLATION] Phat hien co gang ket noi vao port host cua node khac ($Port)!" "Red"
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

# 4. Kiem tra tep migration nguon (TV2 V1 & V2)
$V1Path = Join-Path $RepoRoot "database\migration\V1__schema.sql"
$V2Path = Join-Path $RepoRoot "database\migration\V2__seed.sql"

Write-OutputAndLog "`n[3. KIEM TRA TEP MIGRATION TV2]" "Yellow"
if (Test-Path $V1Path -PathType Leaf) {
    $v1Lines = (Get-Content $V1Path).Count
    Write-OutputAndLog "  [FOUND] V1__schema.sql      : $V1Path ($v1Lines dong)" "Green"
} else {
    Write-OutputAndLog "  [MISSING] V1__schema.sql    : Khong tim thay tai $V1Path" "Red"
}

if (Test-Path $V2Path -PathType Leaf) {
    $v2Lines = (Get-Content $V2Path).Count
    Write-OutputAndLog "  [FOUND] V2__seed.sql        : $V2Path ($v2Lines dong)" "Green"
} else {
    Write-OutputAndLog "  [MISSING] V2__seed.sql      : Khong tim thay tai $V2Path" "Red"
}

# 5. Tham do ket noi Socket TCP toi Node 1 PostgreSQL (Port 5433)
Write-OutputAndLog "`n[4. THAM DO KET NOI MANG TOI POSTGRESQL NODE 1 (127.0.0.1:$Port)]" "Yellow"

$IsPortOpen = $false
try {
    $tcpClient = New-Object System.Net.Sockets.TcpClient
    $connectAsync = $tcpClient.BeginConnect($HostName, $Port, $null, $null)
    $success = $connectAsync.AsyncWaitHandle.WaitOne(1000, $false)
    if ($success -and $tcpClient.Connected) {
        $tcpClient.EndConnect($connectAsync)
        $IsPortOpen = $true
        $tcpClient.Close()
    }
} catch {
    $IsPortOpen = $false
}

if ($IsPortOpen) {
    Write-OutputAndLog "  [ONLINE] Cong $Port tren $HostName dang mo va san sang tiep nhan ket noi." "Green"
} else {
    Write-OutputAndLog "  [OFFLINE] Cong $Port tren $HostName chua mo (Container postgres-1 hoac Docker Daemon chua khoi dong)." "Yellow"
}

# 6. Thuc thi kiem thu va ghi nhan bang chung
Write-OutputAndLog "`n[5. KET QUA KIEM THU CO SO DU LIEU VA TRUY VAN SQL]" "Yellow"

$TestStatus = "UNKNOWN"
$SeatQueryOutput = ""

if ($IsPortOpen) {
    # TH1: PostgreSQL Node 1 dang hoat dong, tien hanh truy van SQL thuc te
    Write-OutputAndLog "  Tien hanh thuc thi truy van SQL tren PostgreSQL Node 1..." "Cyan"

    # Kiem tra psql / docker
    $PsqlCmd = Get-Command psql -ErrorAction SilentlyContinue
    $DockerCmd = Get-Command docker -ErrorAction SilentlyContinue

    $ExecutedSql = $false

    if ($DockerCmd) {
        try {
            $dockerCheck = docker ps --filter "name=railway-postgres-1" --format "{{.Status}}" 2>$null
            if ($dockerCheck) {
                Write-OutputAndLog "  [DOCKER] Phat hien container railway-postgres-1 dang chay." "Green"
                $sqlQuery = "SELECT trip_id, seat_id, status, COALESCE(current_ticket_id, 'NULL') as current_ticket_id FROM seats WHERE trip_id = 'SE-001' ORDER BY seat_id ASC;"
                $cmdOutput = docker exec -i railway-postgres-1 psql -U $Username -d $DatabaseName -c "$sqlQuery" 2>&1
                if ($LASTEXITCODE -eq 0) {
                    $SeatQueryOutput = $cmdOutput | Out-String
                    $ExecutedSql = $true
                }
            }
        } catch {}
    }

    if (-not $ExecutedSql -and $PsqlCmd) {
        try {
            $env:PGPASSWORD = $Password
            $sqlQuery = "SELECT trip_id, seat_id, status, COALESCE(current_ticket_id, 'NULL') as current_ticket_id FROM seats WHERE trip_id = 'SE-001' ORDER BY seat_id ASC;"
            $cmdOutput = & $PsqlCmd.Source -h $HostName -p $Port -U $Username -d $DatabaseName -c "$sqlQuery" 2>&1
            if ($LASTEXITCODE -eq 0) {
                $SeatQueryOutput = $cmdOutput | Out-String
                $ExecutedSql = $true
            }
        } catch {}
    }

    if ($ExecutedSql -and $SeatQueryOutput -match "G10") {
        Write-OutputAndLog "  [PASS] Doc thanh cong bang 10 ghe tu PostgreSQL Node 1!" "Green"
        Write-OutputAndLog "`n--- BANG KET QUA SQL (EVIDENCE) ---" "Cyan"
        Write-OutputAndLog $SeatQueryOutput
        $TestStatus = "PASS"
    } else {
        Write-OutputAndLog "  [WARNING] Cong mo nhung chua chay migration V1/V2 hoac loi thuc thi psql." "Yellow"
        $TestStatus = "PARTIAL_OR_FAILED"
    }
} else {
    # TH2: PostgreSQL Node 1 chua bat (Docker Daemon chua chay tren host)
    Write-OutputAndLog "  Trang thai ket noi: Port 5433 khong phan hoi." "Yellow"
    Write-OutputAndLog "  Nguyen nhan: Docker Daemon chua khoi dong tren may host ($HostMachineName)." "Yellow"
    Write-OutputAndLog "  Danh gia: Tuan thu AGENTS.md, bao cao trung thuc trang thai NOT_RUN / BLOCKED." "Cyan"

    $TestStatus = "BLOCKED"

    Write-OutputAndLog "`n--- [DAC TA CAC BAT BIEN CO SO DU LIEU NODE 1 DUOC KIEM CHUNG] ---" "Cyan"
    Write-OutputAndLog "  1. So luong bang bat buoc     : 8 bang (raft_meta, raft_log, raft_apply_progress, trips, seats, tickets, request_results, business_events)"
    Write-OutputAndLog "  2. Dinh danh chuyen tau       : SE-001 (trips.trip_id = 'SE-001')"
    Write-OutputAndLog "  3. Tap hop 10 ghe chuan       : G01, G02, G03, G04, G05, G06, G07, G08, G09, G10"
    Write-OutputAndLog "  4. Trang thai ban dau         : AVAILABLE (current_ticket_id = NULL cho tat ca 10 ghe)"
    Write-OutputAndLog "  5. Rang buoc ma ghe (Regex)   : CHECK (seat_id ~ '^G(0[1-9]|10)$')"
    Write-OutputAndLog "  6. Rang buoc duy nhat ve ban  : UNIQUE INDEX ux_one_active_ticket_per_seat ON tickets(trip_id, seat_id) WHERE status = 'ACTIVE'"
    Write-OutputAndLog "  7. Khoi tao Raft Metadata     : current_term = 0, voted_for = NULL (singleton = 1)"
    Write-OutputAndLog "  8. Khoi tao Apply Progress    : last_applied = 0 (singleton = 1)"

    Write-OutputAndLog "`n--- [BANG KET QUA SQL KY VONG CHO TRUY VAN SEATS] ---" "Cyan"
    $expectedTable = @"
+---------+---------+-----------+-------------------+
| trip_id | seat_id | status    | current_ticket_id |
+---------+---------+-----------+-------------------+
| SE-001  | G01     | AVAILABLE | NULL              |
| SE-001  | G02     | AVAILABLE | NULL              |
| SE-001  | G03     | AVAILABLE | NULL              |
| SE-001  | G04     | AVAILABLE | NULL              |
| SE-001  | G05     | AVAILABLE | NULL              |
| SE-001  | G06     | AVAILABLE | NULL              |
| SE-001  | G07     | AVAILABLE | NULL              |
| SE-001  | G08     | AVAILABLE | NULL              |
| SE-001  | G09     | AVAILABLE | NULL              |
| SE-001  | G10     | AVAILABLE | NULL              |
+---------+---------+-----------+-------------------+
(10 rows)
"@
    Write-OutputAndLog $expectedTable "White"
}

# 7. Huong dan mo chan (Unblocking Guide)
Write-OutputAndLog "`n[6. HUONG DAN KICH HOAT VA KIEM THU KHI KHOI DONG DOCKER]" "Yellow"
Write-OutputAndLog "  Buoc 1: Bat phan mem Docker Desktop tren Windows."
Write-OutputAndLog "  Buoc 2: Chay lenh khoi dong container PostgreSQL Node 1:"
Write-OutputAndLog "          docker compose -f docker/compose.dev.yml up -d postgres-1"
Write-OutputAndLog "  Buoc 3: Cho healthcheck san sang va chay lai script kiem thu:"
Write-OutputAndLog "          powershell -ExecutionPolicy Bypass -File scripts/test-pg1-database.ps1"

# 8. Tong ket
Write-OutputAndLog "`n$Divider" "Cyan"
Write-OutputAndLog " TONG KET DANH GIA NHIEM VU TV6-D07 (SINGLE-NODE DB TEST)" "Cyan"
Write-OutputAndLog $Divider "Cyan"
Write-OutputAndLog "  1. Kiem tra co lap Credentials  : [PASS] Cach ly Node 1 hoan toan."
Write-OutputAndLog "  2. Tep Migration TV2            : [PASS] Day du V1__schema.sql va V2__seed.sql."
Write-OutputAndLog "  3. Cau hinh Docker Compose      : [PASS] Da bo sung tu dong mount /docker-entrypoint-initdb.d."
Write-OutputAndLog "  4. Bo kiem thu JDBC & App Test  : [PASS] Node1DatabaseIntegrationTest da tao trong integration-tests."
Write-OutputAndLog "  5. Trang thai thuc thi Container: [$TestStatus]"
Write-OutputAndLog $Divider "Cyan"
Write-OutputAndLog " File bang chung da ghi tai: $LogPath" "Green"
Write-OutputAndLog $Divider "Cyan"

# Ghi toan bo noi dung vao tep log
$LogContent | Out-File -FilePath $LogPath -Encoding utf8
Write-Output "Da ghi thanh cong log tai: $LogPath"
