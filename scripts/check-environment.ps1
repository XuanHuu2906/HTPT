<#
.SYNOPSIS
    Script kiem tra moi truong toolchain (JDK, Maven, Docker) va cau truc module Maven.
.DESCRIPTION
    Nhiem vu TV6-D01: Tao script kiem tra toolchain du an va chay Maven validate that.
    Tu dong ghi nhan thong tin OS, commit SHA, exit code va xuat log minh chung tai:
    artifacts/evidence/D01/TV6-D01-maven-validate.log
.PARAMETER LogPath
    Duong dan tep log dau ra (mac dinh: artifacts/evidence/D01/TV6-D01-maven-validate.log)
#>
[CmdletBinding()]
param(
    [string]$LogPath = ""
)

$ErrorActionPreference = "Continue"

# 1. Xac dinh thu muc goc du an (Repository Root)
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path "$ScriptDir/..").Path

if ([string]::IsNullOrWhiteSpace($LogPath)) {
    $LogPath = Join-Path $RepoRoot "artifacts/evidence/D01/TV6-D01-maven-validate.log"
}

# Dam bao thu muc luu log ton tai
$LogDir = Split-Path -Parent $LogPath
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

# Khoi tao log stream
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
Write-OutputAndLog " RAILWAY DISTRIBUTED SYSTEM - NHOM 6 | TOOLCHAIN VALIDATION (TV6-D01)" "Cyan"
Write-OutputAndLog $Divider "Cyan"

# 2. Thu thap thong tin he thong & Git
$TimestampUtc = [System.DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
$TimestampLocal = [System.DateTime]::Now.ToString("yyyy-MM-dd HH:mm:ss zzz")
$Hostname = $env:COMPUTERNAME
$OsCaption = (Get-CimInstance Win32_OperatingSystem -ErrorAction SilentlyContinue).Caption
if (-not $OsCaption) { $OsCaption = [System.Environment]::OSVersion.VersionString }
$PsVersion = $PSVersionTable.PSVersion.ToString()

Write-OutputAndLog "`n[1. HE THONG VA BAN MA NGUON]" "Yellow"
Write-OutputAndLog "  Thoi diem kiem tra (Local) : $TimestampLocal"
Write-OutputAndLog "  Thoi diem kiem tra (UTC)   : $TimestampUtc"
Write-OutputAndLog "  Ten may (Hostname)         : $Hostname"
Write-OutputAndLog "  He dieu hanh               : $OsCaption"
Write-OutputAndLog "  Phien ban PowerShell       : $PsVersion"
Write-OutputAndLog "  Thu muc du an (Repo Root)  : $RepoRoot"

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

# Bien theo doi trang thai tong the
$IsBlocked = $false
$BlockedReasons = [System.Collections.Generic.List[string]]::new()

# 3. Kiem tra Toolchain: Java / JDK
Write-OutputAndLog "`n[2. KIEM TRA JAVA / JDK]" "Yellow"
$JavaCmd = Get-Command java -ErrorAction SilentlyContinue
if ($null -eq $JavaCmd) {
    Write-OutputAndLog "  [FAIL] Khong tim thay lenh 'java' trong PATH he thong." "Red"
    $IsBlocked = $true
    $BlockedReasons.Add("Thieu Java runtime (lenh 'java' khong ton tai trong PATH)")
} else {
    Write-OutputAndLog "  [OK] Duong dan thuc thi : $($JavaCmd.Source)" "Green"
    $JavaVerOutput = (cmd.exe /c "java -version 2>&1") | Out-String
    Write-OutputAndLog "  Chi tiet phien ban:"
    foreach ($line in $JavaVerOutput.Trim().Split("`n")) {
        Write-OutputAndLog "    $($line.Trim())"
    }

    # Danh gia phien ban Java (Du an yeu cau toi thieu Java 17, de xuat Java 21 hoac 25 theo SPEC-08)
    if ($JavaVerOutput -match 'version "(1\.[0-8]|[9]|1[0-6])\.') {
        Write-OutputAndLog "  [CANH BAO BLOCKED] Phien ban Java qua cu (< 17). Can cai dat JDK 17/21/25 cho du an." "Red"
        $IsBlocked = $true
        $BlockedReasons.Add("Phien ban Java khong tuong thich (< 17, hien tai dang la Java 8). Can cai JDK 21 hoac 25.")
    } else {
        Write-OutputAndLog "  [OK] Phien ban Java thoa man dieu kien co ban." "Green"
    }
}

$JavaHomeEnv = [System.Environment]::GetEnvironmentVariable("JAVA_HOME")
Write-OutputAndLog "  Bien JAVA_HOME             : $(if ($JavaHomeEnv) { $JavaHomeEnv } else { '(Chua thiet lap)' })"

# 4. Kiem tra Toolchain: Apache Maven
Write-OutputAndLog "`n[3. KIEM TRA APACHE MAVEN]" "Yellow"
$MvnCmd = Get-Command mvn -ErrorAction SilentlyContinue
$HasMvn = $false
if ($null -eq $MvnCmd) {
    Write-OutputAndLog "  [FAIL] Khong tim thay lenh 'mvn' trong PATH he thong." "Red"
    $IsBlocked = $true
    $BlockedReasons.Add("Thieu Apache Maven (lenh 'mvn' khong ton tai trong PATH)")
} else {
    $HasMvn = $true
    Write-OutputAndLog "  [OK] Duong dan thuc thi : $($MvnCmd.Source)" "Green"
    $MvnVerOutput = (mvn -version 2>&1) | Out-String
    Write-OutputAndLog "  Chi tiet phien ban:"
    foreach ($line in $MvnVerOutput.Trim().Split("`n")) {
        Write-OutputAndLog "    $($line.Trim())"
    }
}

# 5. Kiem tra Toolchain: Docker CLI & Compose
Write-OutputAndLog "`n[4. KIEM TRA DOCKER CLI & COMPOSE]" "Yellow"
$DockerCmd = Get-Command docker -ErrorAction SilentlyContinue
if ($null -eq $DockerCmd) {
    Write-OutputAndLog "  [FAIL] Khong tim thay lenh 'docker' trong PATH he thong." "Red"
    $IsBlocked = $true
    $BlockedReasons.Add("Thieu Docker CLI (lenh 'docker' khong ton tai trong PATH)")
} else {
    Write-OutputAndLog "  [OK] Duong dan thuc thi : $($DockerCmd.Source)" "Green"
    $DockerVerOutput = (docker --version 2>&1) | Out-String
    Write-OutputAndLog "  Docker CLI Version         : $($DockerVerOutput.Trim())"

    $ComposeVerOutput = (docker compose version 2>&1) | Out-String
    if ($LASTEXITCODE -eq 0) {
        Write-OutputAndLog "  Docker Compose Version     : $($ComposeVerOutput.Trim())" "Green"
    } else {
        Write-OutputAndLog "  [CANH BAO] 'docker compose' khong kha dung hoac loi." "Yellow"
    }

    # Kiem tra Docker daemon co dang chay khong
    $DockerInfo = (docker info --format 'ServerVersion: {{.ServerVersion}}, Containers: {{.Containers}}' 2>$null)
    if ($DockerInfo) {
        Write-OutputAndLog "  Docker Daemon Status       : Dang chay ($DockerInfo)" "Green"
    } else {
        Write-OutputAndLog "  [CANH BAO] Docker Daemon chua khoi dong (Docker Desktop chua bat)." "Yellow"
    }
}

# 6. Kiem tra cau truc 5 Maven Modules
Write-OutputAndLog "`n[5. KIEM TRA CAU TRUC 5 MAVEN MODULES CUA REPOSITORY]" "Yellow"
$RequiredModules = @(
    @{ Name = "Root Reactor"; Path = "pom.xml" },
    @{ Name = "Shared (TV3)"; Path = "shared/pom.xml" },
    @{ Name = "Backend Node (TV2/4/5)"; Path = "backend-node/pom.xml" },
    @{ Name = "Desktop Client (TV1)"; Path = "desktop-client/pom.xml" },
    @{ Name = "Desktop Monitor (TV6)"; Path = "desktop-monitor/pom.xml" },
    @{ Name = "Integration Tests (TV6)"; Path = "integration-tests/pom.xml" }
)

foreach ($mod in $RequiredModules) {
    $FullPath = Join-Path $RepoRoot $mod.Path
    if (Test-Path $FullPath) {
        Write-OutputAndLog "  [FOUND] $($mod.Name) -> $($mod.Path)" "Green"
    } else {
        Write-OutputAndLog "  [MISSING] $($mod.Name) -> $($mod.Path) KHONG TON TAI!" "Red"
        $IsBlocked = $true
        $BlockedReasons.Add("Thieu module POM: $($mod.Path)")
    }
}

# 7. Thuc thi Maven Validate that (mvn -B -ntp validate)
Write-OutputAndLog "`n[6. THUC THI KIEM CHUNG CU PHAP: mvn -B -ntp validate]" "Yellow"
if ($HasMvn) {
    Write-OutputAndLog "  Dang chay lenh: 'mvn -B -ntp validate' tai $RepoRoot..." "Cyan"
    Push-Location $RepoRoot
    try {
        $MvnRunOutput = (mvn -B -ntp validate 2>&1)
        $MvnExitCode = $LASTEXITCODE
        foreach ($line in $MvnRunOutput) {
            Write-OutputAndLog "    $line"
        }
        Write-OutputAndLog "  Ma ket thuc (Exit Code)   : $MvnExitCode"
        if ($MvnExitCode -eq 0) {
            Write-OutputAndLog "  [PASS] Cau truc Maven Reactor hop le." "Green"
        } else {
            Write-OutputAndLog "  [FAIL] Maven validate that bai voi ma loi $MvnExitCode." "Red"
            $IsBlocked = $true
            $BlockedReasons.Add("Lenh 'mvn -B -ntp validate' that bai voi exit code $MvnExitCode")
        }
    } finally {
        Pop-Location
    }
} else {
    Write-OutputAndLog "  [SKIPPED] Bo qua kiem tra Maven validate vi may thieu lenh 'mvn'." "Red"
    Write-OutputAndLog "  Ma ket thuc (Exit Code)   : 127 (Command Not Found)"
}

# 8. Tong ket & Danh gia trang thai may
Write-OutputAndLog "`n$SubDivider" "Cyan"
Write-OutputAndLog " TONG KET TINH TRANG MAY (HOST: $Hostname)" "Cyan"
Write-OutputAndLog $SubDivider "Cyan"

if ($IsBlocked) {
    Write-OutputAndLog "`n>>> TRANG THAI MAY: [BLOCKED] <<<" "Red"
    Write-OutputAndLog "May chua san sang tham gia phat trien/kiem thu do cac nguyen nhan sau:" "Red"
    $i = 1
    foreach ($reason in $BlockedReasons) {
        Write-OutputAndLog "  $i. $reason" "Red"
        $i++
    }
    Write-OutputAndLog "`n[HUONG DAN MO CHAN (UNBLOCK)]:" "Yellow"
    Write-OutputAndLog "  1. Cai dat JDK 21 hoac JDK 25 va cau hinh JAVA_HOME, them thu muc bin vao PATH."
    Write-OutputAndLog "  2. Tai Apache Maven (vi du Maven 3.9+) va them thu muc bin vao PATH."
    Write-OutputAndLog "  3. Khoi dong Docker Desktop truoc khi chay cac lenh Docker."
    Write-OutputAndLog "  4. Chay lai script nay sau khi hoan tat thiet lap: .\scripts\check-environment.ps1"
} else {
    Write-OutputAndLog "`n>>> TRANG THAI MAY: [READY / PASS] <<<" "Green"
    Write-OutputAndLog "Moi truong toolchain (Java, Maven, Docker) va cau truc 5 module Maven deu dat chuan!" "Green"
}

Write-OutputAndLog "`n$Divider" "Cyan"
Write-OutputAndLog " File bang chung da ghi tai: $LogPath" "Cyan"
Write-OutputAndLog $Divider "Cyan"

# Ghi toan bo noi dung ra tep log UTF-8
[System.IO.File]::WriteAllLines($LogPath, $LogContent, [System.Text.Encoding]::UTF8)

if ($IsBlocked) {
    exit 1
} else {
    exit 0
}
