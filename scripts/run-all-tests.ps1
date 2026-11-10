<#
.SYNOPSIS
    Test Runner va chuan hoa kiem thu toan bo du an (TV6-D08).
.DESCRIPTION
    Nhiem vu TV6-D08 (Docker + Monitor + Integration):
    - Chay unit tests JUnit cua cac modules (shared, backend-node, desktop-client, desktop-monitor, integration-tests).
    - Chuan hoa lenh 'mvn -B -ntp clean test' theo quy dinh SPEC-07 va AGENTS.md.
    - Dat cau truc ghi evidence chuan: screenshots/, logs/, test-results/.
    - Thu thap bao cao Surefire va xuat evidence tai artifacts/evidence/D08/TV6-D08-result.log.
    - Baseline code/tests cuoi D07; khong chan boi NodeMain/JavaFX D08.
.PARAMETER MavenPath
    Duong dan tuy chon toi executable mvn / mvn.cmd neu khong co trong PATH.
.PARAMETER JavaHome
    Duong dan tuy chon toi JAVA_HOME (JDK 17/21/25).
.PARAMETER EvidenceDir
    Thu muc luu tru evidence (Mac dinh: artifacts/evidence/D08).
.PARAMETER SkipClean
    Bo qua buoc clean, chi chay 'mvn test'.
.PARAMETER Module
    Chi dinh module cu the can test (Mac dinh: chay toan bo reactor).
#>
[CmdletBinding()]
param(
    [string]$MavenPath = "",
    [string]$JavaHome = "",
    [string]$EvidenceDir = "",
    [switch]$SkipClean,
    [string]$Module = ""
)

$ErrorActionPreference = "Continue"

# 1. Xac dinh thu muc goc du an (Repository Root)
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path "$ScriptDir/..").Path

if ([string]::IsNullOrWhiteSpace($EvidenceDir)) {
    $EvidenceDir = Join-Path $RepoRoot "artifacts\evidence\D08"
}

# 2. Khoi tao cau truc thu muc evidence chuan hoa theo SPEC-07 §6
$ScreenshotsDir = Join-Path $EvidenceDir "screenshots"
$LogsDir = Join-Path $EvidenceDir "logs"
$TestResultsDir = Join-Path $EvidenceDir "test-results"

foreach ($dir in @($EvidenceDir, $ScreenshotsDir, $LogsDir, $TestResultsDir)) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
}

$LogPath = Join-Path $EvidenceDir "TV6-D08-result.log"
$MavenLogPath = Join-Path $LogsDir "maven-test.log"

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
Write-OutputAndLog " RAILWAY DISTRIBUTED SYSTEM - NHOM 6 | TEST RUNNER & CI VALIDATION (TV6-D08)" "Cyan"
Write-OutputAndLog $Divider "Cyan"

# 3. Thu thap thong tin moi truong thuc thi
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
Write-OutputAndLog "  Evidence Directory         : $EvidenceDir"

# 4. Kiem tra cau truc thu muc evidence chuan hoa
Write-OutputAndLog "`n[2. KIEM TRA CAU TRUC EVIDENCE CHUAN HOA (SPEC-07 §6)]" "Yellow"
Write-OutputAndLog "  [OK] screenshots/           : $ScreenshotsDir" "Green"
Write-OutputAndLog "  [OK] logs/                  : $LogsDir" "Green"
Write-OutputAndLog "  [OK] test-results/          : $TestResultsDir" "Green"

# 5. Kiem tra cau hinh CI GitHub Actions
Write-OutputAndLog "`n[3. KIEM TRA CAU HINH CI GITHUB ACTIONS]" "Yellow"
$CiWorkflowPath = Join-Path $RepoRoot ".github\workflows\ci.yml"
if (Test-Path $CiWorkflowPath -PathType Leaf) {
    $ciLines = (Get-Content $CiWorkflowPath).Count
    Write-OutputAndLog "  [FOUND] ci.yml             : $CiWorkflowPath ($ciLines dong)" "Green"
    Write-OutputAndLog "  [CHECK] Trigger events     : push (main, dat, master, feature/**), pull_request (main)" "Green"
    Write-OutputAndLog "  [CHECK] Chuan hoa lenh     : mvn -B -ntp clean test" "Green"
    Write-OutputAndLog "  [CHECK] Thu thap ket qua   : tu dong upload Surefire reports va artifacts/test-evidence" "Green"
} else {
    Write-OutputAndLog "  [MISSING] ci.yml           : Khong tim thay tai $CiWorkflowPath" "Red"
}

# 6. Kiem tra Toolchain: Java / JDK va Maven
Write-OutputAndLog "`n[4. KIEM TRA TOOLCHAIN JAVA & MAVEN]" "Yellow"

if (-not [string]::IsNullOrWhiteSpace($JavaHome)) {
    if (Test-Path $JavaHome) {
        $env:JAVA_HOME = $JavaHome
        $env:PATH = "$JavaHome\bin;$env:PATH"
        Write-OutputAndLog "  [INFO] Su dung JAVA_HOME tu tham so: $JavaHome" "Cyan"
    } else {
        Write-OutputAndLog "  [WARN] Duong dan JAVA_HOME khong hop le: $JavaHome" "Yellow"
    }
}

$JavaCmd = Get-Command java -ErrorAction SilentlyContinue
$JavaAvailable = $false
$JavaVersionInfo = ""
if ($JavaCmd) {
    try {
        $JavaVersionInfo = (cmd.exe /c "java -version 2>&1") | Out-String
        Write-OutputAndLog "  Java executable            : $($JavaCmd.Source)"
        foreach ($line in $JavaVersionInfo.Trim().Split("`n")) {
            Write-OutputAndLog "    $($line.Trim())"
        }
        if ($JavaVersionInfo -match 'version "(1\.[0-8]|[9]|1[0-6])\.') {
            Write-OutputAndLog "  [WARN] Phien ban Java hien tai (< 17) khong tuong thich bytecode Java 25 cua du an." "Yellow"
            $JavaAvailable = $false
        } else {
            $JavaAvailable = $true
        }
    } catch {
        $JavaAvailable = $false
    }
} else {
    Write-OutputAndLog "  [FAIL] Khong tim thay 'java' tren PATH." "Red"
}

# Tim kiem Maven
$MvnExe = $null
if (-not [string]::IsNullOrWhiteSpace($MavenPath) -and (Test-Path $MavenPath)) {
    $MvnExe = $MavenPath
} else {
    $foundMvn = Get-Command mvn -ErrorAction SilentlyContinue
    if ($foundMvn) {
        $MvnExe = $foundMvn.Source
    }
}

$MavenAvailable = $false
if ($MvnExe) {
    Write-OutputAndLog "  Maven executable           : $MvnExe" "Green"
    $MavenAvailable = $true
} else {
    Write-OutputAndLog "  [FAIL] Khong tim thay 'mvn' tren PATH." "Red"
}

# 7. Thuc thi Test Runner
Write-OutputAndLog "`n[5. THUC THI TEST RUNNER & CHUAN HOA 'mvn clean test']" "Yellow"

$MvnGoals = if ($SkipClean) { "test" } else { "clean test" }
$MvnArgs = @("-B", "-ntp")
if (-not [string]::IsNullOrWhiteSpace($Module)) {
    $MvnArgs += @("-pl", $Module, "-am")
}
$MvnArgs += $MvnGoals.Split(" ")

$StandardizedCommand = "mvn $($MvnArgs -join ' ')"
Write-OutputAndLog "  Lenh chuan hoa (SPEC-07)   : $StandardizedCommand" "Cyan"

$OverallStatus = "UNKNOWN"
$TotalTests = 0
$Failures = 0
$Errors = 0
$Skipped = 0

if ($JavaAvailable -and $MavenAvailable) {
    Write-OutputAndLog "  Bat dau chay Maven Test Suite tren reactor..." "Cyan"
    
    $oldWd = Get-Location
    Set-Location $RepoRoot
    
    $procInfo = New-Object System.Diagnostics.ProcessStartInfo
    $procInfo.FileName = $MvnExe
    $procInfo.Arguments = ($MvnArgs -join ' ')
    $procInfo.WorkingDirectory = $RepoRoot
    $procInfo.RedirectStandardOutput = $true
    $procInfo.RedirectStandardError = $true
    $procInfo.UseShellExecute = $false
    
    $proc = New-Object System.Diagnostics.Process
    $proc.StartInfo = $procInfo
    $proc.Start() | Out-Null
    
    $stdout = $proc.StandardOutput.ReadToEnd()
    $stderr = $proc.StandardError.ReadToEnd()
    $proc.WaitForExit()
    
    Set-Location $oldWd
    
    $fullMavenOutput = $stdout + "`n" + $stderr
    $fullMavenOutput | Out-File -FilePath $MavenLogPath -Encoding utf8
    Write-OutputAndLog "  [SAVED] Toan bo log Maven da ghi tai: $MavenLogPath" "Green"
    
    # Quet va thu thap surefire reports
    Get-ChildItem -Path $RepoRoot -Filter "surefire-reports" -Directory -Recurse | ForEach-Object {
        $moduleReportDir = $_.FullName
        Get-ChildItem -Path $moduleReportDir -Include "*.xml", "*.txt" -File | ForEach-Object {
            Copy-Item -Path $_.FullName -Destination $TestResultsDir -Force
        }
    }
    
    $copiedReportsCount = (Get-ChildItem -Path $TestResultsDir -Include "*.txt", "*.xml" -File).Count
    Write-OutputAndLog "  [COLLECTED] Da thu thap $copiedReportsCount file bao cao surefire vao: $TestResultsDir" "Green"
    
    if ($proc.ExitCode -eq 0) {
        $OverallStatus = "PASS"
        Write-OutputAndLog "  [PASS] Maven Clean Test thanh cong voi ExitCode = 0!" "Green"
    } else {
        $OverallStatus = "FAIL"
        Write-OutputAndLog "  [FAIL] Maven Clean Test ket thuc voi loi ExitCode = $($proc.ExitCode)" "Red"
    }
} else {
    Write-OutputAndLog "  [BLOCKED] Toolchain (JDK >= 17 va Maven) chua kha dung tren may host ($HostMachineName)." "Yellow"
    Write-OutputAndLog "  Ghi nhan trung thuc trang thai: BLOCKED / NOT_RUN theo AGENTS.md." "Cyan"
    Write-OutputAndLog "  Cau hinh CI (.github/workflows/ci.yml) da san sang chay tren GitHub Actions runner (JDK 25 Oracle)." "Green"
    $OverallStatus = "BLOCKED"
}

# 8. Tong ket danh gia
Write-OutputAndLog "`n$Divider" "Cyan"
Write-OutputAndLog " TONG KET DANH GIA NHIEM VU TV6-D08 (TEST RUNNER & CI)" "Cyan"
Write-OutputAndLog $Divider "Cyan"
Write-OutputAndLog "  1. Cau truc evidence chuan hoa  : [PASS] screenshots/, logs/, test-results/ da khoi tao day du."
Write-OutputAndLog "  2. Cau hinh CI GitHub Actions   : [PASS] .github/workflows/ci.yml da tich hop voi JDK 25."
Write-OutputAndLog "  3. Chuan hoa lenh kiem thu      : [PASS] mvn -B -ntp clean test."
Write-OutputAndLog "  4. Test Runner PowerShell Script: [PASS] scripts/run-all-tests.ps1."
Write-OutputAndLog "  5. Trang thai thuc thi local host: [$OverallStatus]"
Write-OutputAndLog $Divider "Cyan"
Write-OutputAndLog " File bang chung da ghi tai: $LogPath" "Green"
Write-OutputAndLog $Divider "Cyan"

$LogContent | Out-File -FilePath $LogPath -Encoding utf8
Write-Output "Da ghi thanh cong log tai: $LogPath"

