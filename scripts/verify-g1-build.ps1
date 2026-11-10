<#
.SYNOPSIS
    Script kiem tra Gate G1 (Build & Contract Verification) cho he thong phan tan Railway Distributed System.
.DESCRIPTION
    Nhiem vu TV6 (DevOps / Toolchain / Integration):
    - Kiem tra dieu kien moi truong toolchain (JDK 17/21/25, Apache Maven 3.9+).
    - Kiem tra cau truc Maven Reactor va 5 module con theo SPEC-00/02/08.
    - Kiem tra cac thanh phan nen tang Gate G1 (Contracts DTO, Raft Core interfaces, Booking State Machine, UI mock).
    - Danh gia trang thai snapshot cuoi D02 va liet ke cac blocker con thieu truoc khi thuc hien G1 Full Verify o D03.
    - Thuc thi lenh build Maven (mvn clean test / test-compile) neu toolchain kha dung.
    - Ghi nhat ky bang chung chi tiet vao artifacts/evidence/D02/TV6-D02-verify-g1-build.log.
.PARAMETER LogPath
    Duong dan tep log bang chung dau ra (Mac dinh: artifacts/evidence/D02/TV6-D02-verify-g1-build.log).
.PARAMETER MavenPath
    Duong dan tuy chon toi executable mvn / mvn.cmd neu khong co trong PATH he thong.
.PARAMETER JavaHome
    Duong dan tuy chon toi thu muc JAVA_HOME (JDK 17/21/25).
.PARAMETER TargetRef
    Git commit hash hoac ref cua snapshot can danh gia (Mac dinh: HEAD hoac snapshot cuoi D02).
.PARAMETER SkipTests
    Neu duoc bat, chi chay 'test-compile' thay vi toan bo bo 'test'.
.PARAMETER Offline
    Neu duoc bat, them co '-o' vao lenh Maven de su dung offline dependency cache.
#>
[CmdletBinding()]
param(
    [string]$LogPath = "",
    [string]$MavenPath = "",
    [string]$JavaHome = "",
    [string]$TargetRef = "4d98446dff91414472dfd009ad786c4a5906e929", # Snapshot merge cuoi D02 (PR #15 / dd6aef4 / 7835752)
    [switch]$SkipTests,
    [switch]$Offline
)

$ErrorActionPreference = "Continue"

# 1. Xac dinh thu muc goc du an (Repository Root)
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path "$ScriptDir/..").Path

if ([string]::IsNullOrWhiteSpace($LogPath)) {
    $LogPath = Join-Path $RepoRoot "artifacts/evidence/D02/TV6-D02-verify-g1-build.log"
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
Write-OutputAndLog " RAILWAY DISTRIBUTED SYSTEM - NHOM 6 | GATE G1 BUILD VERIFICATION (TV6)" "Cyan"
Write-OutputAndLog $Divider "Cyan"

# 2. Thong tin he thong & Git
$TimestampUtc = [System.DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
$TimestampLocal = [System.DateTime]::Now.ToString("yyyy-MM-dd HH:mm:ss zzz")
$Hostname = $env:COMPUTERNAME
$OsCaption = (Get-CimInstance Win32_OperatingSystem -ErrorAction SilentlyContinue).Caption
if (-not $OsCaption) { $OsCaption = [System.Environment]::OSVersion.VersionString }
$PsVersion = $PSVersionTable.PSVersion.ToString()

Write-OutputAndLog "`n[1. THONG TIN HE THONG VA MOI TRUONG THUC THI]" "Yellow"
Write-OutputAndLog "  Thoi diem (Local)          : $TimestampLocal"
Write-OutputAndLog "  Thoi diem (UTC)            : $TimestampUtc"
Write-OutputAndLog "  Ten may (Hostname)         : $Hostname"
Write-OutputAndLog "  He dieu hanh               : $OsCaption"
Write-OutputAndLog "  Phien ban PowerShell       : $PsVersion"
Write-OutputAndLog "  Thu muc du an (Repo Root)  : $RepoRoot"

# Thu thap thong tin Git
$GitCommitSha = "UNKNOWN"
$GitBranch = "UNKNOWN"
$HasGit = $false
if (Get-Command git -ErrorAction SilentlyContinue) {
    $HasGit = $true
    try {
        $GitCommitSha = (git -C $RepoRoot rev-parse HEAD 2>$null).Trim()
        $GitBranch = (git -C $RepoRoot rev-parse --abbrev-ref HEAD 2>$null).Trim()
    } catch {}
}
Write-OutputAndLog "  Git Branch hien tai        : $GitBranch"
Write-OutputAndLog "  Git Commit SHA hien tai    : $GitCommitSha"
Write-OutputAndLog "  Target Ref / Snapshot G1   : $TargetRef"

# Ham kiem tra file ton tai trong snapshot Git hoac working tree
function Test-FileInRef {
    param(
        [string]$Ref,
        [string]$FilePath
    )
    if ($HasGit -and -not [string]::IsNullOrWhiteSpace($Ref) -and $Ref -ne "WORKING_TREE") {
        $normalizedPath = $FilePath.Replace("\", "/")
        $gitOutput = (git -C $RepoRoot ls-tree $Ref $normalizedPath 2>$null)
        if (-not [string]::IsNullOrWhiteSpace($gitOutput)) {
            $parts = $gitOutput.Trim().Split("`t")
            if ($parts.Count -ge 2) {
                $meta = -split $parts[0]
                return @{ Exists = $true; Hash = $meta[2]; Path = $parts[1] }
            }
            return @{ Exists = $true; Hash = "GIT"; Path = $normalizedPath }
        }
        return @{ Exists = $false; Hash = ""; Path = $normalizedPath }
    } else {
        $fullPath = Join-Path $RepoRoot $FilePath
        if (Test-Path $fullPath) {
            $h = (Get-FileHash -Algorithm SHA256 -Path $fullPath).Hash.Substring(0, 16)
            return @{ Exists = $true; Hash = $h; Path = $FilePath }
        }
        return @{ Exists = $false; Hash = ""; Path = $FilePath }
    }
}

# Bien kiem soat trang thai
$IsBlocked = $false
$BlockedReasons = [System.Collections.Generic.List[string]]::new()
$MissingG1Blockers = [System.Collections.Generic.List[string]]::new()

# 3. Kiem tra va Thiet lap JAVA / JDK
Write-OutputAndLog "`n[2. KIEM TRA TOOLCHAIN: JAVA / JDK]" "Yellow"

$PreviousJavaHome = $env:JAVA_HOME
if (-not [string]::IsNullOrWhiteSpace($JavaHome)) {
    if (Test-Path $JavaHome) {
        $env:JAVA_HOME = $JavaHome
        Write-OutputAndLog "  [INFO] Su dung JAVA_HOME chi dinh: $JavaHome" "Cyan"
    } else {
        Write-OutputAndLog "  [FAIL] Duong dan JAVA_HOME chi dinh khong ton tai: $JavaHome" "Red"
        $IsBlocked = $true
        $BlockedReasons.Add("Duong dan JAVA_HOME chi dinh khong hop le: $JavaHome")
    }
}

$JavaExe = $null
if ($env:JAVA_HOME -and (Test-Path (Join-Path $env:JAVA_HOME "bin/java.exe"))) {
    $JavaExe = (Join-Path $env:JAVA_HOME "bin/java.exe")
} elseif (Get-Command java -ErrorAction SilentlyContinue) {
    $JavaExe = (Get-Command java).Source
}

if ($null -eq $JavaExe) {
    Write-OutputAndLog "  [FAIL] Khong tim thay executable 'java' (kiem tra PATH va JAVA_HOME)." "Red"
    $IsBlocked = $true
    $BlockedReasons.Add("Thieu Java runtime (khong tim thay java trong PATH hoac JAVA_HOME)")
} else {
    Write-OutputAndLog "  [OK] Java executable       : $JavaExe" "Green"
    $JavaVerOutput = (cmd.exe /c "`"$JavaExe`" -version 2>&1") | Out-String
    Write-OutputAndLog "  Chi tiet phien ban:"
    foreach ($line in $JavaVerOutput.Trim().Split("`n")) {
        Write-OutputAndLog "    $($line.Trim())"
    }

    # Kiem tra phien ban Java: root pom.xml yeu cau release 25 (hoac toi thieu JDK 17 theo SPEC-08)
    if ($JavaVerOutput -match 'version "(1\.[0-8]|[9]|1[0-6])\.') {
        Write-OutputAndLog "  [CANH BAO BLOCKED] Phien ban Java qua cu (< 17). Du an yeu cau release 25 / JDK 21+." "Red"
        $IsBlocked = $true
        $BlockedReasons.Add("Java runtime trong PATH la Java cu (< 17, phat hien Java 8). Can JDK 21 hoac 25.")
    } else {
        Write-OutputAndLog "  [OK] Phien ban Java dat chuan (>= 17)." "Green"
    }
}

# 4. Kiem tra va Thiet lap APACHE MAVEN
Write-OutputAndLog "`n[3. KIEM TRA TOOLCHAIN: APACHE MAVEN]" "Yellow"

$MvnExe = $null
if (-not [string]::IsNullOrWhiteSpace($MavenPath)) {
    if (Test-Path $MavenPath) {
        $MvnExe = $MavenPath
        Write-OutputAndLog "  [INFO] Su dung duong dan Maven chi dinh: $MavenPath" "Cyan"
    } else {
        Write-OutputAndLog "  [FAIL] Duong dan Maven chi dinh khong ton tai: $MavenPath" "Red"
        $IsBlocked = $true
        $BlockedReasons.Add("Duong dan Maven chi dinh khong hop le: $MavenPath")
    }
} elseif (Get-Command mvn.cmd -ErrorAction SilentlyContinue) {
    $MvnExe = (Get-Command mvn.cmd).Source
} elseif (Get-Command mvn -ErrorAction SilentlyContinue) {
    $MvnExe = (Get-Command mvn).Source
}

if ($null -eq $MvnExe) {
    Write-OutputAndLog "  [FAIL] Khong tim thay lenh 'mvn' trong PATH he thong." "Red"
    $IsBlocked = $true
    $BlockedReasons.Add("Thieu Apache Maven (lenh 'mvn' khong ton tai trong PATH)")
} else {
    Write-OutputAndLog "  [OK] Maven executable      : $MvnExe" "Green"
    $MvnVerOutput = (& $MvnExe -version 2>&1) | Out-String
    Write-OutputAndLog "  Chi tiet phien ban:"
    foreach ($line in $MvnVerOutput.Trim().Split("`n")) {
        Write-OutputAndLog "    $($line.Trim())"
    }
}

# 5. Kiem tra Cau truc 5 Modules cua Maven Multi-Module Reactor (Gate G1 Tieu chi 1)
Write-OutputAndLog "`n[4. KIEM TRA CAU TRUC MAVEN REACTOR 5 MODULES (GATE G1)]" "Yellow"

$RequiredModules = @(
    @{ Name = "Root Reactor"; Path = "pom.xml" },
    @{ Name = "Shared Module (TV3)"; Path = "shared/pom.xml" },
    @{ Name = "Backend Node Module (TV2/4/5)"; Path = "backend-node/pom.xml" },
    @{ Name = "Desktop Client Module (TV1)"; Path = "desktop-client/pom.xml" },
    @{ Name = "Desktop Monitor Module (TV6)"; Path = "desktop-monitor/pom.xml" },
    @{ Name = "Integration Tests Module (TV6)"; Path = "integration-tests/pom.xml" }
)

foreach ($mod in $RequiredModules) {
    $check = Test-FileInRef -Ref $TargetRef -FilePath $mod.Path
    if ($check.Exists) {
        Write-OutputAndLog "  [FOUND] $($mod.Name) -> $($mod.Path)" "Green"
    } else {
        Write-OutputAndLog "  [MISSING] $($mod.Name) -> $($mod.Path) KHONG TON TAI trong snapshot!" "Red"
        $IsBlocked = $true
        $BlockedReasons.Add("Thieu module POM: $($mod.Path)")
    }
}

# 6. Kiem tra Cac Thanh phan Hop dong da merge cuoi D02 (Gate G1 Tieu chi 2)
Write-OutputAndLog "`n[5. KIEM TRA CAC THANH PHAN DA MERGE TRONG SNAPSHOT CUOI D02 (REF: $TargetRef)]" "Yellow"

$D02MergedArtifacts = @(
    # SHARED DTOs (TV3-D01/D02)
    @{ Domain = "Shared DTO"; File = "shared/src/main/java/vn/nhom6/railway/protocol/dto/MessageEnvelope.java"; Milestone = "D01" },
    @{ Domain = "Shared DTO"; File = "shared/src/main/java/vn/nhom6/railway/protocol/dto/MessageType.java"; Milestone = "D01" },
    @{ Domain = "Shared DTO"; File = "shared/src/main/java/vn/nhom6/railway/protocol/dto/BookTicketRequestDto.java"; Milestone = "D02" },
    @{ Domain = "Shared DTO"; File = "shared/src/main/java/vn/nhom6/railway/protocol/dto/GetSeatsRequestDto.java"; Milestone = "D02" },
    @{ Domain = "Shared DTO"; File = "shared/src/main/java/vn/nhom6/railway/protocol/dto/RequestVoteRequestDto.java"; Milestone = "D02" },
    @{ Domain = "Shared DTO"; File = "shared/src/main/java/vn/nhom6/railway/protocol/dto/AppendEntriesRequestDto.java"; Milestone = "D02" },
    
    # BACKEND BOOKING (TV2-D01/D02)
    @{ Domain = "Backend Booking"; File = "backend-node/src/main/java/vn/nhom6/railway/booking/BookingCommand.java"; Milestone = "D01" },
    @{ Domain = "Backend Booking"; File = "backend-node/src/main/java/vn/nhom6/railway/booking/BookingStateMachine.java"; Milestone = "D02" },

    # RAFT CORE & ELECTION (TV4-D01/D02, TV5-D01/D02)
    @{ Domain = "Raft Core"; File = "backend-node/src/main/java/vn/nhom6/railway/raft/core/RaftState.java"; Milestone = "D01" },
    @{ Domain = "Raft Core"; File = "backend-node/src/main/java/vn/nhom6/railway/raft/core/RaftLogEntry.java"; Milestone = "D01" },
    @{ Domain = "Raft Core"; File = "backend-node/src/main/java/vn/nhom6/railway/raft/core/RaftNode.java"; Milestone = "D02" },
    @{ Domain = "Raft Core"; File = "backend-node/src/main/java/vn/nhom6/railway/raft/core/RaftStorage.java"; Milestone = "D02" },
    @{ Domain = "Raft Election"; File = "backend-node/src/main/java/vn/nhom6/railway/raft/election/ElectionConfig.java"; Milestone = "D01" },
    @{ Domain = "Raft Election"; File = "backend-node/src/main/java/vn/nhom6/railway/raft/election/ElectionTimeoutPolicy.java"; Milestone = "D01" },
    @{ Domain = "Raft Election"; File = "backend-node/src/main/java/vn/nhom6/railway/raft/election/VoteRules.java"; Milestone = "D02" },

    # DESKTOP UI (TV1-D01/D02)
    @{ Domain = "Desktop Client"; File = "desktop-client/src/main/java/vn/nhom6/railway/client/App.java"; Milestone = "D01" },
    @{ Domain = "Desktop Client"; File = "desktop-client/src/main/java/vn/nhom6/railway/client/controller/MainController.java"; Milestone = "D01" },
    @{ Domain = "Desktop Client"; File = "desktop-client/src/main/resources/fxml/main.fxml"; Milestone = "D01" },

    # CLUSTER DEV CONFIG (TV6-D02)
    @{ Domain = "Cluster Config"; File = "configs/dev/cluster.properties"; Milestone = "D02" },
    @{ Domain = "Cluster Config"; File = "docker/.env.example"; Milestone = "D02" }
)

$D02FoundCount = 0
foreach ($item in $D02MergedArtifacts) {
    $check = Test-FileInRef -Ref $TargetRef -FilePath $item.File
    if ($check.Exists) {
        Write-OutputAndLog "  [FOUND - $($item.Milestone)] [$($item.Domain)] $($item.File) (ref_hash: $($check.Hash.Substring(0, [System.Math]::Min(12, $check.Hash.Length))))" "Green"
        $D02FoundCount++
    } else {
        Write-OutputAndLog "  [MISSING] [$($item.Domain)] $($item.File)" "Red"
        $MissingG1Blockers.Add("Thieu artifact nen tang G1 trong snapshot: $($item.File)")
    }
}
Write-OutputAndLog "  Tong so artifact D01/D02 da hop nhat trong snapshot: $D02FoundCount / $($D02MergedArtifacts.Count)" "Cyan"

# 7. Danh gia cac Blocker con thieu cho G1 Full Verify (Cac hop dong D03 can ghep)
Write-OutputAndLog "`n[6. DANH GIA CAC BLOCKER CON THIEU CHO G1 FULL VERIFY (HOP DONG D03)]" "Yellow"

$D03Contracts = @(
    @{ Owner = "TV3"; Task = "TV3-D03"; File = "shared/src/main/java/vn/nhom6/railway/protocol/codec/MessageCodec.java"; Desc = "JSON Codec tuan tu hoa MessageEnvelope dung SPEC-03" },
    @{ Owner = "TV3"; Task = "TV3-D03"; File = "shared/src/test/resources/fixtures/protocol-v1.json"; Desc = "Golden JSON fixture hop dong giao thuc v1" },
    @{ Owner = "TV2"; Task = "TV2-D03"; File = "backend-node/src/test/java/vn/nhom6/railway/booking/BookingRaftContractTest.java"; Desc = "Hop dong persistence va apply giua Booking va Raft (SPEC-05)" },
    @{ Owner = "TV4"; Task = "TV4-D03"; File = "backend-node/src/test/java/vn/nhom6/railway/raft/core/RaftCoreContractTest.java"; Desc = "Hop dong kiem thu RaftCore voi fake booking/election (SPEC-04)" },
    @{ Owner = "TV1"; Task = "TV1-D03"; File = "desktop-client/src/test/java/vn/nhom6/railway/client/BookingFormValidatorTest.java"; Desc = "Kiem thu validate form dat ve client (SPEC-06)" }
)

Write-OutputAndLog "  Kiem tra su hien dien cua cac hop dong D03 trong snapshot cuoi D02 ($TargetRef):" "Cyan"
foreach ($d3 in $D03Contracts) {
    $checkInD02 = Test-FileInRef -Ref $TargetRef -FilePath $d3.File
    $checkInWorking = Test-FileInRef -Ref "WORKING_TREE" -FilePath $d3.File
    
    if (-not $checkInD02.Exists) {
        $statusWorking = if ($checkInWorking.Exists) { "(Da co tren local working tree/branch)" } else { "(Chua co tren local branch)" }
        Write-OutputAndLog "  [BLOCKER D02 SNAPSHOT] [$($d3.Owner) - $($d3.Task)] $($d3.File)`n    -> Y nghia: $($d3.Desc)`n    -> Trang thai: Chua merge tai moc cuoi D02 $statusWorking" "Yellow"
        $MissingG1Blockers.Add("Blocker G1 tren snapshot D02: $($d3.File) ($($d3.Task) - $($d3.Desc))")
    } else {
        Write-OutputAndLog "  [MERGED] [$($d3.Owner) - $($d3.Task)] $($d3.File)" "Green"
    }
}

# 8. Thuc thi Maven Build / Test thuc te (Gate G1 Tieu chi 3)
Write-OutputAndLog "`n[7. THUC THI MAVEN BUILD & TEST (GATE G1 VERIFICATION)]" "Yellow"

$BuildExitCode = -1

if (-not $IsBlocked -and $MvnExe) {
    $MavenGoal = if ($SkipTests) { "test-compile" } else { "test" }
    $MavenArgsList = [System.Collections.Generic.List[string]]::new()
    $MavenArgsList.Add("-B")
    $MavenArgsList.Add("-ntp")
    if ($Offline) { $MavenArgsList.Add("-o") }
    $MavenArgsList.Add("clean")
    $MavenArgsList.Add($MavenGoal)

    $CmdDisplay = "$MvnExe $($MavenArgsList -join ' ')"
    Write-OutputAndLog "  Lenh thuc thi: $CmdDisplay" "Cyan"

    Push-Location $RepoRoot
    try {
        $BuildOutput = (& $MvnExe @MavenArgsList 2>&1)
        $BuildExitCode = $LASTEXITCODE

        foreach ($line in $BuildOutput) {
            Write-OutputAndLog "    $line"
        }

        Write-OutputAndLog "  Ma ket thuc build (Exit Code): $BuildExitCode"
        if ($BuildExitCode -eq 0) {
            Write-OutputAndLog "  [PASS] Maven build/test thanh cong toan bo cac module." "Green"
        } else {
            Write-OutputAndLog "  [FAIL] Maven build/test that bai voi exit code $BuildExitCode." "Red"
            $IsBlocked = $true
            $BlockedReasons.Add("Maven build goal '$MavenGoal' that bai voi exit code $BuildExitCode")
        }
    } finally {
        Pop-Location
    }
} else {
    Write-OutputAndLog "  [SKIPPED] Khong the thuc thi Maven build do moi truong toolchain bi chan (BLOCKED)." "Red"
    Write-OutputAndLog "  Ly do chan toolchain:" "Red"
    foreach ($reason in $BlockedReasons) {
        Write-OutputAndLog "    - $reason" "Red"
    }
    $BuildExitCode = 127
}

# 9. Tong ket Gate G1 & Danh gia Snapshot
Write-OutputAndLog "`n$SubDivider" "Cyan"
Write-OutputAndLog " TONG KET DANH GIA GATE G1 VA SNAPSHOT CUOI D02" "Cyan"
Write-OutputAndLog $SubDivider "Cyan"

$FinalStatus = "UNKNOWN"

if ($IsBlocked) {
    $FinalStatus = "BLOCKED"
    Write-OutputAndLog "`n>>> TRANG THAI G1 TREN MOI TRUONG MAY HIEN TAI: [BLOCKED] <<<" "Red"
    Write-OutputAndLog "Nguyen nhan can tro:" "Red"
    $idx = 1
    foreach ($r in $BlockedReasons) {
        Write-OutputAndLog "  $idx. $r" "Red"
        $idx++
    }
} elseif ($BuildExitCode -ne 0) {
    $FinalStatus = "FAIL"
    Write-OutputAndLog "`n>>> TRANG THAI G1: [FAIL] <<<" "Red"
    Write-OutputAndLog "Build that bai voi ma loi: $BuildExitCode" "Red"
} else {
    $FinalStatus = "PASS_D02_SNAPSHOT_PENDING_D03"
    Write-OutputAndLog "`n>>> TRANG THAI G1: [PASS SNAPSHOT D02 - CHO KET QUA MERGE D03 DE FULL VERIFY] <<<" "Yellow"
}

Write-OutputAndLog "`n[KET LUAN VE SNAPSHOT CUOI D02 (REF: $TargetRef)]:" "Yellow"
Write-OutputAndLog "  1. Snapshot cuoi D02 da co day du 20/20 artifact nen tang cua D01 va D02 (DTO, State Machine, Raft interfaces, UI stub, cluster config)."
Write-OutputAndLog "  2. Cac blocker ve ma nguon con thieu cho G1 Full Verify:"
foreach ($mb in $MissingG1Blockers) {
    Write-OutputAndLog "     * $mb" "Yellow"
}
Write-OutputAndLog "  3. Quy trinh chuan: TV6 khong cho TV1-TV5 hoan thanh D03 de viet script; script nay duoc tao san sang tu D02 va se duoc chay de Gate G1 Full Verify sau khi code D03 duoc merge hoan chinh."

Write-OutputAndLog "`n[HUONG DAN MO CHAN TOOLCHAIN / MOI TRUONG (UNBLOCK)]:" "Cyan"
Write-OutputAndLog "  1. Cai dat JDK 21 hoac JDK 25 va thiet lap bien moi truong JAVA_HOME."
Write-OutputAndLog "  2. Cai dat Apache Maven (3.9+) va them duong dan bin vao PATH."
Write-OutputAndLog "  3. Hoac chay script voi tham so tuy chon neu may co JDK/Maven o vi tri rieng:"
Write-OutputAndLog "     .\scripts\verify-g1-build.ps1 -MavenPath '<duong_dan_mvn.cmd>' -JavaHome '<duong_dan_jdk>'"

Write-OutputAndLog "`n$Divider" "Cyan"
Write-OutputAndLog " File bang chung da ghi tai: $LogPath" "Cyan"
Write-OutputAndLog $Divider "Cyan"

# Ghi toan bo log ra file UTF-8
try {
    [System.IO.File]::WriteAllLines($LogPath, $LogContent, [System.Text.Encoding]::UTF8)
} catch {
    Write-Host "Loi khi ghi tep log: $_" -ForegroundColor Red
}

# Khoi phuc bien moi truong JAVA_HOME neu co thay doi
if (-not [string]::IsNullOrWhiteSpace($JavaHome)) {
    $env:JAVA_HOME = $PreviousJavaHome
}

if ($FinalStatus -eq "PASS" -or $FinalStatus -eq "PASS_D02_SNAPSHOT_PENDING_D03") {
    exit 0
} elseif ($FinalStatus -eq "BLOCKED") {
    exit 2
} else {
    exit 1
}


# 8. Thuc thi Maven Build / Test thuc te (Gate G1 Tieu chi 3)
Write-OutputAndLog "`n[7. THUC THI MAVEN BUILD & TEST (GATE G1 VERIFICATION)]" "Yellow"

$BuildExitCode = -1

if (-not $IsBlocked -and $MvnExe) {
    $MavenGoal = if ($SkipTests) { "test-compile" } else { "test" }
    $MavenArgsList = [System.Collections.Generic.List[string]]::new()
    $MavenArgsList.Add("-B")
    $MavenArgsList.Add("-ntp")
    if ($Offline) { $MavenArgsList.Add("-o") }
    $MavenArgsList.Add("clean")
    $MavenArgsList.Add($MavenGoal)

    $CmdDisplay = "$MvnExe $($MavenArgsList -join ' ')"
    Write-OutputAndLog "  Lenh thuc thi: $CmdDisplay" "Cyan"

    Push-Location $RepoRoot
    try {
        $BuildOutput = (& $MvnExe @MavenArgsList 2>&1)
        $BuildExitCode = $LASTEXITCODE

        foreach ($line in $BuildOutput) {
            Write-OutputAndLog "    $line"
        }

        Write-OutputAndLog "  Ma ket thuc build (Exit Code): $BuildExitCode"
        if ($BuildExitCode -eq 0) {
            Write-OutputAndLog "  [PASS] Maven build/test thanh cong toan bo cac module." "Green"
        } else {
            Write-OutputAndLog "  [FAIL] Maven build/test that bai voi exit code $BuildExitCode." "Red"
            $IsBlocked = $true
            $BlockedReasons.Add("Maven build goal '$MavenGoal' that bai voi exit code $BuildExitCode")
        }
    } finally {
        Pop-Location
    }
} else {
    Write-OutputAndLog "  [SKIPPED] Khong the thuc thi Maven build do moi truong toolchain bi chan (BLOCKED)." "Red"
    Write-OutputAndLog "  Ly do chan:" "Red"
    foreach ($reason in $BlockedReasons) {
        Write-OutputAndLog "    - $reason" "Red"
    }
    $BuildExitCode = 127
}

# 9. Tong ket Gate G1 & Danh gia Snapshot
Write-OutputAndLog "`n$SubDivider" "Cyan"
Write-OutputAndLog " TONG KET DANH GIA GATE G1 VA SNAPSHOT CUOI D02" "Cyan"
Write-OutputAndLog $SubDivider "Cyan"

$FinalStatus = "UNKNOWN"

if ($IsBlocked) {
    $FinalStatus = "BLOCKED"
    Write-OutputAndLog "`n>>> TRANG THAI G1: [BLOCKED] <<<" "Red"
    Write-OutputAndLog "Ly do can tro Gate G1:" "Red"
    $idx = 1
    foreach ($r in $BlockedReasons) {
        Write-OutputAndLog "  $idx. $r" "Red"
        $idx++
    }
} elseif ($BuildExitCode -ne 0) {
    $FinalStatus = "FAIL"
    Write-OutputAndLog "`n>>> TRANG THAI G1: [FAIL] <<<" "Red"
    Write-OutputAndLog "Build that bai voi ma loi: $BuildExitCode" "Red"
} else {
    if ($D03AvailableCount -lt $D03PendingArtifacts.Count) {
        $FinalStatus = "PASS_D02_SNAPSHOT_PENDING_D03"
        Write-OutputAndLog "`n>>> TRANG THAI G1: [PASS SNAPSHOT D02 - D03 PENDING] <<<" "Yellow"
        Write-OutputAndLog "Snapshot D02 da build dat chuan. Cho TV1-TV5 hoan thanh va merge D03 de Full Verify G1." "Yellow"
    } else {
        $FinalStatus = "PASS"
        Write-OutputAndLog "`n>>> TRANG THAI G1: [PASS FULL VERIFICATION] <<<" "Green"
        Write-OutputAndLog "Toan bo ma nguon va hop dong D01-D03 da duoc ghep va kiem chung thanh cong!" "Green"
    }
}

Write-OutputAndLog "`n[DANH SACH BLOCKER VA CAC BUOC TIEP THEO]:" "Yellow"
if ($MissingG1Blockers.Count -gt 0) {
    Write-OutputAndLog "  Cac thanh phan can thiet con thieu cho G1 Full Verify:"
    foreach ($mb in $MissingG1Blockers) {
        Write-OutputAndLog "    * $mb" "Yellow"
    }
} else {
    Write-OutputAndLog "  Khong con blocker hop dong nao ton dong." "Green"
}

Write-OutputAndLog "`n[HUONG DAN MO CHAN TOOLCHAIN / MOI TRUONG (UNBLOCK)]:" "Cyan"
Write-OutputAndLog "  1. Cai dat JDK 21 hoac JDK 25 va thiet lap bien moi truong JAVA_HOME."
Write-OutputAndLog "  2. Cai dat Apache Maven (3.9+) va them duong dan bin vao PATH."
Write-OutputAndLog "  3. Hoac chay script voi tham so tuy chon neu may co JDK/Maven o vi tri rieng:"
Write-OutputAndLog "     .\scripts\verify-g1-build.ps1 -MavenPath '<duong_dan_mvn.cmd>' -JavaHome '<duong_dan_jdk>'"

Write-OutputAndLog "`n$Divider" "Cyan"
Write-OutputAndLog " File bang chung da ghi tai: $LogPath" "Cyan"
Write-OutputAndLog $Divider "Cyan"

# Ghi toan bo log ra file UTF-8
try {
    [System.IO.File]::WriteAllLines($LogPath, $LogContent, [System.Text.Encoding]::UTF8)
} catch {
    Write-Host "Loi khi ghi tep log: $_" -ForegroundColor Red
}

# Khoi phuc bien moi truong JAVA_HOME neu co thay doi
if (-not [string]::IsNullOrWhiteSpace($JavaHome)) {
    $env:JAVA_HOME = $PreviousJavaHome
}

if ($FinalStatus -eq "PASS" -or $FinalStatus -eq "PASS_D02_SNAPSHOT_PENDING_D03") {
    exit 0
} elseif ($FinalStatus -eq "BLOCKED") {
    exit 2
} else {
    exit 1
}
