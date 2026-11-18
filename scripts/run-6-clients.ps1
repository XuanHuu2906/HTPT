<#
.SYNOPSIS
    Mở đồng thời 6 cửa sổ JavaFX Desktop Client tranh chấp cùng 1 ghế (TASK TV1-D18).

.DESCRIPTION
    Script hỗ trợ chạy kịch bản 6 Client đặt đồng thời 1 ghế (mặc định G02) trên Windows PowerShell 5.1+.
    - Chế độ mặc định (-Mode fake): khởi động FakeClusterMain (FAKE SERVER, không phải Node thật, không phải Raft),
      mở 6 cửa sổ SixClientAutopilot xếp lưới 3x2, đồng bộ mốc giờ bấm đặt vé, chụp ảnh toàn màn hình
      artifacts/evidence/D18/TV1-D18-ui.png, tổng hợp bảng requestId/trạng thái vào log và dọn sạch tiến trình/thư mục tạm.
    - Chế độ cụm thật (-Mode real): đọc cấu hình endpoint cụm 5 Node từ client.properties (D16), KHÔNG khởi động
      FakeClusterMain, KHÔNG hiển thị bất kỳ nhãn FAKE nào và ghi nguồn là REAL. Chế độ này dành cho nghiệm thu
      AC03 tại mốc G5 (sau khi TV4-D18 bàn giao ordered apply).

    LƯU Ý QUAN TRỌNG:
    Kết quả chạy với -Mode fake KHÔNG PHẢI là nghiệm thu AC03 thật và KHÔNG chứng minh Raft/Backend PASS.
    AC03 thật chỉ được nghiệm thu bởi cả nhóm tại mốc G5 trên cụm 5 Node Raft + 5 PostgreSQL thật.

.PARAMETER Clients
    Số lượng cửa sổ Client cần mở đồng thời (mặc định: 6).

.PARAMETER Seat
    Mã ghế tranh chấp (G01..G10, mặc định: G02).

.PARAMETER Seed
    Seed cho độ trễ ngẫu nhiên trên FakeClusterMain để tái lập thứ tự đến (mặc định: 42).

.PARAMETER DropResponseFor
    Tùy chọn tạo kịch bản UNKNOWN: chỉ định clientId (ví dụ "3" hoặc "client-3") bị FakeClusterMain
    làm rơi phản hồi BOOK_TICKET lần đầu sau khi đã lưu kết quả.

.PARAMETER TimeoutSec
    Thời gian chờ tối đa (giây) để tất cả Client đạt trạng thái cuối (mặc định: 45).

.PARAMETER Mode
    Chế độ chạy: "fake" (mặc định, dùng FakeClusterMain) hoặc "real" (cụm 5 Node thật tại G5).

.PARAMETER Profile
    Hồ sơ cấu hình cụm khi chạy -Mode real (dev hoặc lan, mặc định: dev).

.PARAMETER ConfigPath
    Đường dẫn file client.properties tùy biến khi chạy -Mode real (để trống sẽ dùng cơ chế mặc định D16).

.PARAMETER ResultLogPath
    Đường dẫn file log tổng hợp (mặc định: artifacts/evidence/D18/TV1-D18-result.log).

.PARAMETER ScreenshotPath
    Đường dẫn ảnh chụp màn hình 3x2 (mặc định: artifacts/evidence/D18/TV1-D18-ui.png).

.PARAMETER SkipScreenshot
    Bỏ qua bước chụp ảnh màn hình nếu chỉ cần thu bảng log.

.EXAMPLE
    # Chạy 6 Client tranh ghế G02 với FAKE SERVER (mặc định D18):
    powershell -ExecutionPolicy Bypass -File .\scripts\run-6-clients.ps1 -Clients 6 -Seat G02 -Seed 42 -Mode fake

.EXAMPLE
    # Chạy kịch bản UNKNOWN (làm rơi phản hồi của Client 3 trên FAKE SERVER):
    powershell -ExecutionPolicy Bypass -File .\scripts\run-6-clients.ps1 -Clients 6 -Seat G02 -DropResponseFor 3 -Mode fake -SkipScreenshot

.EXAMPLE
    # Chạy nghiệm thu AC03 thật tại G5 (CHỈ chạy sau khi TV4-D18 bàn giao và cụm 5 Node đang hoạt động):
    powershell -ExecutionPolicy Bypass -File .\scripts\run-6-clients.ps1 -Clients 6 -Seat G02 -Mode real -Profile dev

.NOTES
    Cách đọc kết quả trong TV1-D18-result.log:
    - Với -Mode fake: kỳ vọng đúng 1 CONFIRMED, 5 REJECTED (SEAT_ALREADY_BOOKED), 1 vé ACTIVE cho ghế G02 trên FAKE SERVER.
    - Nếu dùng -DropResponseFor N: Client N chuyển sang UNKNOWN (pendingCount=1), không phải REJECTED; tổng vé ACTIVE trên server vẫn là 1.
    - Với -Mode real (G5): nguồn ghi là REAL, không có nhãn FAKE; đối chiếu thêm SQL trên 5 PostgreSQL theo SPEC-05.
#>
[CmdletBinding()]
param(
    [ValidateRange(1, 12)]
    [int]$Clients = 6,

    [ValidatePattern('^G(0[1-9]|10)$')]
    [string]$Seat = 'G02',

    [long]$Seed = 42,

    [string]$DropResponseFor = '',

    [ValidateRange(10, 300)]
    [int]$TimeoutSec = 45,

    [ValidateSet('fake', 'real')]
    [string]$Mode = 'fake',

    [string]$Profile = 'dev',

    [string]$ConfigPath = '',

    [string]$ResultLogPath = '',

    [string]$ScreenshotPath = '',

    [switch]$SkipScreenshot
)

$ErrorActionPreference = 'Stop'

# ==============================================================================
# THIẾT LẬP ĐƯỜNG DẪN AN TOÀN (HỖ TRỢ KHOẢNG TRẮNG & KÝ TỰ UNICODE)
# ==============================================================================
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
$EvidenceDir = Join-Path $RepoRoot 'artifacts\evidence\D18'

if ([string]::IsNullOrWhiteSpace($ResultLogPath)) {
    $ResultLogPath = Join-Path $EvidenceDir 'TV1-D18-result.log'
}
if ([string]::IsNullOrWhiteSpace($ScreenshotPath)) {
    $ScreenshotPath = Join-Path $EvidenceDir 'TV1-D18-ui.png'
}

New-Item -ItemType Directory -Path $EvidenceDir -Force | Out-Null

$RunStamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$RunDir = Join-Path $EvidenceDir ("run-" + $RunStamp)
New-Item -ItemType Directory -Path $RunDir -Force | Out-Null

$StopSignalFile = Join-Path $RunDir 'stop.signal'
$ServerProc = $null
$ClientProcs = New-Object System.Collections.ArrayList
$LogLines = New-Object System.Collections.ArrayList
$ExitCode = 0

function Write-Step {
    param([string]$Message)
    $ts = Get-Date -Format 'HH:mm:ss.fff'
    $line = "[$ts] $Message"
    Write-Host $line
    [void]$LogLines.Add($line)
}

function Test-Spec03NodeStatus {
    param(
        [string]$TargetHost,
        [int]$TargetPort,
        [int]$ConnectTimeoutMs = 1500
    )
    $tcp = $null
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient
        $async = $tcp.BeginConnect($TargetHost, $TargetPort, $null, $null)
        if (-not $async.AsyncWaitHandle.WaitOne($ConnectTimeoutMs, $false)) {
            return $false
        }
        $tcp.EndConnect($async)
        $tcp.ReceiveTimeout = 2000
        $tcp.SendTimeout = 2000

        $msgId = [guid]::NewGuid().ToString().ToLowerInvariant()
        $sentAt = [DateTimeOffset]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        $json = '{"protocolVersion":1,"messageId":"' + $msgId + '","correlationId":null,"type":"NODE_STATUS","senderId":"client-probe","sentAt":"' + $sentAt + '","payload":{}}'
        $payloadBytes = [System.Text.Encoding]::UTF8.GetBytes($json)
        $lenBytes = [BitConverter]::GetBytes([int]$payloadBytes.Length)
        if ([BitConverter]::IsLittleEndian) {
            [Array]::Reverse($lenBytes)
        }

        $stream = $tcp.GetStream()
        $stream.Write($lenBytes, 0, 4)
        $stream.Write($payloadBytes, 0, $payloadBytes.Length)
        $stream.Flush()

        # Đọc 4-byte length big-endian
        $respLenBuf = New-Object byte[] 4
        $readTotal = 0
        while ($readTotal -lt 4) {
            $n = $stream.Read($respLenBuf, $readTotal, 4 - $readTotal)
            if ($n -le 0) { return $false }
            $readTotal += $n
        }
        if ([BitConverter]::IsLittleEndian) {
            [Array]::Reverse($respLenBuf)
        }
        $respLen = [BitConverter]::ToInt32($respLenBuf, 0)
        if ($respLen -le 0 -or $respLen -gt 1048576) {
            return $false
        }

        $respBuf = New-Object byte[] $respLen
        $readBody = 0
        while ($readBody -lt $respLen) {
            $n = $stream.Read($respBuf, $readBody, $respLen - $readBody)
            if ($n -le 0) { return $false }
            $readBody += $n
        }
        $respJson = [System.Text.Encoding]::UTF8.GetString($respBuf)
        return ($respJson -match '"NODE_STATUS_RESPONSE"')
    } catch {
        return $false
    } finally {
        if ($null -ne $tcp) {
            try { $tcp.Close() } catch {}
        }
    }
}

function Parse-KeyValueFile {
    param([string]$FilePath)
    $map = @{}
    if (Test-Path -LiteralPath $FilePath) {
        $rawLines = Get-Content -LiteralPath $FilePath -Encoding UTF8
        foreach ($l in $rawLines) {
            if ($l -match '^([^=]+)=(.*)$') {
                $map[$matches[1].Trim()] = $matches[2].Trim()
            }
        }
    }
    return $map
}

function Capture-FullScreenPng {
    param([string]$OutputPath)
    Add-Type -AssemblyName System.Windows.Forms
    Add-Type -AssemblyName System.Drawing
    $bounds = [System.Windows.Forms.Screen]::PrimaryScreen.Bounds
    $bitmap = New-Object System.Drawing.Bitmap($bounds.Width, $bounds.Height)
    try {
        $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
        try {
            $graphics.CopyFromScreen($bounds.Location, [System.Drawing.Point]::Empty, $bounds.Size)
        } finally {
            $graphics.Dispose()
        }
        $parentDir = Split-Path -Parent $OutputPath
        if (-not [string]::IsNullOrWhiteSpace($parentDir)) {
            New-Item -ItemType Directory -Path $parentDir -Force | Out-Null
        }
        $bitmap.Save($OutputPath, [System.Drawing.Imaging.ImageFormat]::Png)
    } finally {
        $bitmap.Dispose()
    }
}

function Compose-GridSnapshotPng {
    param(
        [hashtable]$SnapshotFiles,
        [int]$ClientCount,
        [string]$RunMode,
        [string]$OutputPath
    )
    Add-Type -AssemblyName System.Drawing
    $cols = 3
    $rows = [Math]::Max(1, [int][Math]::Ceiling($ClientCount / 3.0))
    $cellW = 512
    $cellH = 420
    $titleH = 26
    $pad = 4
    $totalW = $cols * $cellW
    $totalH = $rows * $cellH

    $canvas = New-Object System.Drawing.Bitmap($totalW, $totalH)
    $g = [System.Drawing.Graphics]::FromImage($canvas)
    try {
        $g.Clear([System.Drawing.Color]::FromArgb(255, 15, 23, 42))
        $g.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
        $titleFont = New-Object System.Drawing.Font('Segoe UI', 9, [System.Drawing.FontStyle]::Bold)
        $titleBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::White)
        $titleBgBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(255, 30, 41, 59))
        $borderPen = New-Object System.Drawing.Pen([System.Drawing.Color]::FromArgb(255, 100, 116, 139), 2)

        for ($idx = 1; $idx -le $ClientCount; $idx++) {
            $c = ($idx - 1) % $cols
            $r = [int][Math]::Floor(($idx - 1) / $cols)
            $x = $c * $cellW + $pad
            $y = $r * $cellH + $pad
            $w = $cellW - (2 * $pad)
            $h = $cellH - (2 * $pad)

            $g.FillRectangle($titleBgBrush, $x, $y, $w, $titleH)
            $winTitle = if ($RunMode -eq 'real') {
                "CLIENT-$idx - CUM 5 NODE (REAL)"
            } else {
                "CLIENT-$idx - FAKE SERVER (khong phai Node that)"
            }
            $g.DrawString($winTitle, $titleFont, $titleBrush, ($x + 8), ($y + 5))

            $snapPath = $SnapshotFiles[$idx]
            if (-not [string]::IsNullOrWhiteSpace($snapPath) -and (Test-Path -LiteralPath $snapPath)) {
                $img = [System.Drawing.Image]::FromFile($snapPath)
                try {
                    $g.DrawImage($img, $x, ($y + $titleH), $w, ($h - $titleH))
                } finally {
                    $img.Dispose()
                }
            }
            $g.DrawRectangle($borderPen, $x, $y, $w, $h)
        }

        $titleFont.Dispose()
        $titleBrush.Dispose()
        $titleBgBrush.Dispose()
        $borderPen.Dispose()

        $parentDir = Split-Path -Parent $OutputPath
        if (-not [string]::IsNullOrWhiteSpace($parentDir)) {
            New-Item -ItemType Directory -Path $parentDir -Force | Out-Null
        }
        $canvas.Save($OutputPath, [System.Drawing.Imaging.ImageFormat]::Png)
    } finally {
        $g.Dispose()
        $canvas.Dispose()
    }
}

function Quote-Arg {
    param([string]$Value)
    if ($null -eq $Value) { return '""' }
    if ($Value -match '[\s"]') {
        return '"' + ($Value -replace '"', '\"') + '"'
    }
    return $Value
}

try {
    $ModeBanner = if ($Mode -eq 'fake') { 'FAKE SERVER (khong phai Node that, khong phai Raft)' } else { 'REAL CLUSTER (G5)' }
    Write-Step "=== BAT DAU KICH BAN $Clients CLIENT TRANH GHE $Seat | MODE=$Mode ($ModeBanner) | SEED=$Seed ==="
    Write-Step "RepoRoot: $RepoRoot"
    Write-Step "RunDir tam thoi: $RunDir"

    # 1. Kiểm tra JDK & Maven
    $javaCmd = Get-Command java -ErrorAction Stop
    $mvnCmd = Get-Command mvn -ErrorAction Stop
    Write-Step "JDK: $($javaCmd.Source) | Maven: $($mvnCmd.Source)"

    # 2. Biên dịch desktop-client + test sources và dựng classpath (không sửa pom.xml)
    Write-Step "Dang chay: mvn -B -ntp -pl desktop-client -am test-compile -Dmaven.compiler.release=21"
    Push-Location -LiteralPath $RepoRoot
    try {
        & mvn -B -ntp -pl desktop-client -am test-compile "-Dmaven.compiler.release=21"
        if ($LASTEXITCODE -ne 0) {
            throw "mvn test-compile that bai voi exit code $LASTEXITCODE"
        }

        $cpFile = Join-Path $RunDir 'deps-classpath.txt'
        Write-Step "Dang lay runtime/test classpath bang dependency:build-classpath..."
        & mvn -B -ntp -pl desktop-client dependency:build-classpath "-DincludeScope=test" "-Dmdep.outputFile=$cpFile"
        if ($LASTEXITCODE -ne 0) {
            throw "mvn dependency:build-classpath that bai voi exit code $LASTEXITCODE"
        }
    } finally {
        Pop-Location
    }

    $depsCp = (Get-Content -LiteralPath (Join-Path $RunDir 'deps-classpath.txt') -Raw).Trim()
    $testClasses = Join-Path $RepoRoot 'desktop-client\target\test-classes'
    $mainClasses = Join-Path $RepoRoot 'desktop-client\target\classes'
    $sharedClasses = Join-Path $RepoRoot 'shared\target\classes'
    $fullClasspath = "$testClasses;$mainClasses;$sharedClasses;$depsCp"

    # Dùng Java @argfile để tránh giới hạn độ dài dòng lệnh và khoảng trắng đường dẫn trên Windows
    $cpArgFile = Join-Path $RunDir 'java-cp.argfile'
    $escapedCp = $fullClasspath -replace '\\', '\\'
    Set-Content -LiteralPath $cpArgFile -Value ("-cp`r`n`"$escapedCp`"") -Encoding ASCII

    # 3. Khởi động FakeClusterMain nếu ở chế độ fake
    $fakePort = 0
    $serverLogFile = Join-Path $RunDir 'fake-server.log'
    $serverSummaryFile = Join-Path $RunDir 'fake-server-summary.properties'

    if ($Mode -eq 'fake') {
        $portFile = Join-Path $RunDir 'fake-cluster.port'
        $serverArgs = @(
            "@$cpArgFile",
            'vn.nhom6.railway.client.harness.FakeClusterMain',
            '--port', '0',
            '--port-file', $portFile,
            '--node-id', 'node-1',
            '--seed', "$Seed",
            '--max-jitter-ms', '35',
            '--seat', $Seat,
            '--log-file', $serverLogFile,
            '--summary-file', $serverSummaryFile,
            '--stop-file', $StopSignalFile
        )
        if (-not [string]::IsNullOrWhiteSpace($DropResponseFor)) {
            $serverArgs += @('--drop-response-for', $DropResponseFor)
        }

        $serverArgStr = ($serverArgs | ForEach-Object { Quote-Arg $_ }) -join ' '
        $serverStdOut = Join-Path $RunDir 'fake-server-stdout.log'
        $serverStdErr = Join-Path $RunDir 'fake-server-stderr.log'
        Write-Step "Dang khoi dong FakeClusterMain (FAKE SERVER, khong phai Node that)..."
        $ServerProc = Start-Process -FilePath 'java' -ArgumentList $serverArgStr -WorkingDirectory $RepoRoot `
            -RedirectStandardOutput $serverStdOut -RedirectStandardError $serverStdErr -WindowStyle Hidden -PassThru

        # Chờ portFile xuất hiện (timeout 15s)
        $waitDeadline = (Get-Date).AddSeconds(15)
        while ((Get-Date) -lt $waitDeadline) {
            if ($ServerProc.HasExited) {
                $errContent = if (Test-Path -LiteralPath $serverStdErr) { Get-Content -LiteralPath $serverStdErr -Raw } else { '' }
                throw "FakeClusterMain thoat som voi exit code $($ServerProc.ExitCode). Stderr: $errContent"
            }
            if (Test-Path -LiteralPath $portFile) {
                $rawPort = (Get-Content -LiteralPath $portFile -Raw).Trim()
                if ($rawPort -match '^\d+$') {
                    $fakePort = [int]$rawPort
                    break
                }
            }
            Start-Sleep -Milliseconds 100
        }

        if ($fakePort -le 0) {
            throw "Het thoi gian cho file cong fake-cluster.port tu FakeClusterMain"
        }

        # Thăm dò sẵn sàng bằng gói tin SPEC-03 NODE_STATUS
        $ready = $false
        $probeDeadline = (Get-Date).AddSeconds(10)
        while ((Get-Date) -lt $probeDeadline) {
            if (Test-Spec03NodeStatus -TargetHost '127.0.0.1' -TargetPort $fakePort) {
                $ready = $true
                break
            }
            Start-Sleep -Milliseconds 150
        }
        if (-not $ready) {
            throw "FakeClusterMain tren cong $fakePort khong phan hoi NODE_STATUS_RESPONSE dung SPEC-03"
        }
        Write-Step "FakeClusterMain san sang tai 127.0.0.1:$fakePort (NODE_STATUS_RESPONSE OK, PID=$($ServerProc.Id))."
    } else {
        Write-Step "Che do REAL: doc endpoint cum tu client.properties (profile=$Profile), KHONG khoi dong FakeClusterMain."
    }

    # 4. Tính kích thước lưới 3x2 theo độ phân giải màn hình hiện tại
    Add-Type -AssemblyName System.Windows.Forms
    $workArea = [System.Windows.Forms.Screen]::PrimaryScreen.WorkingArea
    $cols = 3
    $rows = [Math]::Max(1, [int][Math]::Ceiling($Clients / 3.0))
    $winW = [Math]::Floor($workArea.Width / $cols)
    $winH = [Math]::Floor($workArea.Height / $rows)
    Write-Step "Kich thuoc vung lam viec man hinh: $($workArea.Width)x$($workArea.Height) -> luoi ${cols}x${rows}, moi cua so ${winW}x${winH}"

    # 5. Tính giờ bắt đầu đồng bộ (12 giây khi mở >=4 JVM JavaFX để tất cả kịp dựng UI trước khi bấm đồng thời)
    $leadMs = if ($Clients -ge 4) { 12000 } else { 7000 }
    $startAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() + $leadMs
    Write-Step "Moc gio dong bo gui lenh dat ghe (scenario.startAt): $startAtMs (+${leadMs}ms)"

    # 6. Tạo thư mục tạm riêng và khởi chạy $Clients tiến trình SixClientAutopilot
    $resultFiles = @{}
    $snapshotFiles = @{}
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    for ($i = 1; $i -le $Clients; $i++) {
        $clientDir = Join-Path $RunDir ("client" + $i)
        $journalDir = Join-Path $clientDir 'journal'
        New-Item -ItemType Directory -Path $journalDir -Force | Out-Null

        $clientCfgPath = $ConfigPath
        if ($Mode -eq 'fake') {
            $clientCfgPath = Join-Path $clientDir 'client.properties'
            $readTimeout = if (-not [string]::IsNullOrWhiteSpace($DropResponseFor)) { 2500 } else { 5000 }
            $cfgContent = @"
cluster.profile=fake
client.mode=socket
client.selectionStrategy=PREFERRED_FIRST
client.preferredNode=node-1
client.connectTimeoutMs=2000
client.readTimeoutMs=$readTimeout
client.maxRedirects=2
client.failureCooldownMs=5000
fake.cluster.nodes=node-1
fake.node.node-1.client.host=127.0.0.1
fake.node.node-1.client.port=$fakePort
"@
            # Ghi UTF-8 KHONG BOM de Java Properties.load() doc dung key dau tien 'cluster.profile'
            [System.IO.File]::WriteAllText($clientCfgPath, $cfgContent, $utf8NoBom)
        }

        $colIdx = ($i - 1) % $cols
        $rowIdx = [int][Math]::Floor(($i - 1) / $cols)
        $posX = $workArea.Left + ($colIdx * $winW)
        $posY = $workArea.Top + ($rowIdx * $winH)

        $resFile = Join-Path $clientDir 'result.properties'
        $snapFile = Join-Path $clientDir ("client-" + $i + ".png")
        $resultFiles[$i] = $resFile
        $snapshotFiles[$i] = $snapFile

        $clientArgs = @(
            "@$cpArgFile",
            "-Dfile.encoding=UTF-8",
            "-Dclient.id=$i",
            "-Dscenario.mode=$Mode",
            "-Drailway.journal.dir=$journalDir",
            "-Dscenario.seat=$Seat",
            "-Dscenario.startAt=$startAtMs",
            "-Dscenario.passenger=Hanh khach Test $i",
            "-Dscenario.window.x=$posX",
            "-Dscenario.window.y=$posY",
            "-Dscenario.window.w=$winW",
            "-Dscenario.window.h=$winH",
            "-Dscenario.resultFile=$resFile",
            "-Dscenario.snapshotFile=$snapFile",
            "-Dscenario.stopFile=$StopSignalFile",
            "-Dscenario.maxLifetimeSec=$($TimeoutSec + 15)"
        )

        if (-not [string]::IsNullOrWhiteSpace($DropResponseFor)) {
            $clientArgs += "-Dbooking.submitTimeoutMs=2000"
        }
        if (-not [string]::IsNullOrWhiteSpace($clientCfgPath)) {
            $clientArgs += "-Dclient.config=$clientCfgPath"
        } elseif ($Mode -eq 'real') {
            $clientArgs += "-Dcluster.profile=$Profile"
            $clientArgs += "-Dclient.mode=socket"
        }

        $clientArgs += 'vn.nhom6.railway.client.harness.SixClientAutopilot'
        $clientArgStr = ($clientArgs | ForEach-Object { Quote-Arg $_ }) -join ' '
        $cOut = Join-Path $clientDir 'stdout.log'
        $cErr = Join-Path $clientDir 'stderr.log'

        $proc = Start-Process -FilePath 'java' -ArgumentList $clientArgStr -WorkingDirectory $RepoRoot `
            -RedirectStandardOutput $cOut -RedirectStandardError $cErr -PassThru
        [void]$ClientProcs.Add($proc)
        Write-Step "Da mo CLIENT-$i (PID=$($proc.Id)) tai ($posX,$posY) kich thuoc ${winW}x${winH}"
    }

    # 7. Chờ tất cả Client ghi file kết quả cuối hoặc hết timeout tổng
    Write-Step "Dang cho tat ca $Clients Client hoan thanh giao dich (timeout=${TimeoutSec}s)..."
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    $completedCount = 0
    while ((Get-Date) -lt $deadline) {
        $completedCount = 0
        for ($i = 1; $i -le $Clients; $i++) {
            if (Test-Path -LiteralPath $resultFiles[$i]) {
                $completedCount++
            }
        }
        if ($completedCount -ge $Clients) {
            break
        }
        Start-Sleep -Milliseconds 250
    }
    Write-Step "So Client da dat trang thai cuoi: $completedCount / $Clients"

    # Chờ thêm 1.5s để UI vẽ xong nhãn CONFIRMED / REJECTED và ghế BOOKED trước khi chụp ảnh
    Start-Sleep -Milliseconds 1500

    # 8. Chụp toàn màn hình 3x2 (nếu môi trường không có interactive desktop cho CopyFromScreen thì ghép từ 6 ảnh Scene.snapshot thật)
    $screenshotMethod = 'SKIPPED'
    if (-not $SkipScreenshot) {
        try {
            Capture-FullScreenPng -OutputPath $ScreenshotPath
            $screenshotMethod = 'CopyFromScreen (full desktop 3x2)'
            Write-Step "Da chup anh man hinh 6 cua so xep 3x2 bang CopyFromScreen tai: $ScreenshotPath"
        } catch {
            Write-Step "CopyFromScreen khong kha dung trong phien hien tai ($($_.Exception.Message)). Dang ghep 3x2 tu 6 anh JavaFX Scene.snapshot() that..."
            try {
                Compose-GridSnapshotPng -SnapshotFiles $snapshotFiles -ClientCount $Clients -RunMode $Mode -OutputPath $ScreenshotPath
                $screenshotMethod = "JavaFX Scene.snapshot() 3x2 composite (CopyFromScreen khong kha dung: $($_.Exception.Message))"
                Write-Step "Da tao anh 6 cua so xep 3x2 tu JavaFX Scene.snapshot() tai: $ScreenshotPath"
            } catch {
                $screenshotMethod = "FAILED ($($_.Exception.Message))"
                Write-Step "CANH BAO: Khong the tao anh man hinh: $($_.Exception.Message)"
            }
        }
    } else {
        Write-Step "Bo qua chup anh man hinh theo co -SkipScreenshot."
    }

    # 9. Phát tín hiệu stop.signal để các tiến trình đóng gọn gàng (nhả journal.lock)
    Set-Content -LiteralPath $StopSignalFile -Value "STOP $(Get-Date -Format 'o')" -Encoding ASCII
    Start-Sleep -Seconds 2

    # 10. Thu thập và tổng hợp bảng kết quả 6 Client + FakeServer
    $confirmedTotal = 0
    $rejectedTotal = 0
    $unknownTotal = 0
    $errorTotal = 0
    $totalElapsed = 0L
    $elapsedSamples = 0

    $summaryBlock = New-Object System.Collections.ArrayList
    [void]$summaryBlock.Add("")
    [void]$summaryBlock.Add("--------------------------------------------------------------------------------")
    [void]$summaryBlock.Add("KET QUA CHAY SCRIPT scripts/run-6-clients.ps1 ($(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK'))")
    [void]$summaryBlock.Add("Mode: $Mode ($ModeBanner) | Clients: $Clients | Seat: $Seat | Seed: $Seed | DropResponseFor: $(if ($DropResponseFor) { $DropResponseFor } else { 'NONE' })")
    [void]$summaryBlock.Add("--------------------------------------------------------------------------------")
    [void]$summaryBlock.Add([string]::Format("{0,-9} | {1,-36} | {2,-24} | {3,-10} | {4,-20} | {5,-8} | {6,-9} | {7}",
        "ClientId", "RequestId", "SentAt (UTC)", "FinalState", "Reason/ErrorCode", "Elapsed", "Seat $Seat", "Pending"))
    [void]$summaryBlock.Add(("-" * 135))

    for ($i = 1; $i -le $Clients; $i++) {
        if (-not (Test-Path -LiteralPath $resultFiles[$i])) {
            $errLog = Join-Path (Split-Path -Parent $resultFiles[$i]) 'stderr.log'
            if (Test-Path -LiteralPath $errLog) {
                $errTxt = (Get-Content -LiteralPath $errLog -Raw -ErrorAction SilentlyContinue)
                if (-not [string]::IsNullOrWhiteSpace($errTxt)) {
                    Write-Step "[DIAG client-$i stderr] $errTxt"
                }
            }
        }
        $r = Parse-KeyValueFile -FilePath $resultFiles[$i]
        $reqId = if ($r.ContainsKey('requestId')) { $r['requestId'] } else { 'N/A (TIMEOUT)' }
        $sentAt = if ($r.ContainsKey('sentAt')) { $r['sentAt'] } else { '-' }
        $fState = if ($r.ContainsKey('finalState')) { $r['finalState'] } else { 'TIMEOUT' }
        $reason = if ($r.ContainsKey('reasonCode')) { $r['reasonCode'] } else { '-' }
        $elStr = if ($r.ContainsKey('elapsedMs')) { $r['elapsedMs'] + 'ms' } else { '-' }
        $seatSt = if ($r.ContainsKey('seatStatus')) { $r['seatStatus'] } else { 'UNKNOWN' }
        $pend = if ($r.ContainsKey('pendingCount')) { $r['pendingCount'] } else { '?' }

        if ($r.ContainsKey('elapsedMs') -and ($r['elapsedMs'] -match '^\d+$')) {
            $totalElapsed += [long]$r['elapsedMs']
            $elapsedSamples++
        }

        switch ($fState) {
            'CONFIRMED' { $confirmedTotal++ }
            'REJECTED'  { $rejectedTotal++ }
            'UNKNOWN'   { $unknownTotal++ }
            default     { $errorTotal++ }
        }

        $rowLine = [string]::Format("{0,-9} | {1,-36} | {2,-24} | {3,-10} | {4,-20} | {5,-8} | {6,-9} | {7}",
            "client-$i", $reqId, $sentAt, $fState, $reason, $elStr, $seatSt, $pend)
        [void]$summaryBlock.Add($rowLine)
    }

    $avgElapsed = if ($elapsedSamples -gt 0) { [Math]::Round($totalElapsed / [double]$elapsedSamples, 1) } else { 0 }
    [void]$summaryBlock.Add(("-" * 135))
    [void]$summaryBlock.Add("Tong ket Client: CONFIRMED=$confirmedTotal | REJECTED=$rejectedTotal | UNKNOWN=$unknownTotal | OTHER/ERROR=$errorTotal | Do tre trung binh=${avgElapsed}ms")
    [void]$summaryBlock.Add("Anh man hinh: $ScreenshotPath | Phuong thuc: $screenshotMethod")

    $activeTicketsServer = 'N/A'
    if ($Mode -eq 'fake' -and (Test-Path -LiteralPath $serverSummaryFile)) {
        $sMap = Parse-KeyValueFile -FilePath $serverSummaryFile
        $activeTicketsServer = $sMap['activeTicketsTargetSeat']
        [void]$summaryBlock.Add("Tong ket FAKE SERVER (khong phai Node that, khong phai Raft): activeTickets($Seat)=$activeTicketsServer | totalArrivals=$($sMap['totalArrivals']) | confirmed=$($sMap['confirmedCount']) | rejected=$($sMap['rejectedCount']) | dropped=$($sMap['droppedResponseCount'])")
    }
    if ($Mode -eq 'fake' -and (Test-Path -LiteralPath $serverLogFile)) {
        [void]$summaryBlock.Add("Audit log phia FAKE SERVER (khong ghi ho ten hanh khach):")
        foreach ($sl in (Get-Content -LiteralPath $serverLogFile -Encoding UTF8)) {
            [void]$summaryBlock.Add("  $sl")
        }
    }

    if ($Mode -eq 'fake') {
        [void]$summaryBlock.Add("LUU Y: Ket qua voi FAKE SERVER chi chung minh luong UI/protocol phia Client, KHONG phai nghiem thu AC03 tren Raft that (G5).")
    } else {
        [void]$summaryBlock.Add("NGUON DU LIEU: REAL (Cum 5 Node Raft).")
    }

    foreach ($line in $summaryBlock) {
        Write-Host $line
    }

    Add-Content -LiteralPath $ResultLogPath -Value $summaryBlock -Encoding UTF8
    Write-Step "Da ghi bang tong hop vao: $ResultLogPath"

    # Kiểm tra tiêu chí thành công của lần chạy
    if ($Mode -eq 'fake' -and [string]::IsNullOrWhiteSpace($DropResponseFor)) {
        if ($confirmedTotal -ne 1 -or $rejectedTotal -ne ($Clients - 1) -or "$activeTicketsServer" -ne '1') {
            Write-Step "LOI KIEM TRA: Ky vong 1 CONFIRMED, $($Clients - 1) REJECTED va 1 ve ACTIVE; thuc te CONFIRMED=$confirmedTotal, REJECTED=$rejectedTotal, activeTickets=$activeTicketsServer"
            $ExitCode = 2
        }
    } elseif ($Mode -eq 'fake' -and -not [string]::IsNullOrWhiteSpace($DropResponseFor)) {
        if ($unknownTotal -lt 1 -or "$activeTicketsServer" -ne '1') {
            Write-Step "LOI KIEM TRA UNKNOWN: Ky vong >=1 UNKNOWN va 1 ve ACTIVE; thuc te UNKNOWN=$unknownTotal, activeTickets=$activeTicketsServer"
            $ExitCode = 2
        }
    }
} catch {
    Write-Error "Loi khi chay run-6-clients.ps1: $($_.Exception.Message)"
    $ExitCode = 1
} finally {
    Write-Step "Don dep trong finally: dung tat ca tien trinh va xoa thu muc chay tam..."
    try {
        if (-not (Test-Path -LiteralPath $StopSignalFile)) {
            Set-Content -LiteralPath $StopSignalFile -Value "STOP_FINALLY" -Encoding ASCII -ErrorAction SilentlyContinue
        }
    } catch {}

    Start-Sleep -Milliseconds 600

    foreach ($p in $ClientProcs) {
        try {
            if ($null -ne $p -and -not $p.HasExited) {
                Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
            }
        } catch {}
    }

    if ($null -ne $ServerProc) {
        try {
            if (-not $ServerProc.HasExited) {
                Stop-Process -Id $ServerProc.Id -Force -ErrorAction SilentlyContinue
            }
        } catch {}
    }

    # Chờ OS nhả file handle rồi xóa sạch thư mục chạy tạm (gồm journal, payload, client.properties tạm)
    Start-Sleep -Milliseconds 500
    if (Test-Path -LiteralPath $RunDir) {
        try {
            Remove-Item -LiteralPath $RunDir -Recurse -Force -ErrorAction Stop
            Write-Host "[CLEANUP] Da xoa thu muc chay tam: $RunDir"
        } catch {
            Write-Warning "[CLEANUP] Khong xoa duoc ngay ${RunDir}: $($_.Exception.Message)"
        }
    }
}

exit $ExitCode
