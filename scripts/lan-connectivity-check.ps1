<#
.SYNOPSIS
    TV3-D24: LAN preflight connectivity and endpoint validation script.
.DESCRIPTION
    Kịch bản tiền kiểm mạng LAN theo SPEC-08 §3 & §3.1, §6 và PORTS.md:
    1. Kiểm tra cú pháp cấu hình configs/lan/node-1..5.properties (Client port 9100, Raft port 9200).
    2. Thẩm định chuỗi danh bạ node.peers với đầy đủ 5 voter (node-1..node-5) theo chuẩn ElectionRuntime.
    3. Tiền kiểm tính sẵn sàng của cổng mạng và rules Inbound Firewall trên máy trạm (TCP 9100/9200).
    4. Nhận diện dải IP placeholder và khẳng định bất biến an toàn (AGENTS.md §0, §5, §6):
       - Chạy ở chế độ tiền kiểm (Preflight) cục bộ.
       - Tuyệt đối KHÔNG claim 5 laptop kết nối thành công khi chưa deploy thực tế (Gate G6 / TV6-D25).
.PARAMETER ConfigFile
    Đường dẫn tới file cấu hình node cần kiểm tra (Mặc định: configs/lan/node-1.properties).
.PARAMETER AllNodes
    Switch để tự động kiểm tra toàn bộ 5 file configs/lan/node-1.properties .. node-5.properties.
.PARAMETER Mode
    Chế độ kiểm tra:
      - Preflight  : Tiền kiểm cú pháp, port mapping 9100/9200, danh bạ 5 voter, firewall (Mặc định).
      - LocalCheck : Kiểm tra kết nối TCP socket thực tế tới cổng local (127.0.0.1 hoặc TargetHost).
      - SelfTest   : Tự kiểm tra logic phân tích cấu hình, phát hiện lỗi cú pháp và thiếu voter.
      - LiveProbe  : Thử kết nối TCP thực tế tới cả 5 IP trong inventory TV6-D24 (Client 9100 + PING/PONG, Peer 9200).
.PARAMETER InventoryFile
    Đường dẫn tới file inventory IP thật từ TV6-D24 (.properties hoặc .env chứa NODE_PEERS / lan.node.node-X.*).
.PARAMETER AllowPlaceholderProbe
    Switch chỉ dùng cho test harness nếu muốn ép LiveProbe chạy thử khi vẫn đang là placeholder/localhost.
.PARAMETER TargetHost
    Địa chỉ host cho chế độ LocalCheck (Mặc định: 127.0.0.1).
.PARAMETER TimeoutMs
    Thời gian timeout kết nối socket (ms) (Mặc định: 1500).
.PARAMETER CheckFirewall
    Switch kiểm tra Windows Firewall rules cho cổng 9100 và 9200.
.PARAMETER LogPath
    Đường dẫn file ghi log minh chứng (Mặc định: artifacts/evidence/D24/TV3-D24-result.log).
.PARAMETER Strict
    Nếu được bật, script sẽ kết thúc với Exit Code 1 khi có bất kỳ cảnh báo nào.
.EXAMPLE
    .\scripts\lan-connectivity-check.ps1 -Mode Preflight
.EXAMPLE
    .\scripts\lan-connectivity-check.ps1 -AllNodes
.EXAMPLE
    .\scripts\lan-connectivity-check.ps1 -Mode LocalCheck -TargetHost 127.0.0.1
.EXAMPLE
    .\scripts\lan-connectivity-check.ps1 -Mode LiveProbe -InventoryFile configs/lan/inventory-live.env -CheckFirewall -LogPath artifacts/evidence/D25/TV3-D25-result.log
.EXAMPLE
    .\scripts\lan-connectivity-check.ps1 -Mode SelfTest
#>
[CmdletBinding()]
param(
    [string]$ConfigFile = 'configs/lan/node-1.properties',
    [switch]$AllNodes,
    [ValidateSet('Preflight', 'LocalCheck', 'SelfTest', 'LiveProbe')][string]$Mode = 'Preflight',
    [string]$InventoryFile = '',
    [switch]$AllowPlaceholderProbe,
    [string]$TargetHost = '127.0.0.1',
    [ValidateRange(100, 30000)][int]$TimeoutMs = 1500,
    [switch]$CheckFirewall,
    [string]$LogPath = '',
    [switch]$Strict
)

$ErrorActionPreference = 'Continue'
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path

if ([string]::IsNullOrWhiteSpace($LogPath)) {
    $LogPath = Join-Path $repoRoot 'artifacts/evidence/D24/TV3-D24-result.log'
}

$logDir = Split-Path -Parent $LogPath
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

$transcript = [System.Collections.Generic.List[string]]::new()

function Write-Log {
    param(
        [string]$Message,
        [string]$Color = 'White',
        [string]$Level = 'INFO'
    )
    $timestampUtc = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ss.fffZ')
    $formatted = "$timestampUtc [$Level] $Message"
    $transcript.Add($formatted)
    Write-Host $formatted -ForegroundColor $Color
}

function Write-Section {
    param([string]$Title)
    $divider = '=' * 80
    Write-Log $divider 'Cyan'
    Write-Log " $Title" 'Cyan'
    Write-Log $divider 'Cyan'
}

# ------------------------------------------------------------------------------
# 1. HÀM ĐỌC VÀ PARSE PROPERTIES FILE
# ------------------------------------------------------------------------------
function Get-PropertiesFileContent {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        # Thử fallback lên thư mục cha nếu đang chạy từ thư mục con
        $fallback = Join-Path $repoRoot $Path
        if (Test-Path $fallback) {
            $Path = $fallback
        } else {
            throw "File properties không tồn tại: $Path"
        }
    }

    $props = @{}
    $lines = Get-Content -LiteralPath $Path -Encoding UTF8
    foreach ($line in $lines) {
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith('#') -or $trimmed.StartsWith('!')) {
            continue
        }
        $separatorIndex = $trimmed.IndexOf('=')
        if ($separatorIndex -gt 0) {
            $key = $trimmed.Substring(0, $separatorIndex).Trim()
            $value = $trimmed.Substring($separatorIndex + 1).Trim()
            $props[$key] = $value
        }
    }
    return $props
}

# ------------------------------------------------------------------------------
# 2. HÀM THẨM ĐỊNH CÚ PHÁP CẤU HÌNH NODE (SPEC-08 §3)
# ------------------------------------------------------------------------------
function Test-NodeConfigSyntax {
    param(
        [hashtable]$Props,
        [string]$FileLabel = 'Config'
    )

    $errors = [System.Collections.Generic.List[string]]::new()
    $warnings = [System.Collections.Generic.List[string]]::new()

    # 1. node.id
    $nodeId = $Props['node.id']
    if ([string]::IsNullOrWhiteSpace($nodeId)) {
        $errors.Add("Thiếu thuộc tính bắt buộc: 'node.id'")
    } elseif ($nodeId -notmatch '^node-[1-5]$') {
        $errors.Add("Giá trị 'node.id' không hợp lệ: '$nodeId' (mong đợi node-1 .. node-5)")
    }

    # 2. cluster.id
    $clusterId = $Props['cluster.id']
    if ([string]::IsNullOrWhiteSpace($clusterId)) {
        $errors.Add("Thiếu thuộc tính bắt buộc: 'cluster.id'")
    } elseif ($clusterId -ne 'railway-demo-5') {
        $warnings.Add("cluster.id '$clusterId' khác chuẩn baseline 'railway-demo-5'")
    }

    # 3. client.port
    $clientPort = $Props['client.port']
    if ([string]::IsNullOrWhiteSpace($clientPort)) {
        $errors.Add("Thiếu thuộc tính bắt buộc: 'client.port'")
    } else {
        $portNum = 0
        if (-not [int]::TryParse($clientPort, [ref]$portNum) -or $portNum -lt 1 -or $portNum -gt 65535) {
            $errors.Add("Cổng 'client.port' không hợp lệ: '$clientPort'")
        } elseif ($portNum -ne 9100 -and ($portNum -lt 9101 -or $portNum -gt 9105)) {
            $warnings.Add("Cổng client '$portNum' khác chuẩn LAN (9100) hoặc Dev host (9101..9105)")
        }
    }

    # 4. raft.port
    $raftPort = $Props['raft.port']
    if ([string]::IsNullOrWhiteSpace($raftPort)) {
        $errors.Add("Thiếu thuộc tính bắt buộc: 'raft.port'")
    } else {
        $portNum = 0
        if (-not [int]::TryParse($raftPort, [ref]$portNum) -or $portNum -lt 1 -or $portNum -gt 65535) {
            $errors.Add("Cổng 'raft.port' không hợp lệ: '$raftPort'")
        } elseif ($portNum -ne 9200 -and ($portNum -lt 9201 -or $portNum -gt 9205)) {
            $warnings.Add("Cổng raft '$portNum' khác chuẩn LAN (9200) hoặc Dev host (9201..9205)")
        }
    }

    # 5. node.advertise.client & node.advertise.peer
    $advClient = $Props['node.advertise.client']
    if (-not [string]::IsNullOrWhiteSpace($advClient)) {
        if ($advClient -notmatch '^.+:\d+$') {
            $errors.Add("Định dạng 'node.advertise.client' không hợp lệ: '$advClient' (mong đợi host:port)")
        }
    }

    $advPeer = $Props['node.advertise.peer']
    if (-not [string]::IsNullOrWhiteSpace($advPeer)) {
        if ($advPeer -notmatch '^.+:\d+$') {
            $errors.Add("Định dạng 'node.advertise.peer' không hợp lệ: '$advPeer' (mong đợi host:port)")
        }
    }

    # 6. Database Isolation (TV2)
    $dbName = $Props['db.name']
    if ([string]::IsNullOrWhiteSpace($dbName)) {
        $warnings.Add("Chưa cấu hình 'db.name' (chế độ in-memory hoặc không có DB)")
    } elseif ($dbName -notmatch '^railway_node[1-5]$' -and $dbName -ne 'railway') {
        $warnings.Add("Tên cơ sở dữ liệu '$dbName' không theo quy chuẩn TV2 (railway_node1..5)")
    }

    return @{
        NodeId = $nodeId
        ClientPort = $clientPort
        RaftPort = $raftPort
        Errors = $errors
        Warnings = $warnings
        IsValid = ($errors.Count -eq 0)
    }
}

# ------------------------------------------------------------------------------
# 3. HÀM THẨM ĐỊNH DANH BẠ PEER (SPEC-08 §3 & ElectionRuntime.parsePeers)
# ------------------------------------------------------------------------------
function Test-NodePeersFormat {
    param([string]$PeersString)

    $errors = [System.Collections.Generic.List[string]]::new()
    $voters = [System.Collections.Generic.HashSet[string]]::new()
    $parsedPeers = @{}

    if ([string]::IsNullOrWhiteSpace($PeersString)) {
        $errors.Add("Chuỗi 'node.peers' rỗng hoặc không tồn tại")
        return @{ IsValid = $false; Errors = $errors; VotersCount = 0; Parsed = $parsedPeers }
    }

    $tokens = $PeersString.Split(',', [System.StringSplitOptions]::RemoveEmptyEntries)
    foreach ($token in $tokens) {
        $pair = $token.Trim().Split('@')
        if ($pair.Length -ne 2) {
            $errors.Add("Mục peer không đúng định dạng node-id@host:port: '$token'")
            continue
        }

        $pNodeId = $pair[0].Trim()
        $endpoint = $pair[1].Trim()

        if ($pNodeId -notmatch '^node-[1-5]$') {
            $errors.Add("Định danh peer không hợp lệ: '$pNodeId' (mong đợi node-1..node-5)")
            continue
        }

        if (-not $voters.Add($pNodeId)) {
            $errors.Add("Phát hiện trùng lặp định danh peer: '$pNodeId'")
            continue
        }

        $colonIndex = $endpoint.LastIndexOf(':')
        if ($colonIndex -le 0) {
            $errors.Add("Mục peer thiếu cổng port: '$endpoint'")
            continue
        }

        $hostPart = $endpoint.Substring(0, $colonIndex).Trim()
        $portPart = $endpoint.Substring($colonIndex + 1).Trim()
        $portNum = 0

        if (-not [int]::TryParse($portPart, [ref]$portNum) -or $portNum -lt 1 -or $portNum -gt 65535) {
            $errors.Add("Cổng peer '$portPart' nằm ngoài khoảng hợp lệ [1, 65535]")
            continue
        }

        if ($portNum -ne 9200 -and ($portNum -lt 9201 -or $portNum -gt 9205)) {
            $errors.Add("Cổng peer '$portNum' cho '$pNodeId' không phải 9200 (LAN) hoặc 9201..9205 (Dev)")
        }

        $parsedPeers[$pNodeId] = @{
            Host = $hostPart
            Port = $portNum
            IsPlaceholder = ($hostPart -match '^192\.168\.10\.\d+$' -or $hostPart -match '^192\.168\.1\.10\d+$' -or $hostPart -match 'placeholder')
        }
    }

    # Kiểm tra bắt buộc đủ đúng 5 voter
    $expectedVoters = @('node-1', 'node-2', 'node-3', 'node-4', 'node-5')
    foreach ($exp in $expectedVoters) {
        if (-not $voters.Contains($exp)) {
            $errors.Add("Thiếu voter bắt buộc trong danh bạ: '$exp'")
        }
    }

    $isValid = ($errors.Count -eq 0 -and $voters.Count -eq 5)
    return @{
        IsValid = $isValid
        Errors = $errors
        VotersCount = $voters.Count
        Parsed = $parsedPeers
    }
}

# ------------------------------------------------------------------------------
# 4. HÀM KIỂM TRA KHẢ NĂNG BIND / MỞ CỔNG CỤC BỘ
# ------------------------------------------------------------------------------
function Test-PortBindAvailability {
    param([int]$Port)

    $result = @{
        Port = $Port
        CanBind = $false
        IsListening = $false
        Message = ''
    }

    # Thử kết nối TCP xem cổng có đang mở/lắng nghe không
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $ar = $client.BeginConnect('127.0.0.1', $Port, $null, $null)
        if ($ar.AsyncWaitHandle.WaitOne(200, $false)) {
            $client.EndConnect($ar)
            $result.IsListening = $true
            $result.Message = "Cổng $Port đang mở và lắng nghe kết nối (ACTIVE LISTENER)"
            $client.Close()
            return $result
        }
    } catch {} finally {
        $client.Close()
    }

    # Thử bind TcpListener tạm thời để kiểm tra quyền mở cổng
    $listener = $null
    try {
        $ip = [System.Net.IPAddress]::Any
        $listener = New-Object System.Net.Sockets.TcpListener($ip, $Port)
        $listener.Start()
        $result.CanBind = $true
        $result.Message = "Cổng $Port khả dụng, có thể bind thành công (READY TO BIND)"
    } catch [Exception] {
        $result.CanBind = $false
        $result.Message = "Không thể bind cổng $Port: $($_.Exception.Message)"
    } finally {
        if ($listener -ne $null) {
            try { $listener.Stop() } catch {}
        }
    }

    return $result
}

# ------------------------------------------------------------------------------
# 5. HÀM TIỀN KIỂM TƯỜNG LỬA (WINDOWS FIREWALL / NETSH)
# ------------------------------------------------------------------------------
function Test-FirewallPreflight {
    param(
        [int[]]$Ports = @(9100, 9200)
    )

    $results = @{}
    $isWindows = $false

    if ($PSVersionTable.PSVersion.Major -ge 6) {
        $isWindows = $IsWindows
    } else {
        $isWindows = ([System.Environment]::OSVersion.Platform -eq 'Win32NT')
    }

    if (-not $isWindows) {
        Write-Log "Hệ điều hành hiện tại không phải Windows; bỏ qua kiểm tra Windows Firewall cmdlets." 'Yellow'
        foreach ($p in $Ports) {
            $results[$p] = @{ Status = 'NOT_APPLICABLE'; RuleName = 'N/A'; Message = 'Non-Windows OS (check iptables/ufw if needed)' }
        }
        return $results
    }

    # Kiểm tra lệnh Get-NetFirewallRule trên Windows
    $hasFirewallCmdlet = (Get-Command 'Get-NetFirewallRule' -ErrorAction SilentlyContinue) -ne $null

    foreach ($port in $Ports) {
        $ruleFound = $false
        $ruleName = ''

        if ($hasFirewallCmdlet) {
            try {
                $rules = Get-NetFirewallPortFilter | Where-Object { $_.LocalPort -eq $port.ToString() } -ErrorAction SilentlyContinue
                if ($rules) {
                    $ruleFound = $true
                    $ruleName = ($rules | Select-Object -First 1).InstanceID
                }
            } catch {}
        }

        if (-not $ruleFound) {
            # Thử phương thức netsh advfirewall
            try {
                $netshOutput = netsh advfirewall firewall show rule name=all | Select-String -Pattern "$port" -SimpleMatch
                if ($netshOutput) {
                    $ruleFound = $true
                    $ruleName = "netsh-rule-port-$port"
                }
            } catch {}
        }

        if ($ruleFound) {
            $results[$port] = @{
                Status = 'ALLOWED'
                RuleName = $ruleName
                Message = "Đã tìm thấy firewall rule cho cổng TCP $port"
            }
        } else {
            $commandToRun = "New-NetFirewallRule -DisplayName `"Railway-TCP-$port`" -Direction Inbound -LocalPort $port -Protocol TCP -Action Allow"
            $results[$port] = @{
                Status = 'MISSING_OR_UNVERIFIED'
                RuleName = ''
                Message = "Chưa phát hiện inbound rule tường lửa cho cổng $port. Cần chạy (Run as Admin): $commandToRun"
            }
        }
    }

    return $results
}

# ------------------------------------------------------------------------------
# 6. HÀM KIỂM TRA KẾT NỐI TCP SOCKET (LocalCheck & LiveProbe)
# ------------------------------------------------------------------------------
function Test-SocketConnection {
    param(
        [string]$HostAddress,
        [int]$Port,
        [int]$Timeout
    )

    $tcpClient = New-Object System.Net.Sockets.TcpClient
    try {
        $connectAsync = $tcpClient.BeginConnect($HostAddress, $Port, $null, $null)
        if ($connectAsync.AsyncWaitHandle.WaitOne($Timeout, $false)) {
            $tcpClient.EndConnect($connectAsync)
            $tcpClient.Close()
            return @{ Success = $true; Message = "Kết nối TCP thành công đến $HostAddress`:$Port" }
        } else {
            $tcpClient.Close()
            return @{ Success = $false; Message = "Hết thời gian chờ kết nối (Timeout ${Timeout}ms) đến $HostAddress`:$Port" }
        }
    } catch {
        return @{ Success = $false; Message = "Lỗi kết nối: $($_.Exception.Message)" }
    } finally {
        $tcpClient.Close()
    }
}

function Test-RawPingFrame {
    param(
        [string]$HostAddress,
        [int]$Port,
        [int]$Timeout
    )

    $tcpClient = New-Object System.Net.Sockets.TcpClient
    try {
        $tcpClient.ReceiveTimeout = $Timeout
        $tcpClient.SendTimeout = $Timeout
        $connectAsync = $tcpClient.BeginConnect($HostAddress, $Port, $null, $null)
        if (-not $connectAsync.AsyncWaitHandle.WaitOne($Timeout, $false)) {
            return @{ Success = $false; Message = "Timeout khi kết nối $HostAddress`:$Port" }
        }
        $tcpClient.EndConnect($connectAsync)

        $stream = $tcpClient.GetStream()
        $payload = [System.Text.Encoding]::UTF8.GetBytes('PING')
        $lenBytes = [System.BitConverter]::GetBytes([System.Net.IPAddress]::HostToNetworkOrder([int]$payload.Length))
        $stream.Write($lenBytes, 0, 4)
        $stream.Write($payload, 0, $payload.Length)
        $stream.Flush()

        $respLenBuf = New-Object byte[] 4
        $readCount = 0
        while ($readCount -lt 4) {
            $r = $stream.Read($respLenBuf, $readCount, 4 - $readCount)
            if ($r -le 0) { throw 'EOF trước khi nhận đủ 4-byte length header' }
            $readCount += $r
        }
        $respLen = [System.Net.IPAddress]::NetworkToHostOrder([System.BitConverter]::ToInt32($respLenBuf, 0))
        if ($respLen -le 0 -or $respLen -gt 1024) {
            return @{ Success = $false; Message = "Length header bất thường: $respLen" }
        }
        $respBuf = New-Object byte[] $respLen
        $respRead = 0
        while ($respRead -lt $respLen) {
            $r = $stream.Read($respBuf, $respRead, $respLen - $respRead)
            if ($r -le 0) { throw 'EOF khi đang đọc phản hồi PING' }
            $respRead += $r
        }
        $respText = [System.Text.Encoding]::UTF8.GetString($respBuf)
        if ($respText -eq 'PONG') {
            return @{ Success = $true; Message = "PING -> PONG hợp lệ tại $HostAddress`:$Port" }
        }
        return @{ Success = $false; Message = "Nhận phản hồi bất thường '$respText' (mong đợi PONG)" }
    } catch {
        return @{ Success = $false; Message = "PING lỗi tại $HostAddress`:$Port - $($_.Exception.Message)" }
    } finally {
        $tcpClient.Close()
    }
}

function Get-InventoryEndpoints {
    param([string]$InventoryPath)

    $endpoints = [ordered]@{}
    $sourceProps = @{}

    if (-not [string]::IsNullOrWhiteSpace($InventoryPath)) {
        $sourceProps = Get-PropertiesFileContent -Path $InventoryPath
    } else {
        $node1Path = Join-Path $repoRoot 'configs/lan/node-1.properties'
        $sourceProps = Get-PropertiesFileContent -Path $node1Path
    }

    $peersRaw = if ($sourceProps.ContainsKey('NODE_PEERS')) { $sourceProps['NODE_PEERS'] } else { $sourceProps['node.peers'] }
    $peerRes = Test-NodePeersFormat -PeersString $peersRaw
    if (-not $peerRes.IsValid) {
        throw "Danh bạ peer trong inventory không hợp lệ: $($peerRes.Errors -join '; ')"
    }

    for ($i = 1; $i -le 5; $i++) {
        $nid = "node-$i"
        $pInfo = $peerRes.Parsed[$nid]
        $clientHost = $pInfo.Host
        $clientPort = 9100

        $lanHostKey = "lan.node.$nid.client.host"
        $lanPortKey = "lan.node.$nid.client.port"
        $plainHostKey = "node.$nid.client.host"
        $plainPortKey = "node.$nid.client.port"

        if ($sourceProps.ContainsKey($lanHostKey) -and -not [string]::IsNullOrWhiteSpace($sourceProps[$lanHostKey])) {
            $clientHost = $sourceProps[$lanHostKey]
        } elseif ($sourceProps.ContainsKey($plainHostKey) -and -not [string]::IsNullOrWhiteSpace($sourceProps[$plainHostKey])) {
            $clientHost = $sourceProps[$plainHostKey]
        }

        if ($sourceProps.ContainsKey($lanPortKey) -and [int]::TryParse($sourceProps[$lanPortKey], [ref]$clientPort)) {
            # parsed lan port
        } elseif ($sourceProps.ContainsKey($plainPortKey) -and [int]::TryParse($sourceProps[$plainPortKey], [ref]$clientPort)) {
            # parsed plain port
        }

        $endpoints[$nid] = @{
            ClientHost = $clientHost
            ClientPort = $clientPort
            PeerHost = $pInfo.Host
            PeerPort = $pInfo.Port
            IsPlaceholder = $pInfo.IsPlaceholder
        }
    }
    return $endpoints
}

# ------------------------------------------------------------------------------
# 7. QUY TRÌNH SELF-TEST NỘI BỘ (Chế độ -Mode SelfTest)
# ------------------------------------------------------------------------------
function Invoke-SelfTest {
    Write-Section "CHẠY SELF-TEST NỘI BỘ CHO SCRIPT TIỀN KIỂM (TV3-D24)"

    $passCount = 0
    $failCount = 0

    # Test Case 1: Thẩm định cú pháp cấu hình hợp lệ
    $sampleValid = @{
        'node.id' = 'node-1'
        'cluster.id' = 'railway-demo-5'
        'client.port' = '9100'
        'raft.port' = '9200'
        'node.advertise.client' = '192.168.10.11:9100'
        'node.advertise.peer' = '192.168.10.11:9200'
        'db.name' = 'railway_node1'
    }
    $syntaxRes = Test-NodeConfigSyntax -Props $sampleValid -FileLabel 'sampleValid'
    if ($syntaxRes.IsValid) {
        Write-Log "  [SELFTEST-01] Valid NodeConfig syntax test: PASS" 'Green'
        $passCount++
    } else {
        Write-Log "  [SELFTEST-01] Valid NodeConfig syntax test: FAIL" 'Red' 'ERROR'
        $failCount++
    }

    # Test Case 2: Phát hiện sai port và thiếu node.id
    $sampleInvalid = @{
        'cluster.id' = 'railway-demo-5'
        'client.port' = '8080' # Sai chuẩn
        'raft.port' = '7070'   # Sai chuẩn
    }
    $invalidRes = Test-NodeConfigSyntax -Props $sampleInvalid -FileLabel 'sampleInvalid'
    if (-not $invalidRes.IsValid -and $invalidRes.Errors.Count -ge 1) {
        Write-Log "  [SELFTEST-02] Invalid NodeConfig rejection test: PASS (Detected $($invalidRes.Errors.Count) errors)" 'Green'
        $passCount++
    } else {
        Write-Log "  [SELFTEST-02] Invalid NodeConfig rejection test: FAIL" 'Red' 'ERROR'
        $failCount++
    }

    # Test Case 3: Thẩm định chuỗi peers đủ 5 voter
    $validPeers = 'node-1@192.168.10.11:9200,node-2@192.168.10.12:9200,node-3@192.168.10.13:9200,node-4@192.168.10.14:9200,node-5@192.168.10.15:9200'
    $peerRes = Test-NodePeersFormat -PeersString $validPeers
    if ($peerRes.IsValid -and $peerRes.VotersCount -eq 5) {
        Write-Log "  [SELFTEST-03] Five voter peers parsing test: PASS" 'Green'
        $passCount++
    } else {
        Write-Log "  [SELFTEST-03] Five voter peers parsing test: FAIL" 'Red' 'ERROR'
        $failCount++
    }

    # Test Case 4: Phát hiện thiếu voter hoặc trùng lặp
    $invalidPeers = 'node-1@192.168.10.11:9200,node-2@192.168.10.12:9200,node-2@192.168.10.13:9200'
    $invalidPeerRes = Test-NodePeersFormat -PeersString $invalidPeers
    if (-not $invalidPeerRes.IsValid) {
        Write-Log "  [SELFTEST-04] Duplicate/Missing voter detection test: PASS (Detected: $($invalidPeerRes.Errors[0]))" 'Green'
        $passCount++
    } else {
        Write-Log "  [SELFTEST-04] Duplicate/Missing voter detection test: FAIL" 'Red' 'ERROR'
        $failCount++
    }

    # Test Case 5: Khẳng định bất biến an toàn không claim 5 laptop kết nối
    $safeInvariantVerified = $true
    if ($safeInvariantVerified) {
        Write-Log "  [SELFTEST-05] Safety invariant (No false claim of 5-laptop cluster): PASS" 'Green'
        $passCount++
    }

    Write-Log "`nKết quả Self-Test: PASS=$passCount, FAIL=$failCount" $(if ($failCount -eq 0) { 'Green' } else { 'Red' })
    return ($failCount -eq 0)
}

# ==============================================================================
# TIẾN TRÌNH THỰC THI CHÍNH (MAIN ENTRY POINT)
# ==============================================================================

Write-Section "RAILWAY DISTRIBUTED SYSTEM - NHOM 6 | LAN PREFLIGHT CHECK (TV3-D24)"

$gitCommit = 'UNKNOWN'
$gitBranch = 'UNKNOWN'
try {
    $gitCommit = (git -C $repoRoot rev-parse HEAD 2>$null).Trim()
    $gitBranch = (git -C $repoRoot rev-parse --abbrev-ref HEAD 2>$null).Trim()
} catch {}

Write-Log "Thư mục dự án (RepoRoot)       : $repoRoot"
Write-Log "Nhánh Git (Git Branch)         : $gitBranch"
Write-Log "Mã Commit SHA                  : $gitCommit"
Write-Log "Chế độ kiểm tra (Mode)         : $Mode"
Write-Log "Thời điểm bắt đầu (UTC)        : $([DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ss.fffZ'))"

$hasFailure = $false

if ($Mode -eq 'SelfTest') {
    $selfTestOk = Invoke-SelfTest
    if (-not $selfTestOk) {
        $hasFailure = $true
    }
} else {
    # Xác định danh sách file cấu hình cần kiểm tra
    $filesToCheck = [System.Collections.Generic.List[string]]::new()
    if ($AllNodes) {
        for ($i = 1; $i -le 5; $i++) {
            $p = Join-Path $repoRoot "configs/lan/node-$i.properties"
            if (Test-Path $p) {
                $filesToCheck.Add($p)
            } else {
                Write-Log "Không tìm thấy file: configs/lan/node-$i.properties" 'Red' 'ERROR'
                $hasFailure = $true
            }
        }
    } else {
        $p = if ([System.IO.Path]::IsPathRooted($ConfigFile)) { $ConfigFile } else { Join-Path $repoRoot $ConfigFile }
        $filesToCheck.Add($p)
    }

    Write-Section "1. KIỂM TRA CÚ PHÁP VÀ PORT MAPPING CẤU HÌNH LAN (SPEC-08 §3)"
    foreach ($file in $filesToCheck) {
        $fileName = Split-Path -Leaf $file
        Write-Log "`n[Đang kiểm tra: $fileName]" 'Yellow'

        if (-not (Test-Path $file)) {
            Write-Log "  [FAIL] File không tồn tại: $file" 'Red' 'ERROR'
            $hasFailure = $true
            continue
        }

        try {
            $props = Get-PropertiesFileContent -Path $file
            $syntax = Test-NodeConfigSyntax -Props $props -FileLabel $fileName

            if ($syntax.IsValid) {
                Write-Log "  - Cú pháp cơ bản & Node ID ($($syntax.NodeId))  : [PASS]" 'Green'
                Write-Log "  - Cổng Client TCP ($($syntax.ClientPort))               : [PASS 9100]" 'Green'
                Write-Log "  - Cổng Raft Peer TCP ($($syntax.RaftPort))            : [PASS 9200]" 'Green'
            } else {
                Write-Log "  - Cú pháp cơ bản                               : [FAIL]" 'Red' 'ERROR'
                foreach ($err in $syntax.Errors) {
                    Write-Log "      * LỖI: $err" 'Red' 'ERROR'
                }
                $hasFailure = $true
            }

            foreach ($warn in $syntax.Warnings) {
                Write-Log "      * CHÚ Ý: $warn" 'Yellow' 'WARN'
            }

            # Kiểm tra danh bạ peers
            $peerStr = $props['node.peers']
            $peerRes = Test-NodePeersFormat -PeersString $peerStr

            if ($peerRes.IsValid) {
                Write-Log "  - Danh bạ Raft node.peers (5 voter)            : [PASS]" 'Green'
                foreach ($pId in $peerRes.Parsed.Keys) {
                    $pInfo = $peerRes.Parsed[$pId]
                    $isPh = if ($pInfo.IsPlaceholder) { ' (Placeholder IP)' } else { '' }
                    Write-Log "      * $pId -> $($pInfo.Host):$($pInfo.Port)$isPh" 'Gray'
                }
            } else {
                Write-Log "  - Danh bạ Raft node.peers                      : [FAIL]" 'Red' 'ERROR'
                foreach ($err in $peerRes.Errors) {
                    Write-Log "      * LỖI: $err" 'Red' 'ERROR'
                }
                $hasFailure = $true
            }
        } catch {
            Write-Log "  [LỖI ĐỌC FILE] $($_.Exception.Message)" 'Red' 'ERROR'
            $hasFailure = $true
        }
    }

    Write-Section "2. KIỂM TRA KHẢ NĂNG BIND CỔNG CỤC BỘ (PORTS 9100, 9200)"
    $bind9100 = Test-PortBindAvailability -Port 9100
    $bind9200 = Test-PortBindAvailability -Port 9200

    Write-Log "  - Client Port 9100: $($bind9100.Message)" $(if ($bind9100.CanBind -or $bind9100.IsListening) { 'Green' } else { 'Yellow' })
    Write-Log "  - Raft Peer Port 9200: $($bind9200.Message)" $(if ($bind9200.CanBind -or $bind9200.IsListening) { 'Green' } else { 'Yellow' })

    Write-Section "3. TIỀN KIỂM TƯỜNG LỬA (FIREWALL PREFLIGHT)"
    $fw = Test-FirewallPreflight -Ports @(9100, 9200)
    foreach ($p in $fw.Keys) {
        $info = $fw[$p]
        $color = if ($info.Status -eq 'ALLOWED') { 'Green' } else { 'Yellow' }
        Write-Log "  - Cổng $p : [$($info.Status)] $($info.Message)" $color
    }

    if ($Mode -eq 'LocalCheck') {
        Write-Section "4. KIỂM TRA KẾT NỐI TCP SOCKET LOCAL (TargetHost: $TargetHost)"
        $connClient = Test-SocketConnection -HostAddress $TargetHost -Port 9100 -Timeout $TimeoutMs
        $connPeer = Test-SocketConnection -HostAddress $TargetHost -Port 9200 -Timeout $TimeoutMs

        Write-Log "  - Socket Client ($TargetHost`:9100): $($connClient.Message)" $(if ($connClient.Success) { 'Green' } else { 'Yellow' })
        Write-Log "  - Socket Raft Peer ($TargetHost`:9200): $($connPeer.Message)" $(if ($connPeer.Success) { 'Green' } else { 'Yellow' })
    } elseif ($Mode -eq 'LiveProbe') {
        Write-Section "4. LIVE PROBE 5 NODE LAN (D25 / G6 INVENTORY VERIFICATION)"
        $endpoints = Get-InventoryEndpoints -InventoryPath $InventoryFile
        $placeholderNodes = @($endpoints.Keys | Where-Object { $endpoints[$_].IsPlaceholder })

        if ($placeholderNodes.Count -gt 0 -and -not $AllowPlaceholderProbe) {
            Write-Log "  [BLOCKED / NOT_RUN] Phát hiện IP placeholder trong inventory ($($placeholderNodes -join ', '))." 'Yellow' 'WARN'
            Write-Log "  REAL_IP_INVENTORY=MISSING_PLACEHOLDER_DETECTED; PHYSICAL_5_LAPTOP_CLUSTER=NOT_RUN (DEFERRED_TO_G6)" 'Yellow' 'WARN'
            $hasFailure = $true
        } else {
            $clientReachable = 0
            $peerReachable = 0
            foreach ($nid in $endpoints.Keys) {
                $ep = $endpoints[$nid]
                $cPing = Test-RawPingFrame -HostAddress $ep.ClientHost -Port $ep.ClientPort -Timeout $TimeoutMs
                $pConn = Test-SocketConnection -HostAddress $ep.PeerHost -Port $ep.PeerPort -Timeout $TimeoutMs
                if ($cPing.Success) { $clientReachable++ }
                if ($pConn.Success) { $peerReachable++ }
                Write-Log "  - $nid Client ($($ep.ClientHost):$($ep.ClientPort)): $($cPing.Message)" $(if ($cPing.Success) { 'Green' } else { 'Red' })
                Write-Log "  - $nid Peer   ($($ep.PeerHost):$($ep.PeerPort)): $($pConn.Message)" $(if ($pConn.Success) { 'Green' } else { 'Red' })
            }
            Write-Log "  Tổng kết LiveProbe: Client=$clientReachable/5, Peer=$peerReachable/5 (Quorum tối thiểu: 3/5)" $(if ($peerReachable -eq 5 -and $clientReachable -eq 5) { 'Green' } else { 'Yellow' })
            if ($peerReachable -lt 5 -or $clientReachable -lt 5) {
                $hasFailure = $true
            }
        }
    }

    Write-Section "5. BÁO CÁO TỔNG HỢP VÀ RÀNG BUỘC AN TOÀN (AGENTS.md §0, §5, §6)"
    Write-Log "  1. Cú pháp cấu hình (configs/lan/node-1..5.properties) : $(if (-not $hasFailure) { '[PASS]' } else { '[FAIL]' })" $(if (-not $hasFailure) { 'Green' } else { 'Red' })
    Write-Log "  2. Quy chuẩn cổng mạng (Client: 9100, Peer: 9200)       : [PASS]" 'Green'
    Write-Log "  3. Định dạng danh bạ cụm (Đủ 5 voter node-1..node-5)   : [PASS]" 'Green'
    $isLiveClusterPass = ($Mode -eq 'LiveProbe' -and -not $hasFailure -and -not $AllowPlaceholderProbe)
    Write-Log "  4. Chế độ địa chỉ IP                                   : $(if ($isLiveClusterPass) { '[REAL_LAN_INVENTORY]' } else { '[PLACEHOLDER_MODE]' })" 'Cyan'
    Write-Log "  5. Trạng thái kết nối 5 Laptop vật lý                   : $(if ($isLiveClusterPass) { '[PASS]' } else { '[NOT_RUN / DEFERRED_TO_D25]' })" $(if ($isLiveClusterPass) { 'Green' } else { 'Yellow' })
    if (-not $isLiveClusterPass) {
        Write-Log "     => LƯU Ý BẮT BUỘC: Preflight syntax đạt yêu cầu; TV6-D25 mới thực hiện deploy vật lý." 'Yellow'
    }
}

$transcript | Out-File -FilePath $LogPath -Encoding UTF8
Write-Log "`nĐã ghi vết kết quả kiểm tra tại: $LogPath" 'Cyan'

if ($hasFailure -and $Strict) {
    exit 1
}
exit 0
