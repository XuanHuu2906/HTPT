<#
.SYNOPSIS
    TV6-D12: collect real Docker logs, ports and local NODE_STATUS observations.
.DESCRIPTION
    Read-only. Does not start/stop nodes, write SQL, or certify Raft quorum.
    SearchOnly replays literal requestId search against previously collected node logs.
    Exit 0 = requested collection/search succeeded; 2 = incomplete runtime evidence.
.EXAMPLE
    .\scripts\collect-logs.ps1 -RequestId '<business-request-uuid>'
.EXAMPLE
    .\scripts\collect-logs.ps1 -SearchOnly -OutputDirectory '<saved-directory>' -RequestId '<uuid>'
#>
[CmdletBinding()]
param(
    [string]$ComposeFile = 'docker/compose.dev.yml',
    [string]$HostAddress = '127.0.0.1',
    [ValidateRange(100, 30000)][int]$TimeoutMs = 2000,
    [ValidateRange(1, 100000)][int]$Tail = 3000,
    [string]$OutputDirectory = '',
    [string]$LogPath = '',
    [ValidatePattern('^$|^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')]
    [string]$RequestId = '',
    [switch]$SearchOnly
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if (!$OutputDirectory) {
    $OutputDirectory = Join-Path $repoRoot ('artifacts/evidence/D12/collection-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
}
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
if (!$LogPath) { $LogPath = Join-Path $repoRoot 'artifacts/evidence/D12/TV6-D12-result.log' }
$LogPath = [IO.Path]::GetFullPath($LogPath)
$utf8 = [Text.UTF8Encoding]::new($false)
$report = [Collections.Generic.List[string]]::new()

function Save-Text([string]$Path, [string]$Content) {
    [IO.File]::WriteAllText($Path, $Content, $utf8)
}

function Report([string]$Message) {
    $report.Add($Message)
    Write-Output $Message
}

function Find-RequestTrace {
    $files = @(1..5 | ForEach-Object { Join-Path $OutputDirectory "node-$_.log" } |
        Where-Object { Test-Path -LiteralPath $_ -PathType Leaf })
    $matches = @()
    if ($files.Count) {
        $matches = @(Select-String -LiteralPath $files -Pattern $RequestId -SimpleMatch)
    }
    $nodes = @($matches | ForEach-Object { [IO.Path]::GetFileNameWithoutExtension($_.Path) } | Sort-Object -Unique)
    $lines = @("requestId=$RequestId", "matchedNodes=$($nodes.Count)/5; matchedLines=$($matches.Count)")
    $lines += @($matches | ForEach-Object { '{0}:{1}: {2}' -f [IO.Path]::GetFileName($_.Path), $_.LineNumber, $_.Line })
    Save-Text (Join-Path $OutputDirectory 'request-trace.log') ($lines -join "`n")
    return $nodes.Count
}

if ($SearchOnly) {
    if (!$RequestId -or !(Test-Path -LiteralPath $OutputDirectory -PathType Container)) {
        throw 'SearchOnly requires RequestId and an existing OutputDirectory.'
    }
    $matchedNodes = Find-RequestTrace
    Write-Output "Literal requestId search: $matchedNodes/5 nodes. See $OutputDirectory/request-trace.log"
    if ($matchedNodes -lt 2) { exit 2 }
    exit 0
}

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $LogPath) | Out-Null
$composePath = if ([IO.Path]::IsPathRooted($ComposeFile)) { $ComposeFile } else { Join-Path $repoRoot $ComposeFile }
$composeArgs = @('compose', '-f', $composePath, '--profile', 'nodes')

function Invoke-DockerCapture([string[]]$Arguments, [string]$Path) {
    # Native stderr is captured as evidence, not mistaken for a successful command.
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = & docker @Arguments 2>&1
        $code = $LASTEXITCODE
    } finally { $ErrorActionPreference = $previousPreference }
    Save-Text $Path (($output | ForEach-Object { $_.ToString() }) -join "`n")
    return $code
}

function Read-Exact($Stream, [int]$Count, $Watch) {
    $buffer = [byte[]]::new($Count)
    $offset = 0
    while ($offset -lt $Count) {
        $remaining = $TimeoutMs - [int]$Watch.ElapsedMilliseconds
        if ($remaining -le 0) { throw 'Response deadline exceeded' }
        $Stream.ReadTimeout = $remaining
        $received = $Stream.Read($buffer, $offset, $Count - $offset)
        if ($received -le 0) { throw 'Incomplete TCP frame' }
        $offset += $received
    }
    return ,$buffer
}

function Get-Status([string]$NodeId, [int]$Port) {
    $tcp = [Net.Sockets.TcpClient]::new()
    $watch = [Diagnostics.Stopwatch]::StartNew()
    try {
        $connection = $tcp.BeginConnect($HostAddress, $Port, $null, $null)
        try {
            if (!$connection.AsyncWaitHandle.WaitOne($TimeoutMs)) { throw 'Connect timeout' }
            $tcp.EndConnect($connection)
        } finally { $connection.AsyncWaitHandle.Close() }
        $stream = $tcp.GetStream()
        $stream.WriteTimeout = $TimeoutMs
        $messageId = [Guid]::NewGuid().ToString()
        $request = [ordered]@{
            protocolVersion = 1; messageId = $messageId; correlationId = $null
            type = 'NODE_STATUS'; senderId = 'monitor-tv6-log-collector'
            sentAt = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ss.fffZ'); payload = @{}
        }
        $body = [Text.Encoding]::UTF8.GetBytes(($request | ConvertTo-Json -Compress -Depth 8))
        $prefix = [BitConverter]::GetBytes([int]$body.Length)
        if ([BitConverter]::IsLittleEndian) { [Array]::Reverse($prefix) }
        $stream.Write($prefix, 0, 4)
        $stream.Write($body, 0, $body.Length)
        $header = Read-Exact $stream 4 $watch
        if ([BitConverter]::IsLittleEndian) { [Array]::Reverse($header) }
        $length = [BitConverter]::ToInt32($header, 0)
        if ($length -lt 1 -or $length -gt 1048576) { throw 'Invalid frame length' }
        $bytes = Read-Exact $stream $length $watch
        $raw = [Text.UTF8Encoding]::new($false, $true).GetString($bytes)
        Save-Text (Join-Path $OutputDirectory "$NodeId-status.json") $raw
        $response = $raw | ConvertFrom-Json
        if ($response.protocolVersion -ne 1 -or $response.correlationId -ne $messageId -or
            $response.senderId -ne $NodeId -or $response.type -ne 'NODE_STATUS_RESPONSE' -or
            $response.payload.nodeId -ne $NodeId) { throw 'Invalid status response/correlation/peer' }
        return $response.payload
    } finally { $tcp.Dispose() }
}

function Test-Port([int]$Port) {
    $tcp = [Net.Sockets.TcpClient]::new()
    try {
        $connection = $tcp.BeginConnect($HostAddress, $Port, $null, $null)
        try {
            if (!$connection.AsyncWaitHandle.WaitOne($TimeoutMs)) { return $false }
            $tcp.EndConnect($connection)
            return $true
        } finally { $connection.AsyncWaitHandle.Close() }
    } catch { return $false } finally { $tcp.Dispose() }
}

Report 'TV6-D12 | REAL LOG/STATUS COLLECTION | supplemental run, not historical D12 execution'
Report "CollectedAtUTC=$([DateTime]::UtcNow.ToString('o')); Host=$env:COMPUTERNAME"
Report "GitCommit=$(git -C $repoRoot rev-parse HEAD); Branch=$(git -C $repoRoot branch --show-current)"
Report "Compose=$composePath; Evidence=$OutputDirectory; RequestId=$RequestId"
Report "Command: scripts/collect-logs.ps1 -ComposeFile '$ComposeFile' -OutputDirectory '$OutputDirectory' -RequestId '$RequestId' -TimeoutMs $TimeoutMs -Tail $Tail"
Report 'Contract: SPEC-03 NODE_STATUS; diagnostics are local observations, not proof of quorum/commit.'
Report 'Legacy result replaced: offline/static config checks did not prove runtime logging or privacy.'

$dockerReady = $false
if (Get-Command docker -ErrorAction SilentlyContinue) {
    $infoCode = Invoke-DockerCapture @('info', '--format', '{{.ServerVersion}}') (Join-Path $OutputDirectory 'docker-version.log')
    $dockerReady = $infoCode -eq 0
}
$logCount = 0
$rows = @()
if ($dockerReady) {
    $psCode = Invoke-DockerCapture ($composeArgs + @('ps', '-a')) (Join-Path $OutputDirectory 'docker-ps.log')
    Report "Docker ps exit=$psCode; raw=docker-ps.log"
}

foreach ($number in 1..5) {
    $nodeId = "node-$number"
    $clientPort = 9100 + $number
    $peerPort = 9200 + $number
    $logCode = -1
    if ($dockerReady) {
        $logCode = Invoke-DockerCapture ($composeArgs + @('logs', '--no-color', '--no-log-prefix', '--timestamps', '--tail', "$Tail", "backend-node-$number")) (Join-Path $OutputDirectory "$nodeId.log")
        $containerIds = & docker @composeArgs ps -a -q "backend-node-$number"
        if ($LASTEXITCODE -eq 0 -and $containerIds) {
            $null = Invoke-DockerCapture (@('inspect', '--format', '{{.Id}} image={{.Image}} state={{.State.Status}} ports={{json .NetworkSettings.Ports}}') + @($containerIds)) (Join-Path $OutputDirectory "$nodeId-container.log")
        }
        if ($logCode -eq 0 -and (Get-Item -LiteralPath (Join-Path $OutputDirectory "$nodeId.log")).Length -gt 0) { $logCount++ }
    }
    $row = [ordered]@{ nodeId = $nodeId; clientPort = $clientPort; peerPort = $peerPort
        peerPortOpen = (Test-Port $peerPort); status = 'UNKNOWN'; role = $null; term = $null
        commitIndex = $null; lastApplied = $null; storageHealthy = $null; termField = $null; contract = 'NOT_RUN' }
    try {
        $payload = Get-Status $nodeId $clientPort
        $row.status = 'ONLINE'
        $row.role = $payload.role
        $row.commitIndex = $payload.commitIndex
        $row.lastApplied = $payload.lastApplied
        $row.storageHealthy = $payload.storageHealthy
        $fields = @($payload.PSObject.Properties.Name)
        $required = @('nodeId','role','term','leaderId','lastLogIndex','commitIndex','lastApplied','storageHealthy','peerConnectivity','observedAt')
        $missing = @($required | Where-Object { $_ -notin $fields })
        $extra = @($fields | Where-Object { $_ -notin $required })
        $peerKeys = @($payload.peerConnectivity.PSObject.Properties.Name)
        $expectedPeers = @(1..5 | ForEach-Object { "node-$_" } | Where-Object { $_ -ne $nodeId })
        $peerMismatch = @($expectedPeers | Where-Object { $_ -notin $peerKeys }).Count -gt 0 -or
            @($peerKeys | Where-Object { $_ -notin $expectedPeers }).Count -gt 0
        $row.contract = if ($missing.Count -or $extra.Count -or $peerMismatch) { 'GAP' } else { 'FIELD_SET_OK' }
        if ('term' -in $fields) { $row.term = $payload.term; $row.termField = 'term' }
        elseif ('currentTerm' -in $fields) { $row.term = $payload.currentTerm; $row.termField = 'currentTerm (legacy, not SPEC-03)' }
        Report "$nodeId status=$($row.status) role=$($row.role) term=$($row.term) commitIndex=$($row.commitIndex) lastApplied=$($row.lastApplied) client=$clientPort peer=$peerPort peerOpen=$($row.peerPortOpen) logsExit=$logCode"
        Report "  SPEC-03=$($row.contract); missing=$($missing -join ','); extra=$($extra -join ','); peerKeysMismatch=$peerMismatch; raw=$nodeId-status.json"
    } catch {
        $row.status = 'UNKNOWN'
        Report "$nodeId status=UNKNOWN role/term/index=null client=$clientPort error=$($_.Exception.Message) logsExit=$logCode"
    }
    $rows += [PSCustomObject]$row
}
$rows | Export-Csv -LiteralPath (Join-Path $OutputDirectory 'status-summary.csv') -NoTypeInformation -Encoding UTF8
$onlineCount = @($rows | Where-Object { $_.status -eq 'ONLINE' }).Count
Report "COLLECT-01 $(if ($logCount -eq 5) {'PASS'} else {'BLOCKED'}): nonempty real Docker node logs=$logCount/5"
Report "COLLECT-02 $(if ($onlineCount -eq 5) {'PASS'} else {'BLOCKED'}): correlated NODE_STATUS responses=$onlineCount/5; schema result is recorded separately."
$indexSamples = @($rows | Where-Object { $_.status -eq 'ONLINE' -and
    $null -ne $_.commitIndex -and $null -ne $_.lastApplied })
$indexErrors = @($indexSamples | Where-Object { $_.lastApplied -lt 0 -or $_.commitIndex -lt $_.lastApplied })
Report "OBSERVATION-CHECK: applied<=commit violations=$($indexErrors.Count) among $($indexSamples.Count) samples; not a Raft safety test."
$observedLeaders = @($rows | Where-Object { $_.status -eq 'ONLINE' -and $_.role -eq 'LEADER' })
Report "OBSERVED leaders=$($observedLeaders.Count): $(($observedLeaders | ForEach-Object { $_.nodeId + '@term=' + $_.term }) -join ','); sequential local samples, not an atomic cluster snapshot."
Report 'Quorum/safety/privacy: NOT_RUN. Reachable client ports, log patterns, or a config comment cannot prove these.'
$traceNodes = 0
if ($RequestId) {
    $traceNodes = Find-RequestTrace
    Report "COLLECT-03 $(if ($traceNodes -ge 2) {'PASS'} else {'BLOCKED'}): requestId=$RequestId found in $traceNodes node logs; see request-trace.log. Search uses literal Select-String -SimpleMatch."
} else { Report 'COLLECT-03 NOT_RUN: supply -RequestId to prove a cross-node business-request trace.' }
$probePath = Join-Path $OutputDirectory 'request-probe.log'
if ($RequestId -and (Test-Path -LiteralPath $probePath)) {
    $probeRecords = Get-Content -LiteralPath $probePath -Raw | ConvertFrom-Json
    $probes = @($probeRecords | Where-Object { $_.request.payload.requestId -eq $RequestId })
    foreach ($probe in $probes) {
        Report "READ-ONLY-PROBE $($probe.nodeId): type=$($probe.response.type) error=$($probe.response.payload.errorCode) message=$($probe.response.payload.message); client-side transcript, not a backend log trace."
    }
}
Report 'Failure scenario IDs (catalog only; fault actions were NOT_RUN in this collection):'
@('FS-01 TC-AC05A follower crash','FS-02 TC-AC05B two-node crash','FS-03 TC-AC06 majority loss',
  'FS-04 TC-AC07 leader crash/re-election','FS-05 TC-AC08 restart/catch-up',
  'FS-06 TC-AC09 partition 3-2','FS-07 TC-AC10 response drop/retry') | ForEach-Object { Report "  $_ : NOT_RUN" }
Report 'If COLLECT-03 is BLOCKED with working nodes, coordinate requestId runtime instrumentation with TV3/TV4/TV5; do not inject fake log lines.'
Report 'Current runtime uses java.util.logging for TCP/dispatcher paths; logback.xml MDC configuration alone is not evidence those paths emit requestId.'
Report "SourceScriptSHA256=$((Get-FileHash -LiteralPath $PSCommandPath -Algorithm SHA256).Hash)"
$hashes = Get-ChildItem -LiteralPath $OutputDirectory -File | Where-Object { $_.Name -ne 'sha256.log' } | Get-FileHash -Algorithm SHA256
Save-Text (Join-Path $OutputDirectory 'sha256.log') (($hashes | ForEach-Object { "$($_.Hash)  $([IO.Path]::GetFileName($_.Path))" }) -join "`n")
Save-Text $LogPath ($report -join "`n")
Write-Output "Result=$LogPath"
if ($logCount -ne 5 -or $onlineCount -ne 5 -or !$RequestId -or $traceNodes -lt 2) { exit 2 }
exit 0
