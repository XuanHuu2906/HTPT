<#
.SYNOPSIS
    TV6-D16 fault harness: stop/start 1-3 backend nodes and await Raft diagnostics.
.DESCRIPTION
    Default Mode=Fake: no Docker, sockets, SQL or actual fault injection.
    Run restores the selected nodes in finally, including on partial stop/timeout.
    Clean baseline means five healthy nodes and no leftover faults, NOT a DB reset.
    Never deletes volumes or runs down/up/rm. 3/5 live safety remains a separate G5 gate.
    Logs default to TEMP. SelfTest is embedded to keep the task in this single file.
    Exit 0 = runner/observation succeeded; exit 1 = failed. Never certifies booking safety.
.EXAMPLE
    .\scripts\cluster-fault.ps1 -Action SelfTest
.EXAMPLE
    .\scripts\cluster-fault.ps1 -NodeCount 3
.EXAMPLE
    .\scripts\cluster-fault.ps1 -Mode Live -Project docker -NodeIds node-2,node-4 -AllowLegacyCurrentTerm
.EXAMPLE
    .\scripts\cluster-fault.ps1 -Mode Live -Action Start -Project docker -NodeIds node-2,node-4 -AllowLegacyCurrentTerm
.NOTES
    Live expects an already created Compose project. Default uses compose.dev.yml.
    For D15's isolated fixture set ComposeFile=docker/compose.election-test.yml,
    ServicePrefix=node and Project=<existing test project>; keep its environment.
    Client host ports are discovered from that project's services, never guessed.
    Stop leaves the selected nodes stopped; Start restores them; Run does both.
    SPEC-03 uses term. Baseline currentTerm requires explicit AllowLegacyCurrentTerm.
#>
[CmdletBinding()]
param(
    [ValidateSet('Run', 'Stop', 'Start', 'SelfTest')][string]$Action = 'Run',
    [ValidateSet('Fake', 'Live')][string]$Mode = 'Fake',
    [ValidateRange(1, 3)][int]$NodeCount = 1,
    [string[]]$NodeIds = @(),
    [string]$ComposeFile = 'docker/compose.dev.yml',
    [ValidatePattern('^$|^[a-z0-9][a-z0-9_-]*$')][string]$Project = '',
    [ValidateSet('backend-node', 'node')][string]$ServicePrefix = 'backend-node',
    [ValidateRange(1, 120)][int]$WaitSeconds = 20,
    [ValidateRange(20, 2000)][int]$PollIntervalMs = 250,
    [ValidateRange(100, 5000)][int]$TimeoutMs = 750,
    [switch]$AllowLegacyCurrentTerm,
    [string]$LogPath = ''
)

$ErrorActionPreference = 'Stop'
$allNodes = @('node-1', 'node-2', 'node-3', 'node-4', 'node-5')
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if (!$LogPath) {
    $LogPath = Join-Path ([IO.Path]::GetTempPath()) ('railway-cluster-fault-' + [Guid]::NewGuid().ToString('N') + '.log')
}
$LogPath = [IO.Path]::GetFullPath($LogPath)
$transcript = [Collections.Generic.List[string]]::new()

function Write-FaultLog([string]$Message) {
    $line = [DateTime]::UtcNow.ToString('o') + ' ' + $Message
    $transcript.Add($line)
    Write-Host $line
}

function Get-Targets([string[]]$Ids, [int]$Count) {
    if (!$Ids.Count) {
        if ($Count -lt 1 -or $Count -gt 3) { throw 'Select 1-3 nodes.' }
        $Ids = @($allNodes | Select-Object -First $Count)
    }
    if ($Ids.Count -lt 1 -or $Ids.Count -gt 3 -or
        @($Ids | Sort-Object -Unique).Count -ne $Ids.Count -or
        @($Ids | Where-Object { $_ -cnotin $allNodes }).Count) {
        throw 'Select 1-3 unique node IDs from node-1 through node-5.'
    }
    return $Ids
}

function New-FaultContext([string]$AdapterMode) {
    $running = [Collections.Generic.HashSet[string]]::new()
    $allNodes | ForEach-Object { $null = $running.Add($_) }
    return @{
        Mode = $AdapterMode; Running = $running; Calls = [Collections.Generic.List[string]]::new()
        Term = 8L; Leader = 'node-5'; Commit = 43L; Ports = @{}
        Wait = $WaitSeconds; Interval = $PollIntervalMs
        FailStopOnce = $false; FailStart = $false; TimeoutAfterStop = $false
    }
}

function Invoke-Compose([string[]]$Arguments) {
    $path = if ([IO.Path]::IsPathRooted($ComposeFile)) { $ComposeFile } else { Join-Path $repoRoot $ComposeFile }
    $dockerArgs = @('compose', '-p', $Project, '-f', $path) + $Arguments
    $savedPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try { $output = & docker @dockerArgs 2>&1; $code = $LASTEXITCODE }
    finally { $ErrorActionPreference = $savedPreference }
    $text = ($output | ForEach-Object { $_.ToString() }) -join "`n"
    Write-FaultLog "DOCKER $($dockerArgs -join ' ') exit=$code`n$text"
    if ($code -ne 0) { throw "Docker command failed with exit $code" }
    return $text
}

function Resolve-ClientPorts($Context) {
    if ($Context.Mode -eq 'Fake') { return }
    foreach ($id in $allNodes) {
        $service = $ServicePrefix + '-' + $id.Substring(5)
        $binding = Invoke-Compose @('port', $service, '9100')
        $match = [regex]::Match($binding.Trim(), ':(\d+)\s*$')
        if (!$match.Success) { throw "Cannot resolve client port for $service" }
        $Context.Ports[$id] = [int]$match.Groups[1].Value
    }
    if (@($Context.Ports.Values | Sort-Object -Unique).Count -ne 5) { throw 'Client ports must identify five distinct nodes.' }
}

function Set-NodeFault($Context, [string]$Verb, [string[]]$Targets) {
    $Context.Calls.Add("$Verb $($Targets -join ',')")
    Write-FaultLog "FAULT mode=$($Context.Mode) action=$Verb nodes=$($Targets -join ',')"
    if ($Context.Mode -eq 'Live') {
        $services = @($Targets | ForEach-Object { $ServicePrefix + '-' + $_.Substring(5) })
        $arguments = if ($Verb -eq 'stop') { @('stop', '--timeout', '5') } else { @('start') }
        $null = Invoke-Compose ($arguments + $services)
        return
    }
    if ($Verb -eq 'start' -and $Context.FailStart) { throw 'FAKE start failed' }
    foreach ($id in $Targets) {
        if ($Verb -eq 'stop') {
            $null = $Context.Running.Remove($id)
            if ($Context.FailStopOnce) { $Context.FailStopOnce = $false; throw 'FAKE partial stop failed' }
        } else { $null = $Context.Running.Add($id) }
    }
    if ($Verb -eq 'stop') { $Context.Term++ }
    if ($Context.Running.Count -ge 3 -and !$Context.Running.Contains($Context.Leader)) {
        $Context.Leader = @($Context.Running | Sort-Object)[-1]
    }
}

function Read-FaultFrame($Stream, [int]$Length, $Watch) {
    $bytes = [byte[]]::new($Length)
    $offset = 0
    while ($offset -lt $Length) {
        $remaining = $TimeoutMs - [int]$Watch.ElapsedMilliseconds
        if ($remaining -le 0) { throw 'NODE_STATUS deadline exceeded' }
        $Stream.ReadTimeout = $remaining
        $read = $Stream.Read($bytes, $offset, $Length - $offset)
        if ($read -le 0) { throw 'Incomplete NODE_STATUS frame' }
        $offset += $read
    }
    return ,$bytes
}

function Get-LivePayload([string]$Id, [int]$Port) {
    $tcp = [Net.Sockets.TcpClient]::new()
    $watch = [Diagnostics.Stopwatch]::StartNew()
    try {
        $connection = $tcp.BeginConnect('127.0.0.1', $Port, $null, $null)
        try {
            if (!$connection.AsyncWaitHandle.WaitOne($TimeoutMs)) { throw 'NODE_STATUS connect timeout' }
            $tcp.EndConnect($connection)
        } finally { $connection.AsyncWaitHandle.Close() }
        $stream = $tcp.GetStream()
        $stream.WriteTimeout = $TimeoutMs
        $messageId = [Guid]::NewGuid().ToString()
        $request = [ordered]@{ protocolVersion = 1; messageId = $messageId; correlationId = $null
            type = 'NODE_STATUS'; senderId = 'monitor-cluster-fault'
            sentAt = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ss.fffZ'); payload = @{} }
        $body = [Text.Encoding]::UTF8.GetBytes(($request | ConvertTo-Json -Compress -Depth 6))
        $header = [BitConverter]::GetBytes([int]$body.Length)
        if ([BitConverter]::IsLittleEndian) { [Array]::Reverse($header) }
        $stream.Write($header, 0, 4)
        $stream.Write($body, 0, $body.Length)
        $prefix = Read-FaultFrame $stream 4 $watch
        if ([BitConverter]::IsLittleEndian) { [Array]::Reverse($prefix) }
        $length = [BitConverter]::ToInt32($prefix, 0)
        if ($length -lt 1 -or $length -gt 1048576) { throw 'Invalid NODE_STATUS frame size' }
        $raw = [Text.UTF8Encoding]::new($false, $true).GetString((Read-FaultFrame $stream $length $watch))
        $response = $raw | ConvertFrom-Json
        if ($response.protocolVersion -ne 1 -or $response.correlationId -ne $messageId -or
            $response.senderId -cne $Id -or $response.type -ne 'NODE_STATUS_RESPONSE' -or
            $response.payload.nodeId -cne $Id) { throw 'NODE_STATUS correlation/type/peer mismatch' }
        return $response.payload
    } finally { $tcp.Dispose() }
}

function Get-RaftValues($Payload, [bool]$AcceptLegacy = $AllowLegacyCurrentTerm) {
    $termField = 'term'
    if (!$Payload.PSObject.Properties['term']) {
        if (!$AcceptLegacy -or !$Payload.PSObject.Properties['currentTerm']) {
            throw 'SPEC-03 term missing; legacy currentTerm requires explicit switch.'
        }
        $termField = 'currentTerm'
    }
    foreach ($field in @($termField, 'commitIndex', 'lastApplied')) {
        $value = $Payload.$field
        if (($value -isnot [long] -and $value -isnot [int]) -or $value -lt 0) { throw "Untrusted $field" }
    }
    if ($Payload.storageHealthy -isnot [bool] -or !$Payload.storageHealthy -or
        $Payload.role -cnotin @('FOLLOWER', 'CANDIDATE', 'LEADER') -or
        $Payload.lastApplied -gt $Payload.commitIndex) { throw 'Uninitialized/unhealthy Raft observation' }
    return [PSCustomObject]@{ role = $Payload.role; term = $Payload.$termField
        commitIndex = $Payload.commitIndex; lastApplied = $Payload.lastApplied; termField = $termField }
}

function Get-FaultSnapshot($Context) {
    foreach ($id in $allNodes) {
        try {
            if ($Context.Mode -eq 'Fake') {
                if (!$Context.Running.Contains($id) -or ($Context.TimeoutAfterStop -and $Context.Running.Count -lt 5)) {
                    throw 'FAKE unavailable'
                }
                $payload = [PSCustomObject]@{ nodeId = $id; role = $(if ($id -eq $Context.Leader) {'LEADER'} else {'FOLLOWER'})
                    term = [long]$Context.Term; commitIndex = [long]$Context.Commit; lastApplied = [long]$Context.Commit; storageHealthy = $true }
            } else { $payload = Get-LivePayload $id $Context.Ports[$id] }
            $values = Get-RaftValues $payload
            [PSCustomObject]@{ nodeId = $id; online = $true; role = $values.role; term = $values.term
                commitIndex = $values.commitIndex; lastApplied = $values.lastApplied; termField = $values.termField; error = $null }
        } catch {
            [PSCustomObject]@{ nodeId = $id; online = $false; role = $null; term = $null
                commitIndex = $null; lastApplied = $null; termField = $null; error = $_.Exception.Message }
        }
    }
}

function Wait-RaftState($Context, [string[]]$ExpectedNodes) {
    $watch = [Diagnostics.Stopwatch]::StartNew()
    $stable = 0
    $previous = ''
    while ($watch.Elapsed.TotalSeconds -lt $Context.Wait) {
        $snapshot = @(Get-FaultSnapshot $Context)
        Write-FaultLog ('STATUS ' + ($snapshot | ConvertTo-Json -Compress -Depth 5))
        $online = @($snapshot | Where-Object { $_.online })
        $exact = @($ExpectedNodes | Where-Object { $_ -cnotin $online.nodeId }).Count -eq 0 -and
            @($online | Where-Object { $_.nodeId -cnotin $ExpectedNodes }).Count -eq 0
        $key = ($ExpectedNodes | Sort-Object) -join ','
        if ($exact -and $ExpectedNodes.Count -ge 3) {
            $leaders = @($online | Where-Object { $_.role -eq 'LEADER' })
            $terms = @($online.term | Sort-Object -Unique)
            $exact = $leaders.Count -eq 1 -and $terms.Count -eq 1 -and $terms[0] -gt 0
            $key += "|$($leaders.nodeId)|$($terms -join ',')"
        }
        # With 2/5 an old Leader may still report LEADER; role alone cannot prove safety.
        if ($exact) {
            if ($key -eq $previous) { $stable++ } else { $stable = 1 }
            $previous = $key
            if ($stable -ge 3) {
                Write-FaultLog "OBSERVED stable=$stable reachable=$($ExpectedNodes.Count)/5; quorum fixed=3/5; safety=NOT_RUN_G5"
                return
            }
        } else { $stable = 0; $previous = '' }
        Start-Sleep -Milliseconds $Context.Interval
    }
    throw "Timed out waiting for Raft observations: expected nodes=$($ExpectedNodes -join ',')"
}

function Invoke-FaultRun($Context, [string]$Operation, [string[]]$Targets) {
    if ($Operation -eq 'Start') {
        Set-NodeFault $Context 'start' $Targets
        Resolve-ClientPorts $Context
        Wait-RaftState $Context $allNodes
        return
    }
    Resolve-ClientPorts $Context
    Wait-RaftState $Context $allNodes
    $remaining = @($allNodes | Where-Object { $_ -cnotin $Targets })
    if ($Operation -eq 'Stop') {
        Set-NodeFault $Context 'stop' $Targets
        Wait-RaftState $Context $remaining
        return
    }
    $failures = [Collections.Generic.List[Exception]]::new()
    try {
        Set-NodeFault $Context 'stop' $Targets
        Wait-RaftState $Context $remaining
    } catch { $failures.Add($_.Exception) }
    finally {
        try {
            Set-NodeFault $Context 'start' $Targets
            Wait-RaftState $Context $allNodes
        } catch { $failures.Add($_.Exception) }
    }
    if ($failures.Count) { throw [AggregateException]::new('Fault run or recovery failed', $failures.ToArray()) }
}

function Assert-Fault([bool]$Condition, [string]$Message) {
    if (!$Condition) { throw "SELFTEST ASSERTION: $Message" }
}

function Invoke-FaultSelfTest {
    $passed = 0
    foreach ($count in 1..3) {
        $ctx = New-FaultContext 'Fake'; $ctx.Interval = 20
        $targets = @(Get-Targets @() $count)
        Invoke-FaultRun $ctx 'Run' $targets
        Assert-Fault ($ctx.Running.Count -eq 5 -and $ctx.Commit -eq 43 -and $ctx.Term -gt 8) 'Run must restore topology without resetting metadata'
        Assert-Fault ($ctx.Calls.Count -eq 2 -and $ctx.Calls[0].StartsWith('stop ') -and $ctx.Calls[1].StartsWith('start ')) 'Only stop/start allowed'
        $passed++; Write-FaultLog "SELFTEST PASS Run $count nodes and restore"
        $ctx = New-FaultContext 'Fake'; $ctx.Interval = 20
        Invoke-FaultRun $ctx 'Stop' $targets
        Assert-Fault ($ctx.Running.Count -eq (5 - $count)) 'Stop must affect exactly target nodes'
        Invoke-FaultRun $ctx 'Start' $targets
        Assert-Fault ($ctx.Running.Count -eq 5) 'Start must restore selected nodes'
        $passed++; Write-FaultLog "SELFTEST PASS standalone Stop/Start $count nodes"
    }
    foreach ($bad in @(@('node-1','node-1'), @('node-6'), @('node-1','node-2','node-3','node-4'))) {
        $failed = $false
        try { $null = Get-Targets $bad 1 } catch { $failed = $true }
        Assert-Fault $failed 'Invalid targets must fail before faults'
        $passed++; Write-FaultLog "SELFTEST PASS reject targets=$($bad -join ',')"
    }
    $ctx = New-FaultContext 'Fake'; $ctx.Interval = 20; $ctx.FailStopOnce = $true
    $failed = $false
    try { Invoke-FaultRun $ctx 'Run' @('node-1','node-2') } catch { $failed = $true }
    Assert-Fault ($failed -and $ctx.Running.Count -eq 5 -and $ctx.Calls.Count -eq 2) 'Partial stop must recover and retain failure'
    $passed++; Write-FaultLog 'SELFTEST PASS partial stop failure still restores targets'
    $ctx = New-FaultContext 'Fake'; $ctx.Interval = 20; $ctx.Wait = 1; $ctx.TimeoutAfterStop = $true
    $failed = $false
    try { Invoke-FaultRun $ctx 'Run' @('node-1') } catch { $failed = $true }
    Assert-Fault ($failed -and $ctx.Running.Count -eq 5) 'Timeout must recover and retain failure'
    $passed++; Write-FaultLog 'SELFTEST PASS observation timeout recovers without false PASS'
    $ctx = New-FaultContext 'Fake'; $ctx.Interval = 20; $ctx.FailStart = $true
    $failed = $false
    try { Invoke-FaultRun $ctx 'Run' @('node-1') } catch { $failed = $true }
    Assert-Fault ($failed -and $ctx.Running.Count -eq 4) 'Recovery failure must be reported'
    $fresh = New-FaultContext 'Fake'
    Assert-Fault ($fresh.Running.Count -eq 5 -and $fresh.Calls.Count -eq 0) 'Tests must not share fault state'
    $passed++; Write-FaultLog 'SELFTEST PASS recovery failure and fresh context isolation'
    $ctx = New-FaultContext 'Fake'; $ctx.Interval = 20
    Invoke-FaultRun $ctx 'Run' @('node-5')
    Assert-Fault ($ctx.Running.Count -eq 5 -and $ctx.Leader -eq 'node-4') 'Leader selection must follow observations, not node-1'
    $passed++; Write-FaultLog 'SELFTEST PASS stopping fake Leader allows another node to lead'
    $legacy = [PSCustomObject]@{ role = 'FOLLOWER'; currentTerm = 8L; commitIndex = 43L; lastApplied = 43L; storageHealthy = $true }
    $failed = $false
    try { $null = Get-RaftValues $legacy $false } catch { $failed = $true }
    Assert-Fault $failed 'Legacy currentTerm must require explicit compatibility'
    $values = Get-RaftValues $legacy $true
    Assert-Fault ($values.term -eq 8 -and $values.termField -eq 'currentTerm') 'Legacy mode must preserve and label term'
    $legacy.currentTerm = '8'
    $failed = $false
    try { $null = Get-RaftValues $legacy $true } catch { $failed = $true }
    Assert-Fault $failed 'String term must not be coerced to a trusted number'
    $passed++; Write-FaultLog 'SELFTEST PASS explicit legacy handling and rejection of untrusted term'
    Write-FaultLog "SELFTEST RESULT PASS count=$passed mode=Fake; Docker/network calls=0; G5_live_safety=NOT_RUN"
}

$exitCode = 0
try {
    Write-FaultLog "TV6-D16 mode=$Mode action=$Action; G5_live_safety=NOT_RUN; volumes preserved"
    Write-FaultLog "SourceSHA256=$((Get-FileHash -LiteralPath $PSCommandPath -Algorithm SHA256).Hash)"
    if ($Action -eq 'SelfTest') {
        if ($Mode -ne 'Fake') { throw 'SelfTest requires Fake mode.' }
        Invoke-FaultSelfTest
    } else {
        $targets = @(Get-Targets $NodeIds $NodeCount)
        if ($Mode -eq 'Live') {
            if (!$Project) { throw 'Live mode requires an explicit existing Compose Project.' }
            if (!(Get-Command docker -ErrorAction SilentlyContinue)) { throw 'Docker is unavailable.' }
        }
        $context = New-FaultContext $Mode
        Invoke-FaultRun $context $Action $targets
        Write-FaultLog "RUNNER PASS mode=$Mode action=$Action nodes=$($targets -join ','); G5_live_safety=NOT_RUN"
    }
} catch {
    $exitCode = 1
    Write-FaultLog "RUNNER FAIL: $($_.Exception.ToString())"
} finally {
    $parent = Split-Path -Parent $LogPath
    New-Item -ItemType Directory -Force -Path $parent | Out-Null
    [IO.File]::WriteAllLines($LogPath, $transcript, [Text.UTF8Encoding]::new($false))
    Write-Host "Evidence=$LogPath; Exit=$exitCode"
}
exit $exitCode
