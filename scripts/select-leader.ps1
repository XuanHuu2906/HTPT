<#
.SYNOPSIS
    TV6-D21: Query NODE_STATUS across Raft voters and dynamically select the current Leader.
.DESCRIPTION
    Probes configured node endpoints using the SPEC-03 TCP length-prefixed JSON protocol
    (4-byte big-endian length prefix + UTF-8 JSON envelope, type=NODE_STATUS, TV3-D15
    NodeStatusHandler) and identifies the current Leader dynamically without hardcoding node-1.

    Exit codes:
      0 = Exactly one healthy LEADER observed
      1 = Validation / configuration / unexpected runtime error
      2 = No LEADER currently observed (0 leaders online)
      3 = Multiple LEADERs observed (fail-closed unless -SelectHighestTerm is set)
.EXAMPLE
    .\scripts\select-leader.ps1
.EXAMPLE
    .\scripts\select-leader.ps1 -Endpoints "node-1@127.0.0.1:9101,node-2@127.0.0.1:9102,node-3@127.0.0.1:9103,node-4@127.0.0.1:9104,node-5@127.0.0.1:9105" -AsJson
.EXAMPLE
    .\scripts\select-leader.ps1 -Quiet
#>
[CmdletBinding()]
param(
    [string]$HostAddress = '127.0.0.1',
    [string]$Endpoints = '',
    [string]$ComposeFile = 'docker/compose.dev.yml',
    [ValidatePattern('^$|^[a-z0-9][a-z0-9_-]*$')][string]$Project = '',
    [ValidateSet('backend-node', 'node')][string]$ServicePrefix = 'backend-node',
    [ValidateRange(100, 10000)][int]$TimeoutMs = 1500,
    [switch]$AllowLegacyCurrentTerm,
    [switch]$SelectHighestTerm,
    [switch]$AsJson,
    [switch]$Quiet,
    [string]$LogPath = ''
)

$ErrorActionPreference = 'Stop'
$allNodes = @('node-1', 'node-2', 'node-3', 'node-4', 'node-5')
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$transcript = [Collections.Generic.List[string]]::new()

function Write-StatusLog([string]$Message) {
    $line = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ss.fffZ') + ' [select-leader] ' + $Message
    $transcript.Add($line)
    if (-not $Quiet) {
        [Console]::Error.WriteLine($line)
    }
}

function Resolve-NodeEndpoints {
    $resolved = [Collections.Generic.List[PSCustomObject]]::new()

    if (-not [string]::IsNullOrWhiteSpace($Endpoints)) {
        $seen = [Collections.Generic.HashSet[string]]::new()
        foreach ($entry in ($Endpoints -split ',')) {
            $trimmed = $entry.Trim()
            if ([string]::IsNullOrWhiteSpace($trimmed)) { continue }
            $match = [regex]::Match($trimmed, '^(node-[1-5])@([^:\s]+):(\d+)$')
            if (-not $match.Success) {
                throw "Invalid endpoint specification '$trimmed'. Expected format: node-1@host:port"
            }
            $nid = $match.Groups[1].Value
            $epHost = $match.Groups[2].Value
            $epPort = [int]$match.Groups[3].Value
            if ($epPort -lt 1 -or $epPort -gt 65535) {
                throw "Port out of range for ${nid}: $epPort"
            }
            if (-not $seen.Add($nid)) {
                throw "Duplicate node endpoint specified for $nid"
            }
            $resolved.Add([PSCustomObject]@{ NodeId = $nid; Host = $epHost; Port = $epPort })
        }
        if ($resolved.Count -eq 0) {
            throw 'No valid node endpoints were parsed from -Endpoints.'
        }
        return $resolved
    }

    if (-not [string]::IsNullOrWhiteSpace($Project)) {
        $composePath = if ([IO.Path]::IsPathRooted($ComposeFile)) { $ComposeFile } else { Join-Path $repoRoot $ComposeFile }
        foreach ($nid in $allNodes) {
            $svc = $ServicePrefix + '-' + $nid.Substring(5)
            $savedPref = $ErrorActionPreference
            $ErrorActionPreference = 'Continue'
            try {
                $binding = & docker compose -p $Project -f $composePath port $svc 9100 2>&1
                $code = $LASTEXITCODE
            } finally {
                $ErrorActionPreference = $savedPref
            }
            $text = (($binding | ForEach-Object { $_.ToString() }) -join ' ').Trim()
            if ($code -ne 0) {
                Write-StatusLog "WARN cannot resolve port for service=$svc exit=$code output=$text"
                continue
            }
            $m = [regex]::Match($text, ':(\d+)\s*$')
            if ($m.Success) {
                $resolved.Add([PSCustomObject]@{ NodeId = $nid; Host = $HostAddress; Port = [int]$m.Groups[1].Value })
            }
        }
        if ($resolved.Count -gt 0) {
            return $resolved
        }
        Write-StatusLog 'WARN compose port discovery returned 0 endpoints; falling back to default dev client ports 9101..9105.'
    }

    for ($i = 1; $i -le 5; $i++) {
        $resolved.Add([PSCustomObject]@{ NodeId = "node-$i"; Host = $HostAddress; Port = (9100 + $i) })
    }
    return $resolved
}

function Read-ExactBytes($Stream, [int]$Length, $Watch) {
    $buffer = [byte[]]::new($Length)
    $offset = 0
    while ($offset -lt $Length) {
        $remaining = $TimeoutMs - [int]$Watch.ElapsedMilliseconds
        if ($remaining -le 0) {
            throw "NODE_STATUS read deadline exceeded (${TimeoutMs}ms)"
        }
        $Stream.ReadTimeout = $remaining
        $read = $Stream.Read($buffer, $offset, $Length - $offset)
        if ($read -le 0) {
            throw 'EOF before completing NODE_STATUS frame'
        }
        $offset += $read
    }
    return ,$buffer
}

function Query-NodeStatus([string]$ExpectedNodeId, [string]$TargetHost, [int]$TargetPort) {
    $tcp = [Net.Sockets.TcpClient]::new()
    $watch = [Diagnostics.Stopwatch]::StartNew()
    try {
        $ar = $tcp.BeginConnect($TargetHost, $TargetPort, $null, $null)
        try {
            if (-not $ar.AsyncWaitHandle.WaitOne($TimeoutMs)) {
                throw "Connect timeout (${TimeoutMs}ms)"
            }
            $tcp.EndConnect($ar)
        } finally {
            $ar.AsyncWaitHandle.Close()
        }

        $stream = $tcp.GetStream()
        $stream.WriteTimeout = $TimeoutMs
        $messageId = [Guid]::NewGuid().ToString().ToLowerInvariant()
        $requestObj = [ordered]@{
            protocolVersion = 1
            messageId       = $messageId
            correlationId   = $null
            type            = 'NODE_STATUS'
            senderId        = 'monitor-select-leader'
            sentAt          = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ss.fffZ')
            payload         = @{}
        }
        $json = $requestObj | ConvertTo-Json -Compress -Depth 6
        $bodyBytes = [Text.Encoding]::UTF8.GetBytes($json)
        $lenBytes = [BitConverter]::GetBytes([int]$bodyBytes.Length)
        if ([BitConverter]::IsLittleEndian) { [Array]::Reverse($lenBytes) }

        $stream.Write($lenBytes, 0, 4)
        $stream.Write($bodyBytes, 0, $bodyBytes.Length)
        $stream.Flush()

        $prefix = Read-ExactBytes $stream 4 $watch
        if ([BitConverter]::IsLittleEndian) { [Array]::Reverse($prefix) }
        $respLen = [BitConverter]::ToInt32($prefix, 0)
        if ($respLen -lt 1 -or $respLen -gt 1048576) {
            throw "Invalid NODE_STATUS response frame size: $respLen"
        }

        $payloadBytes = Read-ExactBytes $stream $respLen $watch
        $rawJson = [Text.UTF8Encoding]::new($false, $true).GetString($payloadBytes)
        $envelope = $rawJson | ConvertFrom-Json

        if ($envelope.protocolVersion -ne 1) {
            throw "Unexpected protocolVersion=$($envelope.protocolVersion)"
        }
        if ($envelope.correlationId -cne $messageId) {
            throw "correlationId mismatch: expected=$messageId actual=$($envelope.correlationId)"
        }
        if ($envelope.type -cne 'NODE_STATUS_RESPONSE') {
            throw "Unexpected response type=$($envelope.type)"
        }
        if ($envelope.senderId -cne $ExpectedNodeId) {
            throw "senderId mismatch: expected=$ExpectedNodeId actual=$($envelope.senderId)"
        }

        $p = $envelope.payload
        if ($null -eq $p -or $p.nodeId -cne $ExpectedNodeId) {
            throw "payload.nodeId mismatch: expected=$ExpectedNodeId actual=$($p.nodeId)"
        }

        $termVal = $null
        $termField = 'term'
        if ($p.PSObject.Properties['term']) {
            $termVal = $p.term
        } elseif ($AllowLegacyCurrentTerm -and $p.PSObject.Properties['currentTerm']) {
            $termVal = $p.currentTerm
            $termField = 'currentTerm'
        } else {
            throw 'SPEC-03 term field missing in NODE_STATUS_RESPONSE'
        }

        if ($null -ne $termVal -and ($termVal -isnot [long] -and $termVal -isnot [int])) {
            throw "Untrusted non-numeric $termField value"
        }

        return [PSCustomObject]@{
            nodeId         = $ExpectedNodeId
            endpoint       = "${TargetHost}:${TargetPort}"
            online         = $true
            role           = $p.role
            term           = $termVal
            termField      = $termField
            leaderId       = if ($p.PSObject.Properties['leaderId']) { $p.leaderId } else { $null }
            lastLogIndex   = if ($p.PSObject.Properties['lastLogIndex']) { $p.lastLogIndex } else { $null }
            commitIndex    = if ($p.PSObject.Properties['commitIndex']) { $p.commitIndex } else { $null }
            lastApplied    = if ($p.PSObject.Properties['lastApplied']) { $p.lastApplied } else { $null }
            storageHealthy = if ($p.PSObject.Properties['storageHealthy'] -and ($p.storageHealthy -is [bool])) { $p.storageHealthy } else { $false }
            observedAt     = if ($p.PSObject.Properties['observedAt']) { $p.observedAt } else { $null }
            error          = $null
        }
    } catch {
        return [PSCustomObject]@{
            nodeId         = $ExpectedNodeId
            endpoint       = "${TargetHost}:${TargetPort}"
            online         = $false
            role           = $null
            term           = $null
            termField      = $null
            leaderId       = $null
            lastLogIndex   = $null
            commitIndex    = $null
            lastApplied    = $null
            storageHealthy = $false
            observedAt     = $null
            error          = $_.Exception.Message
        }
    } finally {
        $tcp.Dispose()
    }
}

$exitCode = 0
$resultOutput = $null

try {
    $targets = @(Resolve-NodeEndpoints)
    Write-StatusLog "Probing $($targets.Count) node endpoint(s) via NODE_STATUS (timeout=${TimeoutMs}ms)"

    $observations = [Collections.Generic.List[PSCustomObject]]::new()
    foreach ($t in $targets) {
        $obs = Query-NodeStatus $t.NodeId $t.Host $t.Port
        $observations.Add($obs)
        if ($obs.online) {
            Write-StatusLog "OBSERVED node=$($obs.nodeId) endpoint=$($obs.endpoint) role=$($obs.role) term=$($obs.term) leaderId=$($obs.leaderId) commitIndex=$($obs.commitIndex) lastApplied=$($obs.lastApplied) storageHealthy=$($obs.storageHealthy)"
        } else {
            Write-StatusLog "OFFLINE  node=$($obs.nodeId) endpoint=$($obs.endpoint) reason=$($obs.error)"
        }
    }

    $onlineNodes = @($observations | Where-Object { $_.online })
    $healthyLeaders = @($onlineNodes | Where-Object { $_.role -ceq 'LEADER' -and $_.storageHealthy -eq $true -and $null -ne $_.term })

    $selectedLeader = $null
    $statusLabel = ''

    if ($healthyLeaders.Count -eq 1) {
        $selectedLeader = $healthyLeaders[0]
        $statusLabel = 'SINGLE_LEADER'
        $exitCode = 0
        Write-StatusLog "SELECTED leader=$($selectedLeader.nodeId) term=$($selectedLeader.term) online=$($onlineNodes.Count)/$($targets.Count)"
    } elseif ($healthyLeaders.Count -eq 0) {
        $statusLabel = 'NO_LEADER'
        $exitCode = 2
        Write-StatusLog "NO_LEADER_FOUND online=$($onlineNodes.Count)/$($targets.Count); refusing to hardcode node-1"
    } else {
        $distinctTerms = @($healthyLeaders | ForEach-Object { $_.term } | Sort-Object -Unique)
        $leaderSummary = ($healthyLeaders | ForEach-Object { "$($_.nodeId)@term$($_.term)" }) -join ', '
        if ($SelectHighestTerm -and $distinctTerms.Count -gt 1) {
            $highestTerm = ($distinctTerms | Measure-Object -Maximum).Maximum
            $highestLeaders = @($healthyLeaders | Where-Object { $_.term -eq $highestTerm })
            if ($highestLeaders.Count -eq 1) {
                $selectedLeader = $highestLeaders[0]
                $statusLabel = 'HIGHEST_TERM_LEADER'
                $exitCode = 0
                Write-StatusLog "WARN multiple leaders observed ($leaderSummary); selected highest-term leader=$($selectedLeader.nodeId) term=$($selectedLeader.term)"
            } else {
                $statusLabel = 'SPLIT_BRAIN_SAME_TERM'
                $exitCode = 3
                Write-StatusLog "ERROR split-brain detected at highest term ${highestTerm}: $leaderSummary"
            }
        } else {
            $statusLabel = 'MULTIPLE_LEADERS'
            $exitCode = 3
            Write-StatusLog "ERROR multiple leaders observed ($leaderSummary); fail-closed exit=3"
        }
    }

    $summary = [ordered]@{
        observedAtUtc = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ss.fffZ')
        status        = $statusLabel
        leaderNodeId  = if ($null -ne $selectedLeader) { $selectedLeader.nodeId } else { $null }
        leaderTerm    = if ($null -ne $selectedLeader) { $selectedLeader.term } else { $null }
        leaderCount   = $healthyLeaders.Count
        onlineCount   = $onlineNodes.Count
        totalProbed   = $targets.Count
        exitCode      = $exitCode
        nodes         = @($observations)
    }

    if ($AsJson) {
        $resultOutput = $summary | ConvertTo-Json -Depth 6
    } elseif ($null -ne $selectedLeader) {
        $resultOutput = $selectedLeader.nodeId
    }
} catch {
    $exitCode = 1
    Write-StatusLog "ERROR $($_.Exception.Message)"
    if ($AsJson) {
        $resultOutput = ([ordered]@{
            observedAtUtc = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ss.fffZ')
            status        = 'ERROR'
            leaderNodeId  = $null
            error         = $_.Exception.Message
            exitCode      = $exitCode
        } | ConvertTo-Json -Depth 4)
    }
} finally {
    if (-not [string]::IsNullOrWhiteSpace($LogPath)) {
        $fullLog = [IO.Path]::GetFullPath($LogPath)
        $logDir = Split-Path -Parent $fullLog
        if ($logDir -and -not (Test-Path -LiteralPath $logDir)) {
            New-Item -ItemType Directory -Force -Path $logDir | Out-Null
        }
        [IO.File]::WriteAllLines($fullLog, $transcript, [Text.UTF8Encoding]::new($false))
    }
}

if ($null -ne $resultOutput) {
    Write-Output $resultOutput
}
exit $exitCode
