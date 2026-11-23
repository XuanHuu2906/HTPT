<#
.SYNOPSIS
    TV6-D21: Start / restart a single Raft node container via Docker without deleting volumes.
.DESCRIPTION
    Starts a stopped Docker container (or cleanly stops and starts a running container when
    -RestartIfRunning is set), specified by -NodeId (node-1..node-5) or -ContainerName.

    Safety guarantees (SPEC-08 §5, §7):
      - Uses ONLY 'docker start' / 'docker compose start' (plus 'stop' if -RestartIfRunning).
      - NEVER removes containers or volumes (no rm, down, down -v, volume rm).
      - Logs ISO-8601 UTC timestamps, exact Docker command, exit code, container state,
        verifies attached volumes remain intact, and waits until the container is running.
      - Optional -WaitStatus flag probes TV3-D15 NODE_STATUS over TCP for Gate G6 catch-up checks.

    Exit codes:
      0 = Target container running and volumes verified intact (and NODE_STATUS reachable if -WaitStatus)
      1 = Validation / Docker / startup / status wait failure
.EXAMPLE
    .\scripts\restart-node.ps1 -NodeId node-2
.EXAMPLE
    .\scripts\restart-node.ps1 -NodeId node-2 -RestartIfRunning -WaitStatus
.EXAMPLE
    .\scripts\restart-node.ps1 -ContainerName railway-tv6-d21-test-node -LogPath artifacts/evidence/D21/restart.log
#>
[CmdletBinding()]
param(
    [ValidatePattern('^$|^node-[1-5]$')][string]$NodeId = '',
    [ValidatePattern('^$|^[a-zA-Z0-9][a-zA-Z0-9_.-]*$')][string]$ContainerName = '',
    [switch]$UseCompose,
    [string]$ComposeFile = 'docker/compose.dev.yml',
    [ValidatePattern('^$|^[a-z0-9][a-z0-9_-]*$')][string]$Project = '',
    [ValidateSet('backend-node', 'node')][string]$ServicePrefix = 'backend-node',
    [switch]$RestartIfRunning,
    [ValidateRange(1, 120)][int]$StopTimeoutSeconds = 10,
    [ValidateRange(1, 120)][int]$WaitRunningSeconds = 15,
    [switch]$WaitStatus,
    [string]$HostAddress = '127.0.0.1',
    [ValidateRange(0, 65535)][int]$StatusPort = 0,
    [ValidateRange(1, 120)][int]$WaitStatusSeconds = 20,
    [ValidateRange(100, 10000)][int]$StatusTimeoutMs = 1500,
    [switch]$AllowLegacyCurrentTerm,
    [string]$LogPath = ''
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$transcript = [Collections.Generic.List[string]]::new()

function Write-ActionLog([string]$Message) {
    $line = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ss.fffZ') + ' [restart-node] ' + $Message
    $transcript.Add($line)
    Write-Host $line
}

function Invoke-DockerCommand([string[]]$DockerArgs, [bool]$LogOutput = $true) {
    $cmdText = 'docker ' + ($DockerArgs -join ' ')
    Write-ActionLog "EXEC: $cmdText"
    $savedPref = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $raw = & docker @DockerArgs 2>&1
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $savedPref
    }
    $text = (($raw | ForEach-Object { $_.ToString() }) -join "`n").Trim()
    if ($LogOutput -or $code -ne 0) {
        Write-ActionLog "EXIT: code=$code cmd=`"$cmdText`" output=`"$($text -replace "`r?`n", ' | ')`""
    } else {
        Write-ActionLog "EXIT: code=$code cmd=`"$cmdText`" outputBytes=$($text.Length)"
    }
    return [PSCustomObject]@{ ExitCode = $code; Output = $text; Command = $cmdText }
}

function Get-ContainerSnapshot([string]$Target) {
    $res = Invoke-DockerCommand @('inspect', $Target) $false
    if ($res.ExitCode -ne 0) {
        throw "Container '$Target' not found or docker inspect failed (exit=$($res.ExitCode)): $($res.Output)"
    }
    $arr = $res.Output | ConvertFrom-Json
    $info = if ($arr -is [System.Array]) { $arr[0] } else { $arr }
    $mounts = @()
    if ($info.Mounts) {
        foreach ($m in $info.Mounts) {
            $mounts += [PSCustomObject]@{
                Type        = $m.Type
                Name        = if ($m.PSObject.Properties['Name']) { $m.Name } else { '' }
                Source      = $m.Source
                Destination = $m.Destination
            }
        }
    }
    $mountKeys = @($mounts | ForEach-Object { "$($_.Type):$($_.Name):$($_.Source)->$($_.Destination)" } | Sort-Object)
    return [PSCustomObject]@{
        Id        = $info.Id
        Name      = $info.Name.TrimStart('/')
        Status    = $info.State.Status
        Running   = [bool]$info.State.Running
        StartedAt = $info.State.StartedAt
        ExitCode  = [int]$info.State.ExitCode
        Mounts    = $mounts
        MountKeys = $mountKeys
    }
}

function Resolve-TargetSpec {
    $hasNode = -not [string]::IsNullOrWhiteSpace($NodeId)
    $hasContainer = -not [string]::IsNullOrWhiteSpace($ContainerName)
    if ($hasNode -eq $hasContainer) {
        throw 'Specify exactly one target: -NodeId <node-1..node-5> or -ContainerName <name>.'
    }

    if ($hasContainer) {
        if ($UseCompose) {
            throw '-UseCompose cannot be combined with raw -ContainerName; use -NodeId.'
        }
        return [PSCustomObject]@{
            NodeId        = $null
            ServiceName   = $null
            ContainerName = $ContainerName
        }
    }

    $nodeNum = $NodeId.Substring(5)
    $serviceName = "${ServicePrefix}-${nodeNum}"
    if ($UseCompose) {
        $composePath = if ([IO.Path]::IsPathRooted($ComposeFile)) { $ComposeFile } else { Join-Path $repoRoot $ComposeFile }
        $psArgs = @('compose')
        if (-not [string]::IsNullOrWhiteSpace($Project)) { $psArgs += @('-p', $Project) }
        $psArgs += @('-f', $composePath, 'ps', '-a', '-q', $serviceName)
        $psRes = Invoke-DockerCommand $psArgs
        if ($psRes.ExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($psRes.Output)) {
            throw "Cannot resolve container ID for compose service '$serviceName' (exit=$($psRes.ExitCode))."
        }
        $cid = ($psRes.Output -split "`r?`n")[0].Trim()
        return [PSCustomObject]@{
            NodeId        = $NodeId
            ServiceName   = $serviceName
            ContainerName = $cid
        }
    }

    return [PSCustomObject]@{
        NodeId        = $NodeId
        ServiceName   = $serviceName
        ContainerName = "railway-${serviceName}"
    }
}

function Wait-ForNodeStatus([string]$ExpectedNodeId, [string]$TargetHost, [int]$TargetPort) {
    $selectorScript = Join-Path $PSScriptRoot 'select-leader.ps1'
    if (-not (Test-Path -LiteralPath $selectorScript -PathType Leaf)) {
        throw "Missing status probe helper: $selectorScript"
    }
    $endpoint = "${ExpectedNodeId}@${TargetHost}:${TargetPort}"
    $watch = [Diagnostics.Stopwatch]::StartNew()
    while ($watch.Elapsed.TotalSeconds -lt $WaitStatusSeconds) {
        $probeParams = @{
            Endpoints = $endpoint
            TimeoutMs = $StatusTimeoutMs
            AsJson    = $true
            Quiet     = $true
        }
        if ($AllowLegacyCurrentTerm) { $probeParams['AllowLegacyCurrentTerm'] = $true }
        $savedPref = $ErrorActionPreference
        $ErrorActionPreference = 'Continue'
        try {
            $raw = & $selectorScript @probeParams
        } finally {
            $ErrorActionPreference = $savedPref
        }
        $jsonText = (($raw | ForEach-Object { $_.ToString() }) -join "`n").Trim()
        if (-not [string]::IsNullOrWhiteSpace($jsonText)) {
            try {
                $parsed = $jsonText | ConvertFrom-Json
                $nodeRow = @($parsed.nodes | Where-Object { $_.nodeId -ceq $ExpectedNodeId })[0]
                if ($null -ne $nodeRow -and $nodeRow.online) {
                    Write-ActionLog "STATUS_READY node=$($nodeRow.nodeId) role=$($nodeRow.role) term=$($nodeRow.term) commitIndex=$($nodeRow.commitIndex) lastApplied=$($nodeRow.lastApplied) storageHealthy=$($nodeRow.storageHealthy)"
                    return
                }
            } catch {}
        }
        Start-Sleep -Milliseconds 300
    }
    throw "Timed out after ${WaitStatusSeconds}s waiting for NODE_STATUS on $endpoint"
}

$exitCode = 0
try {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw 'Docker CLI is not installed or not in PATH.'
    }

    Write-ActionLog "START restart-node (volumes_preserved=true; restartIfRunning=$RestartIfRunning)"
    $target = Resolve-TargetSpec
    Write-ActionLog "TARGET nodeId=$($target.NodeId) service=$($target.ServiceName) container=$($target.ContainerName) useCompose=$UseCompose"

    $before = Get-ContainerSnapshot $target.ContainerName
    Write-ActionLog "BEFORE container=$($before.Name) status=$($before.Status) running=$($before.Running) startedAt=$($before.StartedAt) mounts=$($before.Mounts.Count) [$($before.MountKeys -join '; ')]"

    if ($before.Running -and $RestartIfRunning) {
        Write-ActionLog "Container is currently running and -RestartIfRunning is set; stopping before start."
        if ($UseCompose) {
            $composePath = if ([IO.Path]::IsPathRooted($ComposeFile)) { $ComposeFile } else { Join-Path $repoRoot $ComposeFile }
            $stopArgs = @('compose')
            if (-not [string]::IsNullOrWhiteSpace($Project)) { $stopArgs += @('-p', $Project) }
            $stopArgs += @('-f', $composePath, 'stop', '--timeout', "$StopTimeoutSeconds", $target.ServiceName)
            $stopRes = Invoke-DockerCommand $stopArgs
        } else {
            $stopRes = Invoke-DockerCommand @('stop', '--time', "$StopTimeoutSeconds", $before.Name)
        }
        if ($stopRes.ExitCode -ne 0) {
            throw "Docker stop during restart failed with exit=$($stopRes.ExitCode): $($stopRes.Output)"
        }
    }

    if ($UseCompose) {
        $composePath = if ([IO.Path]::IsPathRooted($ComposeFile)) { $ComposeFile } else { Join-Path $repoRoot $ComposeFile }
        $startArgs = @('compose')
        if (-not [string]::IsNullOrWhiteSpace($Project)) { $startArgs += @('-p', $Project) }
        $startArgs += @('-f', $composePath, 'start', $target.ServiceName)
        $startRes = Invoke-DockerCommand $startArgs
    } else {
        $startRes = Invoke-DockerCommand @('start', $before.Name)
    }

    if ($startRes.ExitCode -ne 0) {
        throw "Docker start failed with exit=$($startRes.ExitCode): $($startRes.Output)"
    }

    $watch = [Diagnostics.Stopwatch]::StartNew()
    $after = $null
    while ($watch.Elapsed.TotalSeconds -lt $WaitRunningSeconds) {
        $after = Get-ContainerSnapshot $before.Name
        if ($after.Running) { break }
        Start-Sleep -Milliseconds 250
    }

    if ($null -eq $after -or -not $after.Running) {
        throw "Container '$($before.Name)' failed to reach running state within ${WaitRunningSeconds}s (status=$($after.Status))."
    }

    Write-ActionLog "AFTER  container=$($after.Name) status=$($after.Status) running=$($after.Running) startedAt=$($after.StartedAt) mounts=$($after.Mounts.Count) [$($after.MountKeys -join '; ')]"

    if (($before.MountKeys -join '|') -cne ($after.MountKeys -join '|')) {
        throw 'SAFETY VIOLATION: Mounts changed during restart-node!'
    }

    foreach ($m in $after.Mounts) {
        if ($m.Type -eq 'volume' -and -not [string]::IsNullOrWhiteSpace($m.Name)) {
            $volCheck = Invoke-DockerCommand @('volume', 'inspect', $m.Name, '--format', '{{.Name}}')
            if ($volCheck.ExitCode -ne 0) {
                throw "Volume '$($m.Name)' missing after start!"
            }
            Write-ActionLog "VOLUME_VERIFIED intact name=$($m.Name) destination=$($m.Destination)"
        }
    }

    if ($WaitStatus) {
        if ([string]::IsNullOrWhiteSpace($target.NodeId)) {
            throw '-WaitStatus requires -NodeId (node-1..node-5) so NODE_STATUS senderId can be validated.'
        }
        $portToProbe = if ($StatusPort -gt 0) { $StatusPort } else { 9100 + [int]$target.NodeId.Substring(5) }
        Wait-ForNodeStatus $target.NodeId $HostAddress $portToProbe
    }

    Write-ActionLog "RESULT PASS action=restart-node container=$($after.Name) status=$($after.Status) volumesIntact=true exit=0"
    $exitCode = 0
} catch {
    $exitCode = 1
    Write-ActionLog "RESULT FAIL action=restart-node exit=$exitCode error=`"$($_.Exception.Message)`""
} finally {
    if (-not [string]::IsNullOrWhiteSpace($LogPath)) {
        $fullLog = [IO.Path]::GetFullPath($LogPath)
        $logDir = Split-Path -Parent $fullLog
        if ($logDir -and -not (Test-Path -LiteralPath $logDir)) {
            New-Item -ItemType Directory -Force -Path $logDir | Out-Null
        }
        [IO.File]::WriteAllLines($fullLog, $transcript, [Text.UTF8Encoding]::new($false))
        Write-Host "LogPath=$fullLog; Exit=$exitCode"
    }
}
exit $exitCode
