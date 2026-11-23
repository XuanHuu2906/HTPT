<#
.SYNOPSIS
    TV6-D21: Stop a single Raft node container via Docker without deleting volumes.
.DESCRIPTION
    Stops one Docker container (specified by -NodeId, -ContainerName, or dynamically selected
    via -TargetLeader using scripts/select-leader.ps1 against TV3-D15 NODE_STATUS).

    Safety guarantees (SPEC-08 §5, §7):
      - Uses ONLY 'docker stop' or 'docker compose stop'.
      - NEVER removes containers or volumes (no rm, down, down -v, volume rm).
      - Logs ISO-8601 UTC timestamps, exact Docker command, exit code, container state,
        and verifies attached volume mounts remain intact before and after stop.

    Exit codes:
      0 = Target container stopped (or already stopped) and volumes verified intact
      1 = Validation / Docker / runtime failure
      2 = -TargetLeader could not identify a unique current Leader
.EXAMPLE
    .\scripts\stop-node.ps1 -NodeId node-2
.EXAMPLE
    .\scripts\stop-node.ps1 -TargetLeader
.EXAMPLE
    .\scripts\stop-node.ps1 -ContainerName railway-tv6-d21-test-node -LogPath artifacts/evidence/D21/stop.log
#>
[CmdletBinding()]
param(
    [ValidatePattern('^$|^node-[1-5]$')][string]$NodeId = '',
    [switch]$TargetLeader,
    [ValidatePattern('^$|^[a-zA-Z0-9][a-zA-Z0-9_.-]*$')][string]$ContainerName = '',
    [switch]$UseCompose,
    [string]$ComposeFile = 'docker/compose.dev.yml',
    [ValidatePattern('^$|^[a-z0-9][a-z0-9_-]*$')][string]$Project = '',
    [ValidateSet('backend-node', 'node')][string]$ServicePrefix = 'backend-node',
    [ValidateRange(1, 120)][int]$StopTimeoutSeconds = 10,
    [string]$HostAddress = '127.0.0.1',
    [string]$Endpoints = '',
    [ValidateRange(100, 10000)][int]$StatusTimeoutMs = 1500,
    [switch]$AllowLegacyCurrentTerm,
    [string]$LogPath = ''
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$transcript = [Collections.Generic.List[string]]::new()

function Write-ActionLog([string]$Message) {
    $line = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ss.fffZ') + ' [stop-node] ' + $Message
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
        Id         = $info.Id
        Name       = $info.Name.TrimStart('/')
        Status     = $info.State.Status
        Running    = [bool]$info.State.Running
        ExitCode   = [int]$info.State.ExitCode
        FinishedAt = $info.State.FinishedAt
        Mounts     = $mounts
        MountKeys  = $mountKeys
    }
}

function Resolve-TargetSpec {
    $selectedModes = 0
    if (-not [string]::IsNullOrWhiteSpace($NodeId)) { $selectedModes++ }
    if ($TargetLeader) { $selectedModes++ }
    if (-not [string]::IsNullOrWhiteSpace($ContainerName)) { $selectedModes++ }

    if ($selectedModes -ne 1) {
        throw 'Specify exactly one target: -NodeId <node-1..node-5>, -TargetLeader, or -ContainerName <name>.'
    }

    if ($TargetLeader) {
        $selectorScript = Join-Path $PSScriptRoot 'select-leader.ps1'
        if (-not (Test-Path -LiteralPath $selectorScript -PathType Leaf)) {
            throw "Leader selector script missing: $selectorScript"
        }
        $selectorParams = @{
            HostAddress = $HostAddress
            TimeoutMs   = $StatusTimeoutMs
            Quiet       = $true
        }
        if (-not [string]::IsNullOrWhiteSpace($Endpoints)) { $selectorParams['Endpoints'] = $Endpoints }
        if (-not [string]::IsNullOrWhiteSpace($Project)) {
            $selectorParams['Project'] = $Project
            $selectorParams['ComposeFile'] = $ComposeFile
            $selectorParams['ServicePrefix'] = $ServicePrefix
        }
        if ($AllowLegacyCurrentTerm) { $selectorParams['AllowLegacyCurrentTerm'] = $true }

        Write-ActionLog "Invoking select-leader.ps1 to dynamically discover current Leader via NODE_STATUS"
        $savedPref = $ErrorActionPreference
        $ErrorActionPreference = 'Continue'
        try {
            $leaderOut = & $selectorScript @selectorParams
            $selExit = $LASTEXITCODE
        } finally {
            $ErrorActionPreference = $savedPref
        }
        $discovered = (($leaderOut | ForEach-Object { $_.ToString() }) -join '').Trim()
        Write-ActionLog "select-leader.ps1 exit=$selExit leader=`"$discovered`""
        if ($selExit -ne 0 -or [string]::IsNullOrWhiteSpace($discovered)) {
            throw [InvalidOperationException]::new("NO_UNIQUE_LEADER: select-leader.ps1 returned exit=$selExit")
        }
        $NodeId = $discovered
    }

    if (-not [string]::IsNullOrWhiteSpace($ContainerName)) {
        if ($UseCompose) {
            throw '-UseCompose cannot be combined with raw -ContainerName; use -NodeId or -TargetLeader.'
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

    $defaultContainer = "railway-${serviceName}"
    return [PSCustomObject]@{
        NodeId        = $NodeId
        ServiceName   = $serviceName
        ContainerName = $defaultContainer
    }
}

$exitCode = 0
try {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw 'Docker CLI is not installed or not in PATH.'
    }

    Write-ActionLog "START stop-node (volumes_preserved=true; timeout=${StopTimeoutSeconds}s)"
    $target = Resolve-TargetSpec
    Write-ActionLog "TARGET nodeId=$($target.NodeId) service=$($target.ServiceName) container=$($target.ContainerName) useCompose=$UseCompose"

    $before = Get-ContainerSnapshot $target.ContainerName
    Write-ActionLog "BEFORE container=$($before.Name) status=$($before.Status) running=$($before.Running) mounts=$($before.Mounts.Count) [$($before.MountKeys -join '; ')]"

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
        throw "Docker stop failed with exit=$($stopRes.ExitCode): $($stopRes.Output)"
    }

    $after = Get-ContainerSnapshot $before.Name
    Write-ActionLog "AFTER  container=$($after.Name) status=$($after.Status) running=$($after.Running) containerExitCode=$($after.ExitCode) finishedAt=$($after.FinishedAt) mounts=$($after.Mounts.Count) [$($after.MountKeys -join '; ')]"

    if ($after.Running) {
        throw "Container '$($after.Name)' is still running after docker stop."
    }
    if (($before.MountKeys -join '|') -cne ($after.MountKeys -join '|')) {
        throw "SAFETY VIOLATION: Mounts changed during stop-node!"
    }

    foreach ($m in $after.Mounts) {
        if ($m.Type -eq 'volume' -and -not [string]::IsNullOrWhiteSpace($m.Name)) {
            $volCheck = Invoke-DockerCommand @('volume', 'inspect', $m.Name, '--format', '{{.Name}}')
            if ($volCheck.ExitCode -ne 0) {
                throw "Volume '$($m.Name)' missing after stop!"
            }
            Write-ActionLog "VOLUME_VERIFIED intact name=$($m.Name) destination=$($m.Destination)"
        }
    }

    Write-ActionLog "RESULT PASS action=stop-node container=$($after.Name) status=$($after.Status) volumesIntact=true exit=0"
    $exitCode = 0
} catch [InvalidOperationException] {
    if ($_.Exception.Message -like 'NO_UNIQUE_LEADER:*') {
        $exitCode = 2
    } else {
        $exitCode = 1
    }
    Write-ActionLog "RESULT FAIL action=stop-node exit=$exitCode error=`"$($_.Exception.Message)`""
} catch {
    $exitCode = 1
    Write-ActionLog "RESULT FAIL action=stop-node exit=$exitCode error=`"$($_.Exception.Message)`""
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
