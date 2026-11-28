<#
.SYNOPSIS
    SQL checker hau recovery cho 5 PostgreSQL nodes (Railway Distributed System - Nhom 6).
    Owner: TV2 (Pham Minh Tuan).
.DESCRIPTION
    Chay doc lap database/compare-db.sql (READ ONLY) tren danh sach node PostgreSQL,
    kiem tra bat bien cuc bo, dinh nghia index ux_one_active_ticket_per_seat, va doi chieu
    tinh nhat quan theo quy tac D20 (CUNG lastApplied => CUNG state_hash; lastApplied thap hon => LAG).

    Exit codes:
      0 = CONSISTENT
      2 = DIVERGENCE
      3 = LAG_UNRESOLVED
      4 = UNREACHABLE
      5 = CHECK_FAILED
      6 = USAGE_ERROR / MISSING_PREREQUISITE
#>
[CmdletBinding()]
param(
    [string]$NodesFile = "",
    [string[]]$Nodes = @(),
    [string]$Schema = "public",
    [string]$PasswordEnvVar = "DB_PASSWORD",
    [int]$PollRetries = 3,
    [int]$PollIntervalSeconds = 2,
    [int]$ConnectTimeoutSeconds = 5,
    [string]$CompareSqlPath = "",
    [string]$PsqlPath = ""
)

$ErrorActionPreference = "Stop"

$EXIT_CONSISTENT      = 0
$EXIT_DIVERGENCE      = 2
$EXIT_LAG_UNRESOLVED  = 3
$EXIT_UNREACHABLE     = 4
$EXIT_CHECK_FAILED    = 5
$EXIT_USAGE_ERROR     = 6

function Resolve-PsqlExecutable {
    param([string]$ExplicitPath)
    if (-not [string]::IsNullOrWhiteSpace($ExplicitPath)) {
        if (Test-Path $ExplicitPath) { return $ExplicitPath }
        return $null
    }
    $cmd = Get-Command "psql" -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }

    $candidates = Get-ChildItem -Path "C:\Program Files\PostgreSQL\*\bin\psql.exe" -ErrorAction SilentlyContinue |
                  Sort-Object FullName -Descending
    if ($candidates -and $candidates.Count -gt 0) {
        return $candidates[0].FullName
    }
    return $null
}

function Parse-NodeList {
    param([string]$FilePath, [string[]]$InlineNodes)
    $list = @()

    if (-not [string]::IsNullOrWhiteSpace($FilePath)) {
        if (-not (Test-Path $FilePath)) {
            throw "Khong tim thay file cau hinh node: $FilePath"
        }
        $ext = [System.IO.Path]::GetExtension($FilePath).ToLowerInvariant()
        if ($ext -eq ".json") {
            $raw = Get-Content -Path $FilePath -Raw -Encoding UTF8 | ConvertFrom-Json
            foreach ($item in $raw) {
                $list += [PSCustomObject]@{
                    Id   = [string]$item.id
                    Host = [string]$item.host
                    Port = [int]$item.port
                    Db   = [string]$item.db
                    User = [string]$item.user
                }
            }
        }
        elseif ($ext -eq ".csv") {
            $rows = Import-Csv -Path $FilePath -Encoding UTF8
            foreach ($r in $rows) {
                $list += [PSCustomObject]@{
                    Id   = [string]$r.id
                    Host = [string]$r.host
                    Port = [int]$r.port
                    Db   = [string]$r.db
                    User = [string]$r.user
                }
            }
        }
        else {
            $lines = Get-Content -Path $FilePath -Encoding UTF8
            foreach ($line in $lines) {
                $trimmed = $line.Trim()
                if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith("#")) { continue }
                $parts = $trimmed -split '\|'
                if ($parts.Count -lt 5) {
                    throw "Dong cau hinh khong hop le (can id|host|port|db|user): $trimmed"
                }
                $list += [PSCustomObject]@{
                    Id   = $parts[0].Trim()
                    Host = $parts[1].Trim()
                    Port = [int]$parts[2].Trim()
                    Db   = $parts[3].Trim()
                    User = $parts[4].Trim()
                }
            }
        }
        return ,$list
    }

    if ($InlineNodes -and $InlineNodes.Count -gt 0) {
        $idx = 1
        foreach ($spec in $InlineNodes) {
            # Format: id=host:port/db:user  OR  host:port/db:user
            $nodeId = "node-$idx"
            $rest = $spec.Trim()
            if ($rest -match '^([^=]+)=(.+)$') {
                $nodeId = $Matches[1].Trim()
                $rest = $Matches[2].Trim()
            }
            if ($rest -match '^([^:]+):(\d+)/([^:]+):(.+)$') {
                $list += [PSCustomObject]@{
                    Id   = $nodeId
                    Host = $Matches[1].Trim()
                    Port = [int]$Matches[2].Trim()
                    Db   = $Matches[3].Trim()
                    User = $Matches[4].Trim()
                }
            }
            else {
                throw "Tham so -Nodes khong hop le (can id=host:port/db:user): $spec"
            }
            $idx++
        }
        return ,$list
    }

    # Default baseline D25 (5 local PostgreSQL containers)
    for ($i = 1; $i -le 5; $i++) {
        $list += [PSCustomObject]@{
            Id   = "node-$i"
            Host = "127.0.0.1"
            Port = 5432 + $i
            Db   = "railway_node$i"
            User = "railway"
        }
    }
    return ,$list
}

function Invoke-PsqlCompare {
    param(
        [string]$PsqlExe,
        [object]$Node,
        [string]$SqlFile,
        [string]$TargetSchema,
        [int]$TimeoutSec
    )

    $env:PGCONNECT_TIMEOUT = "$TimeoutSec"
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $PsqlExe
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true

    $args = @(
        "-X",
        "-h", $Node.Host,
        "-p", "$($Node.Port)",
        "-U", $Node.User,
        "-d", $Node.Db,
        "-q", "-t", "-A", "-F", "|",
        "-v", "ON_ERROR_STOP=1"
    )
    if (-not [string]::IsNullOrWhiteSpace($TargetSchema)) {
        $args += @("-v", "target_schema=$TargetSchema")
    }
    $args += @("-f", $SqlFile)
    $psi.Arguments = ($args | ForEach-Object {
        if ($_ -match '\s') { '"' + $_ + '"' } else { $_ }
    }) -join " "

    $proc = New-Object System.Diagnostics.Process
    $proc.StartInfo = $psi
    [void]$proc.Start()
    $stdout = $proc.StandardOutput.ReadToEnd()
    $stderr = $proc.StandardError.ReadToEnd()
    $proc.WaitForExit()

    if ($proc.ExitCode -ne 0) {
        return [PSCustomObject]@{
            Success = $false
            Error   = ($stderr.Trim() -replace '\r?\n', ' ')
            Lines   = @()
        }
    }

    # Query READ ONLY kiem tra dinh nghia index ux_one_active_ticket_per_seat
    $idxSql = @"
SET search_path TO $TargetSchema;
BEGIN TRANSACTION READ ONLY;
SELECT 'INDEX_DEF|' || COALESCE(
    (SELECT regexp_replace(indexdef, '\s+ON\s+([^.\s]+\.)?tickets\s+', ' ON tickets ')
     FROM pg_indexes
     WHERE schemaname = current_schema()
       AND tablename = 'tickets'
       AND indexname = 'ux_one_active_ticket_per_seat'
     LIMIT 1),
    'MISSING');
COMMIT;
"@
    $psi2 = New-Object System.Diagnostics.ProcessStartInfo
    $psi2.FileName = $PsqlExe
    $psi2.RedirectStandardInput = $true
    $psi2.RedirectStandardOutput = $true
    $psi2.RedirectStandardError = $true
    $psi2.UseShellExecute = $false
    $psi2.CreateNoWindow = $true
    $args2 = @(
        "-X",
        "-h", $Node.Host,
        "-p", "$($Node.Port)",
        "-U", $Node.User,
        "-d", $Node.Db,
        "-q", "-t", "-A",
        "-v", "ON_ERROR_STOP=1"
    )
    $psi2.Arguments = ($args2 | ForEach-Object {
        if ($_ -match '\s') { '"' + $_ + '"' } else { $_ }
    }) -join " "

    $proc2 = New-Object System.Diagnostics.Process
    $proc2.StartInfo = $psi2
    [void]$proc2.Start()
    $proc2.StandardInput.Write($idxSql)
    $proc2.StandardInput.Close()
    $stdout2 = $proc2.StandardOutput.ReadToEnd()
    $stderr2 = $proc2.StandardError.ReadToEnd()
    $proc2.WaitForExit()

    if ($proc2.ExitCode -ne 0) {
        return [PSCustomObject]@{
            Success = $false
            Error   = ($stderr2.Trim() -replace '\r?\n', ' ')
            Lines   = @()
        }
    }

    $allLines = @()
    $allLines += ($stdout -split '\r?\n' | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $allLines += ($stdout2 -split '\r?\n' | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })

    return [PSCustomObject]@{
        Success = $true
        Error   = ""
        Lines   = $allLines
    }
}

function Parse-NodeDigest {
    param([object]$Node, [string[]]$Lines)
    $digest = [PSCustomObject]@{
        Id          = $Node.Id
        Endpoint    = "$($Node.Host):$($Node.Port)/$($Node.Db)"
        Reachable   = $true
        Error       = ""
        LastApplied = $null
        EntryCount  = 0
        SeatsHash   = ""
        TicketsHash = ""
        ResultsHash = ""
        StateHash   = ""
        IndexDef    = "MISSING"
        ChecksPass  = $true
        FailedChecks = @()
    }

    foreach ($line in $Lines) {
        $parts = $line -split '\|', 4
        if ($parts.Count -lt 2) { continue }
        $sec = $parts[0]
        if ($sec -eq "PROGRESS") {
            if ($parts[1] -eq "last_applied") { $digest.LastApplied = [long]$parts[2] }
            elseif ($parts[1] -eq "applied_results_count") { $digest.EntryCount = [long]$parts[2] }
        }
        elseif ($sec -eq "SUMMARY") {
            if ($parts[1] -eq "seats_hash")   { $digest.SeatsHash = $parts[2] }
            elseif ($parts[1] -eq "tickets_hash") { $digest.TicketsHash = $parts[2] }
            elseif ($parts[1] -eq "results_hash") { $digest.ResultsHash = $parts[2] }
            elseif ($parts[1] -eq "state_hash")   { $digest.StateHash = $parts[2] }
        }
        elseif ($sec -eq "CHECK") {
            $chkName = $parts[1]
            $chkStatus = $parts[2]
            if ($chkStatus -ne "PASS") {
                $digest.ChecksPass = $false
                $msg = if ($parts.Count -ge 4) { $parts[3] } else { $chkStatus }
                $digest.FailedChecks += "$chkName ($msg)"
            }
        }
        elseif ($sec -eq "INDEX_DEF") {
            $digest.IndexDef = $parts[1]
        }
    }

    if ($digest.IndexDef -eq "MISSING") {
        $digest.ChecksPass = $false
        $digest.FailedChecks += "INDEX_MISSING (ux_one_active_ticket_per_seat)"
    }

    return $digest
}

function Short-Hash {
    param([string]$Hash)
    if ([string]::IsNullOrWhiteSpace($Hash)) { return "-" }
    if ($Hash -eq "EMPTY") { return "EMPTY" }
    if ($Hash.Length -le 8) { return $Hash }
    return $Hash.Substring(0, 8)
}

# --- MAIN ---
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($CompareSqlPath)) {
    $CompareSqlPath = Join-Path $scriptDir "compare-db.sql"
}
if (-not (Test-Path $CompareSqlPath)) {
    Write-Error "Khong tim thay file SQL: $CompareSqlPath"
    exit $EXIT_USAGE_ERROR
}

$psqlExe = Resolve-PsqlExecutable -ExplicitPath $PsqlPath
if (-not $psqlExe) {
    Write-Output "BLOCKED/NOT_RUN: Khong tim thay psql.exe trong PATH hoac C:\Program Files\PostgreSQL\*\bin\"
    exit $EXIT_USAGE_ERROR
}

try {
    $nodeList = Parse-NodeList -FilePath $NodesFile -InlineNodes $Nodes
}
catch {
    Write-Error $_
    exit $EXIT_USAGE_ERROR
}

if (-not $nodeList -or $nodeList.Count -eq 0) {
    Write-Error "Danh sach node rong."
    exit $EXIT_USAGE_ERROR
}

# Lay mat khau tu bien moi truong (khong in, khong ghi log)
$prevPgPassword = $env:PGPASSWORD
$prevPgTimeout  = $env:PGCONNECT_TIMEOUT
$resolvedPw = [Environment]::GetEnvironmentVariable($PasswordEnvVar)
if ([string]::IsNullOrWhiteSpace($resolvedPw)) {
    $resolvedPw = [Environment]::GetEnvironmentVariable("RAILWAY_TEST_DB_PASSWORD")
}
if ([string]::IsNullOrWhiteSpace($resolvedPw)) {
    $resolvedPw = $prevPgPassword
}
if ([string]::IsNullOrWhiteSpace($resolvedPw)) {
    Write-Error "Thieu mat khau PostgreSQL trong bien moi truong ($PasswordEnvVar / RAILWAY_TEST_DB_PASSWORD / PGPASSWORD)."
    exit $EXIT_USAGE_ERROR
}

try {
    $env:PGPASSWORD = $resolvedPw

    $digests = @()
    $hasLag = $false

    for ($attempt = 0; $attempt -le $PollRetries; $attempt++) {
        $digests = @()
        foreach ($n in $nodeList) {
            $res = Invoke-PsqlCompare -PsqlExe $psqlExe -Node $n -SqlFile $CompareSqlPath -TargetSchema $Schema -TimeoutSec $ConnectTimeoutSeconds
            if (-not $res.Success) {
                $digests += [PSCustomObject]@{
                    Id           = $n.Id
                    Endpoint     = "$($n.Host):$($n.Port)/$($n.Db)"
                    Reachable    = $false
                    Error        = $res.Error
                    LastApplied  = $null
                    EntryCount   = 0
                    SeatsHash    = ""
                    TicketsHash  = ""
                    ResultsHash  = ""
                    StateHash    = ""
                    IndexDef     = ""
                    ChecksPass   = $false
                    FailedChecks = @("UNREACHABLE")
                }
            }
            else {
                $digests += Parse-NodeDigest -Node $n -Lines $res.Lines
            }
        }

        $reachable = @($digests | Where-Object { $_.Reachable })
        $hasLag = $false
        if ($reachable.Count -ge 2) {
            $maxApplied = ($reachable | Measure-Object -Property LastApplied -Maximum).Maximum
            $minApplied = ($reachable | Measure-Object -Property LastApplied -Minimum).Minimum
            if ($minApplied -lt $maxApplied) {
                $hasLag = $true
            }
        }

        if ($hasLag -and $attempt -lt $PollRetries) {
            Write-Output ("[POLL {0}/{1}] Phat hien LAG (minLastApplied={2}, maxLastApplied={3}), cho {4}s..." -f ($attempt + 1), $PollRetries, $minApplied, $maxApplied, $PollIntervalSeconds)
            Start-Sleep -Seconds $PollIntervalSeconds
        }
        else {
            break
        }
    }

    # Phan tich ket qua
    $unreachableNodes = @($digests | Where-Object { -not $_.Reachable })
    $reachableNodes   = @($digests | Where-Object { $_.Reachable })
    $checkFailedNodes = @($reachableNodes | Where-Object { -not $_.ChecksPass })

    # Kiem tra index definition consistency giua cac node reachable co index
    $indexMismatch = $false
    $idxGroups = @($reachableNodes | Where-Object { $_.IndexDef -ne "MISSING" } | Group-Object -Property IndexDef)
    if ($idxGroups.Count -gt 1) {
        $indexMismatch = $true
    }

    # Kiem tra DIVERGENCE: CHỈ so sanh cac node co CUNG last_applied
    $divergenceDetails = @()
    $byApplied = $reachableNodes | Group-Object -Property LastApplied
    foreach ($grp in $byApplied) {
        $grpNodes = @($grp.Group)
        if ($grpNodes.Count -lt 2) { continue }
        $uniqueState = @($grpNodes | Select-Object -ExpandProperty StateHash -Unique)
        if ($uniqueState.Count -gt 1) {
            $ref = $grpNodes[0]
            $diffTables = @()
            if (@($grpNodes | Select-Object -ExpandProperty SeatsHash -Unique).Count -gt 1) { $diffTables += "seats" }
            if (@($grpNodes | Select-Object -ExpandProperty TicketsHash -Unique).Count -gt 1) { $diffTables += "tickets" }
            if (@($grpNodes | Select-Object -ExpandProperty ResultsHash -Unique).Count -gt 1) { $diffTables += "request_results" }
            $diffNodes = @($grpNodes | ForEach-Object { "$($_.Id)(state=$(Short-Hash $_.StateHash),seats=$(Short-Hash $_.SeatsHash),tickets=$(Short-Hash $_.TicketsHash),results=$(Short-Hash $_.ResultsHash))" })
            $divergenceDetails += "last_applied=$($grp.Name): lech bang [$($diffTables -join ', ')] giua: $($diffNodes -join '; ')"
        }
    }

    # In bang tom tat
    Write-Output ""
    Write-Output ("=== COMPARE-5DB SUMMARY (schema={0}, nodes={1}) ===" -f $Schema, $digests.Count)
    $table = $digests | ForEach-Object {
        $statusStr = if (-not $_.Reachable) { "UNREACHABLE" }
                     elseif (-not $_.ChecksPass) { "CHECK_FAILED" }
                     elseif ($reachableNodes.Count -ge 2 -and $_.LastApplied -lt (($reachableNodes | Measure-Object -Property LastApplied -Maximum).Maximum)) { "LAG" }
                     else { "OK" }
        [PSCustomObject]@{
            Node        = $_.Id
            Endpoint    = $_.Endpoint
            Status      = $statusStr
            LastApplied = if ($null -eq $_.LastApplied) { "-" } else { $_.LastApplied }
            Entries     = if ($_.Reachable) { $_.EntryCount } else { "-" }
            Seats       = Short-Hash $_.SeatsHash
            Tickets     = Short-Hash $_.TicketsHash
            Results     = Short-Hash $_.ResultsHash
            StateHash   = Short-Hash $_.StateHash
            Checks      = if (-not $_.Reachable) { "ERR" } elseif ($_.ChecksPass) { "PASS" } else { "FAIL" }
            Index       = if (-not $_.Reachable) { "-" } elseif ($_.IndexDef -eq "MISSING") { "MISSING" } else { "OK" }
        }
    }
    $table | Format-Table -AutoSize | Out-String | ForEach-Object { $_.TrimEnd() }

    if ($unreachableNodes.Count -gt 0) {
        Write-Output "--- UNREACHABLE NODES ---"
        foreach ($u in $unreachableNodes) {
            Write-Output ("  * {0} ({1}): {2}" -f $u.Id, $u.Endpoint, $u.Error)
        }
    }
    if ($checkFailedNodes.Count -gt 0 -or $indexMismatch) {
        Write-Output "--- LOCAL CHECK / INDEX FAILURES ---"
        foreach ($cf in $checkFailedNodes) {
            Write-Output ("  * {0}: {1}" -f $cf.Id, ($cf.FailedChecks -join "; "))
        }
        if ($indexMismatch) {
            Write-Output "  * INDEX_DEF_MISMATCH: Dinh nghia ux_one_active_ticket_per_seat khac nhau giua cac node:"
            foreach ($rn in $reachableNodes) {
                Write-Output ("    - {0}: {1}" -f $rn.Id, $rn.IndexDef)
            }
        }
    }
    if ($divergenceDetails.Count -gt 0) {
        Write-Output "--- DIVERGENCE DETAILS ---"
        foreach ($d in $divergenceDetails) {
            Write-Output ("  * {0}" -f $d)
        }
    }
    if ($hasLag) {
        Write-Output "--- LAG DETAILS ---"
        $maxLA = ($reachableNodes | Measure-Object -Property LastApplied -Maximum).Maximum
        foreach ($rn in ($reachableNodes | Where-Object { $_.LastApplied -lt $maxLA })) {
            Write-Output ("  * {0}: last_applied={1} < max={2} (khong so state_hash voi node tien xa hon)" -f $rn.Id, $rn.LastApplied, $maxLA)
        }
    }

    # Ket luan cuoi cung
    $conclusion = "CONSISTENT"
    $exitCode   = $EXIT_CONSISTENT

    if ($checkFailedNodes.Count -gt 0 -or $indexMismatch) {
        $conclusion = "CHECK_FAILED"
        $exitCode   = $EXIT_CHECK_FAILED
    }
    elseif ($divergenceDetails.Count -gt 0) {
        $conclusion = "DIVERGENCE"
        $exitCode   = $EXIT_DIVERGENCE
    }
    elseif ($unreachableNodes.Count -gt 0) {
        $conclusion = "UNREACHABLE"
        $exitCode   = $EXIT_UNREACHABLE
    }
    elseif ($hasLag) {
        $conclusion = "LAG_UNRESOLVED"
        $exitCode   = $EXIT_LAG_UNRESOLVED
    }

    Write-Output ""
    Write-Output ("CONCLUSION: {0} (exit={1})" -f $conclusion, $exitCode)
    exit $exitCode
}
finally {
    $env:PGPASSWORD = $prevPgPassword
    $env:PGCONNECT_TIMEOUT = $prevPgTimeout
}