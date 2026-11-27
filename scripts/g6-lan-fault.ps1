<#
.SYNOPSIS
    TV6-D26: independent G6 fault runner; DryRun is the default.
.DESCRIPTION
    DryRun validates docker/lan.yml via Compose with documentation-only addresses,
    then executes a FAKE five-node model: select Leader, stop/restart, partition
    3-2 (12 directed cross links), heal and collect. No Docker mutation, TCP probe,
    PowerShell remoting or firewall change is made in DryRun/SelfTest.

    Live requires five already deployed Windows hosts and configured, authenticated
    PowerShell remoting with rights to Docker and Windows Firewall. It reuses D21
    stop-node.ps1/restart-node.ps1/select-leader.ps1. The "kill" phase is D21 stop
    with a one-second grace period, not physical power loss. Only backend stops;
    PostgreSQL stays running. Per-run firewall rules block peer TCP 9200 only.
    Container-origin mesh probes MUST observe the actual cut, otherwise fail.
    Recovery is attempted even after partial faults; only own tagged rules removed.
    No rm/down/reset/volume deletion, no BOOK or SQL writes, no automatic G6 PASS.

    Inventory JSON (local file, no passwords): {"nodes":[ five objects with
    "nodeId":"node-1", "host":"<measured IPv4>", "repoPath":"D:\\HTPT",
    "container":"railway-lan-node-1-backend-1",
    "postgres":"railway-lan-node-1-postgres-1" ]}.
    Each host must have the frozen CandidateRevision and matching D21 scripts.
    No remoting/firewall setup is silently enabled by this runner.
    Exit 0=runner gate passed, 1=failure/recovery error, 2=missing Live prerequisites.
.EXAMPLE
    .\scripts\g6-lan-fault.ps1
.EXAMPLE
    .\scripts\g6-lan-fault.ps1 -Mode SelfTest
.EXAMPLE
    .\scripts\g6-lan-fault.ps1 -Mode Live -InventoryPath <local.json> -CandidateRevision <40-char-SHA> -Credential (Get-Credential)
.NOTES
    Logs default to a new TEMP folder. Formal D26 evidence is separate from G6.
    DryRun fixtures/observations are not physical LAN, Raft or SQL evidence.
#>
[CmdletBinding()]
param(
    [ValidateSet('DryRun','SelfTest','Live')][string]$Mode = 'DryRun',
    [string]$InventoryPath = '',
    [ValidatePattern('^$|^[0-9a-f]{40}$')][string]$CandidateRevision = '',
    [PSCredential]$Credential,
    [ValidateRange(5,120)][int]$WaitSeconds = 45,
    [ValidateSet('None','Kill','Restart','Partition','Heal','Logs','StatusTimeout','RestartAlways')]
    [string]$InjectFailure = 'None',
    [string]$OutputDirectory = '',
    [string]$ResultLog = ''
)
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if (!$OutputDirectory) { $OutputDirectory = Join-Path ([IO.Path]::GetTempPath()) ('railway-g6-' + [Guid]::NewGuid().ToString('N')) }
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
if (!$ResultLog) { $ResultLog = Join-Path $OutputDirectory 'result.log' }
$ResultLog = [IO.Path]::GetFullPath($ResultLog)
$transcript = [Collections.Generic.List[string]]::new()
$utf8 = [Text.UTF8Encoding]::new($false)
$ids = @('node-1','node-2','node-3','node-4','node-5')

function Log([string]$Text) {
    $line = [DateTime]::UtcNow.ToString('o') + ' ' + $Text
    $transcript.Add($line); Write-Host $line
}
function Require([bool]$Condition, [string]$Message) { if (!$Condition) { throw $Message } }
function Blocked([string]$Message) { throw [NotSupportedException]::new($Message) }
function Local([string]$Exe, [string[]]$Arguments, [int[]]$Allowed = @(0)) {
    $saved = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
    try { $raw = & $Exe @Arguments 2>&1; $code = $LASTEXITCODE }
    finally { $ErrorActionPreference = $saved }
    $value = ($raw | ForEach-Object { $_.ToString() }) -join "`n"
    Require ($code -in $Allowed) "Local command $Exe failed exit=$code : $value"
    return [PSCustomObject]@{ Code=$code; Text=$value }
}

function Validate-Inventory($Nodes) {
    Require (@($Nodes).Count -eq 5 -and (@($Nodes.nodeId | Sort-Object -Unique) -join ',') -eq ($ids -join ',')) 'Inventory must contain exactly five unique voters'
    Require (@($Nodes.host | Sort-Object -Unique).Count -eq 5) 'Five separate physical host addresses required'
    foreach ($node in $Nodes) {
        $ip = $null
        Require ([Net.IPAddress]::TryParse($node.host,[ref]$ip) -and $ip.AddressFamily -eq [Net.Sockets.AddressFamily]::InterNetwork) 'Measured IPv4 required'
        Require ($node.host -match '^(10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.)') 'Live inventory requires private LAN IPv4, not loopback/APIPA/test addresses'
        Require ($node.host -notmatch '^192\.168\.(10\.1[1-5]|1\.10[1-5])$') 'D23 template addresses are not measured inventory'
        Require ($node.repoPath -match '^[A-Za-z]:\\' -and $node.repoPath -notmatch '[\r\n]') 'Absolute Windows repository path required'
        foreach ($name in @($node.container,$node.postgres)) { Require ($name -match '^[a-zA-Z0-9][a-zA-Z0-9_.-]+$') 'Explicit safe container names required' }
        Require ($node.container -ne $node.postgres) 'Backend and PostgreSQL must be separate containers'
    }
}

function New-Context([string]$Kind, [string]$Failure = 'None') {
    $nodes = @(1..5 | ForEach-Object { [PSCustomObject]@{ nodeId="node-$_"; host="198.51.100.$(10+$_)";
        repoPath=$repo; container="railway-lan-node-$_-backend-1"; postgres="railway-lan-node-$_-postgres-1" } })
    $running = [Collections.Generic.HashSet[string]]::new(); foreach ($id in $ids) { $null=$running.Add($id) }
    return @{ Kind=$Kind; Nodes=$nodes; Running=$running; Links=[Collections.Generic.HashSet[string]]::new()
        Actions=[Collections.Generic.List[string]]::new(); Changed=[Collections.Generic.HashSet[string]]::new()
        Rules=[Collections.Generic.HashSet[string]]::new(); Sessions=@{}; Volumes=@{}
        Leader='node-4'; Term=8L; Commit=43L; Failure=$Failure; Injected=$false
        Group=('Railway-G6-' + [Guid]::NewGuid().ToString('N')); Index=0 }
}

function Fail-Once($Context, [string]$Stage) {
    if ($Context.Failure -eq $Stage -and !$Context.Injected) {
        $Context.Injected=$true; throw "INJECTED $Stage after possible partial mutation"
    }
}

# Runs only inside an explicitly connected host session in Live mode.
$remoteBody = {
    param($Verb,$Node,$Others,$Group,$Candidate,$Hashes)
    $ErrorActionPreference='Stop'
    function Remote-Docker([string[]]$Arguments, [int[]]$Allowed=@(0)) {
        $saved=$ErrorActionPreference; $ErrorActionPreference='Continue'
        try { $raw=& docker @Arguments 2>&1; $code=$LASTEXITCODE } finally { $ErrorActionPreference=$saved }
        $text=($raw | ForEach-Object { $_.ToString() }) -join "`n"
        if ($code -notin $Allowed) { throw "docker $($Arguments -join ' ') exit=$code : $text" }
        [PSCustomObject]@{ Code=$code; Text=$text }
    }
    switch ($Verb) {
        'Inspect' {
            $head=(& git -C $Node.repoPath rev-parse HEAD).Trim()
            if ($LASTEXITCODE -ne 0 -or $head -ne $Candidate) { throw 'Remote candidate mismatch' }
            foreach ($file in @('stop-node.ps1','restart-node.ps1')) {
                $path=Join-Path $Node.repoPath "scripts/$file"
                if ((Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash -ne $Hashes[$file]) { throw 'Remote D21 script mismatch' }
            }
            $backend=(Remote-Docker @('inspect','--format','{{.State.Running}}',$Node.container)).Text.Trim()
            $pg=(Remote-Docker @('inspect','--format','{{.State.Running}}',$Node.postgres)).Text.Trim()
            if ($backend -ne 'true' -or $pg -ne 'true') { throw 'All backend/PG containers must already be running' }
            # Read selected mount fields only; never return Config.Env / passwords.
            $mounts=(Remote-Docker @('inspect','--format','{{json .Mounts}}',$Node.postgres)).Text | ConvertFrom-Json
            $volumes=@($mounts | Where-Object Type -eq 'volume' | ForEach-Object { "$($_.Name)->$($_.Destination)" } | Sort-Object)
            if (!$volumes.Count) { throw 'Persistent PostgreSQL volume required' }
            [PSCustomObject]@{ Code=0; Text=($volumes -join ';') }
        }
        { $_ -in @('Kill','Restart') } {
            $file=if ($Verb -eq 'Kill') {'stop-node.ps1'} else {'restart-node.ps1'}
            $arguments=@('-NoProfile','-NonInteractive','-File',(Join-Path $Node.repoPath "scripts/$file"),'-ContainerName',$Node.container)
            if ($Verb -eq 'Kill') { $arguments+=@('-StopTimeoutSeconds','1') }
            $saved=$ErrorActionPreference; $ErrorActionPreference='Continue'
            try { $raw=& powershell.exe @arguments 2>&1; $code=$LASTEXITCODE } finally { $ErrorActionPreference=$saved }
            [PSCustomObject]@{ Code=$code; Text=(($raw | ForEach-Object { $_.ToString() }) -join "`n") }
        }
        'Partition' {
            # Block only cross-side Raft TCP. Do not touch client, PG or management ports.
            $null=New-NetFirewallRule -Name "$Group-in" -DisplayName "$Group-in" -Group $Group -Direction Inbound -Protocol TCP -LocalPort 9200 -RemoteAddress $Others -Action Block -Profile Any
            $null=New-NetFirewallRule -Name "$Group-out" -DisplayName "$Group-out" -Group $Group -Direction Outbound -Protocol TCP -RemotePort 9200 -RemoteAddress $Others -Action Block -Profile Any
            [PSCustomObject]@{ Code=0; Text="Scoped peer block installed: $($Others -join ',')" }
        }
        'Heal' {
            Get-NetFirewallRule -Group $Group -ErrorAction SilentlyContinue | Remove-NetFirewallRule -ErrorAction Stop
            [PSCustomObject]@{ Code=0; Text="Own firewall group removed: $Group" }
        }
        'Mesh' { Remote-Docker @('exec',$Node.container,'nc','-z','-w','1',$Others[0],'9200') @(0,1) }
        'Logs' {
            foreach ($name in @($Node.container,$Node.postgres)) {
                $value=Remote-Docker @('logs','--timestamps','--tail','120',$name)
                [PSCustomObject]@{ Code=$value.Code; Text="container=$name`n$($value.Text)" }
            }
        }
    }
}

function Action($Context, [string]$Verb, [string]$Id, [string[]]$Others=@()) {
    $Context.Actions.Add("$Verb $Id")
    $node=@($Context.Nodes | Where-Object nodeId -eq $Id)[0]
    if ($Context.Kind -eq 'Live') {
        $hashes=@{}; foreach ($file in @('stop-node.ps1','restart-node.ps1')) { $hashes[$file]=(Get-FileHash -LiteralPath (Join-Path $PSScriptRoot $file) -Algorithm SHA256).Hash }
        $job=Invoke-Command -Session $Context.Sessions[$Id] -ScriptBlock $remoteBody -ArgumentList $Verb,$node,$Others,$Context.Group,$CandidateRevision,$hashes -AsJob
        try {
            if (!(Wait-Job -Job $job -Timeout 30)) { Stop-Job $job; throw [TimeoutException]::new("Remote $Verb deadline: $Id") }
            $values=@(Receive-Job $job -ErrorAction Stop)
            Require ($values.Count -gt 0) "No remote result: $Verb $Id"
            foreach ($value in $values) {
                Log "LIVE $Verb node=$Id exit=$($value.Code) $($value.Text)"
                if ($Verb -eq 'Mesh') { Require ($value.Code -in @(0,1)) 'Mesh probe error' }
                else { Require ($value.Code -eq 0) "Remote $Verb failed: $Id" }
            }
            return $values[-1]
        } finally { Remove-Job $job -Force -ErrorAction SilentlyContinue }
    }
    if ($Verb -eq 'Restart' -and $Context.Failure -eq 'RestartAlways') { throw 'INJECTED persistent restart failure' }
    $code=0; $text='FAKE_ONLY'
    switch ($Verb) {
        'Kill' {
            $null=$Context.Running.Remove($Id)
            $Context.Leader=@($ids | Where-Object { $Context.Running.Contains($_) })[0]; $Context.Term++
        }
        'Restart' { $null=$Context.Running.Add($Id) }
        'Partition' { foreach ($other in $Others) { $null=$Context.Links.Add("$Id>$other") } }
        'Heal' { foreach ($link in @($Context.Links | Where-Object { $_.StartsWith("$Id>") })) { $null=$Context.Links.Remove($link) } }
        'Mesh' { if ($Context.Links.Contains("$Id>$($Others[0])")) { $code=1 } }
        'Inspect' { $text="FAKE_PG_VOLUME_$Id" }
    }
    Fail-Once $Context $Verb
    Log "FAKE action=$Verb node=$Id exit=$code peers=$($Others -join ',') $text"
    return [PSCustomObject]@{ Code=$code; Text=$text }
}

function Snapshot($Context) {
    if ($Context.Kind -eq 'Live') {
        $endpoints=($Context.Nodes | ForEach-Object { "$($_.nodeId)@$($_.host):9100" }) -join ','
        $result=Local 'powershell.exe' @('-NoProfile','-NonInteractive','-File',(Join-Path $PSScriptRoot 'select-leader.ps1'),'-Endpoints',$endpoints,'-AsJson','-Quiet','-TimeoutMs','1000') @(0,2,3)
        $rows=@(($result.Text | ConvertFrom-Json).nodes)
    } else {
        if ($Context.Running.Count -lt 5) { Fail-Once $Context 'StatusTimeout' }
        $rows=@(foreach ($id in $ids) { [PSCustomObject]@{ nodeId=$id; online=$Context.Running.Contains($id); storageHealthy=$true;
            role=$(if ($id -eq $Context.Leader) {'LEADER'} else {'FOLLOWER'}); term=$Context.Term; commitIndex=$Context.Commit; lastApplied=$Context.Commit } })
    }
    Require ($rows.Count -eq 5) 'Five status observations required'
    Log "STATUS mode=$($Context.Kind) $($rows | ConvertTo-Json -Compress -Depth 5)"
    return $rows
}

function Wait-Stable($Context, [string[]]$Expected, [long]$AppliedFloor=0) {
    $watch=[Diagnostics.Stopwatch]::StartNew(); $stable=0; $previous=''
    do {
        $rows=@(Snapshot $Context); $active=@($rows | Where-Object { $_.nodeId -in $Expected -and $_.online -and $_.storageHealthy })
        $leaders=@($active | Where-Object role -eq 'LEADER'); $terms=@($active.term | Sort-Object -Unique)
        $valid=$active.Count -eq $Expected.Count -and $leaders.Count -eq 1 -and $terms.Count -eq 1 -and $terms[0] -gt 0
        foreach ($row in $active) {
            $valid=$valid -and ($row.commitIndex -is [int] -or $row.commitIndex -is [long]) -and
                ($row.lastApplied -is [int] -or $row.lastApplied -is [long]) -and $row.lastApplied -ge $AppliedFloor -and $row.lastApplied -le $row.commitIndex
        }
        if ($valid) {
            $key="$($leaders[0].nodeId)@$($terms[0])"
            if ($key -eq $previous) { $stable++ } else { $stable=1 }; $previous=$key
            if ($stable -ge 3) { return $leaders[0].nodeId }
        } else { $stable=0 }
        if ($Context.Kind -ne 'Live' -and !$valid) { throw [TimeoutException]::new('Fake status did not stabilize') }
        if ($Context.Kind -eq 'Live') { Start-Sleep -Milliseconds 300 }
    } while ($watch.Elapsed.TotalSeconds -lt $WaitSeconds)
    throw [TimeoutException]::new('Raft observation deadline; no stable unique Leader')
}

function Mesh($Context, [bool]$Partitioned) {
    $open=0; $closed=0
    foreach ($from in $ids) { foreach ($to in @($ids | Where-Object { $_ -ne $from })) {
        $sourceMajority=$from -in @('node-1','node-2','node-3'); $targetMajority=$to -in @('node-1','node-2','node-3')
        $expected=if ($Partitioned -and $sourceMajority -ne $targetMajority) {1} else {0}
        $address=if ($Context.Kind -eq 'Live') { @($Context.Nodes | Where-Object nodeId -eq $to)[0].host } else {$to}
        $result=Action $Context 'Mesh' $from @($address)
        Require ($result.Code -eq $expected) "Peer cut not observed: $from>$to expected=$expected actual=$($result.Code)"
        if ($result.Code -eq 0) {$open++} else {$closed++}
    } }
    Log "MESH mode=$($Context.Kind) open=$open blocked=$closed partition=$Partitioned (transport only; no AC09 claim)"
}

function Collect-All($Context, [string]$Phase) {
    $errors=[Collections.Generic.List[string]]::new()
    foreach ($id in $ids) {
        try { $null=Action $Context 'Logs' $id } catch { $errors.Add("$id : $($_.Exception.Message)") }
    }
    Log "COLLECT phase=$Phase attempted=5 errors=$($errors.Count)"
    Require ($errors.Count -eq 0) ($errors -join '; ')
}

function Run-Flow($Context) {
    $errors=[Collections.Generic.List[string]]::new()
    try {
        foreach ($id in $ids) { $Context.Volumes[$id]=(Action $Context 'Inspect' $id).Text }
        $leader=Wait-Stable $Context $ids
        $before=@(Snapshot $Context); $floor=($before.commitIndex | Measure-Object -Maximum).Maximum
        Mesh $Context $false; Collect-All $Context 'baseline'
        Log "TC-G6-RUNNER-KILL selectedLeader=$leader; D21 stop timeout=1; G6_ACCEPTANCE=NOT_RUN"
        $null=$Context.Changed.Add($leader) # Journal BEFORE mutation, including partial failure.
        $null=Action $Context 'Kill' $leader
        $replacement=Wait-Stable $Context @($ids | Where-Object { $_ -ne $leader })
        Require ($replacement -ne $leader) 'Stopped leader selected again'
        $null=Action $Context 'Restart' $leader
        $null=Wait-Stable $Context $ids $floor
        $null=$Context.Changed.Remove($leader)
        Collect-All $Context 'restarted'
        foreach ($id in $ids) {
            $others=@($ids | Where-Object { ($_ -in @('node-1','node-2','node-3')) -ne ($id -in @('node-1','node-2','node-3')) })
            $addresses=if ($Context.Kind -eq 'Live') { @($Context.Nodes | Where-Object { $_.nodeId -in $others } | ForEach-Object host) } else {$others}
            $null=$Context.Rules.Add($id)
            $null=Action $Context 'Partition' $id $addresses
        }
        Mesh $Context $true
        $partitionStatus=@(Snapshot $Context)
        Require (@($partitionStatus | Where-Object { $_.online -and $_.storageHealthy }).Count -eq 5) 'Partition must keep all five client endpoints and processes alive'
        $null=Wait-Stable $Context @('node-1','node-2','node-3')
        Collect-All $Context 'partition-both-sides'
        foreach ($id in $ids) { $null=Action $Context 'Heal' $id; $null=$Context.Rules.Remove($id) }
        Mesh $Context $false
        $null=Wait-Stable $Context $ids $floor
    } catch { $errors.Add($_.Exception.Message) }
    finally {
        # Independent recovery attempts: one failed heal/start/collector never skips others.
        foreach ($id in @($Context.Rules)) {
            try { $null=Action $Context 'Heal' $id; $null=$Context.Rules.Remove($id) } catch { $errors.Add("RECOVERY Heal $id : $($_.Exception.Message)") }
        }
        foreach ($id in @($Context.Changed)) {
            try { $null=Action $Context 'Restart' $id; $null=$Context.Changed.Remove($id) } catch { $errors.Add("RECOVERY Restart $id : $($_.Exception.Message)") }
        }
        try { Collect-All $Context 'finally' } catch { $errors.Add($_.Exception.Message) }
        foreach ($id in @($Context.Volumes.Keys)) {
            try { Require ((Action $Context 'Inspect' $id).Text -eq $Context.Volumes[$id]) "PostgreSQL volume changed: $id" }
            catch { $errors.Add($_.Exception.Message) }
        }
    }
    Require ($errors.Count -eq 0) ($errors -join '; ')
    Log "RUNNER PASS mode=$($Context.Kind); kill/restart/partition/heal/log orchestration only; G6_ACCEPTANCE=NOT_RUN"
}

function Check-D25-Compose {
    if (!(Get-Command docker -ErrorAction SilentlyContinue)) { Blocked 'Docker CLI missing for D25 Compose dry-run validation' }
    $names=@('NODE_ID','LAN_IP','NODE_PEERS','DB_NAME','DB_PASSWORD','BACKEND_IMAGE','POSTGRES_IMAGE')
    $saved=@{}; foreach ($name in $names) { $saved[$name]=[Environment]::GetEnvironmentVariable($name,'Process') }
    $empty=Join-Path $OutputDirectory 'empty.env'; [IO.File]::WriteAllText($empty,'',$utf8)
    try {
        foreach ($n in 1..5) {
            $env:NODE_ID="node-$n"; $env:LAN_IP="198.51.100.$(10+$n)"; $env:DB_NAME="railway_node$n"
            $env:NODE_PEERS=(1..5 | ForEach-Object { "node-$_@198.51.100.$(10+$_):9200" }) -join ','
            $env:DB_PASSWORD='DRY_RUN_ONLY_NOT_A_REAL_CREDENTIAL'; $env:BACKEND_IMAGE='railway-backend-node:d26-dry-run'; $env:POSTGRES_IMAGE='postgres:16-alpine'
            $value=Local 'docker' @('compose','--env-file',$empty,'-p',"tv6-d26-dry-$n",'-f',(Join-Path $repo 'docker/lan.yml'),'config','--format','json')
            $config=$value.Text | ConvertFrom-Json
            Require (@($config.services.PSObject.Properties).Count -eq 2) 'D25 must have exactly backend+postgres'
            Require ($config.services.backend.environment.NODE_ID -eq "node-$n" -and $config.services.backend.environment.DB_HOST -eq 'postgres') 'D25 node/DB mapping mismatch'
            Require ($config.services.postgres.environment.POSTGRES_DB -eq "railway_node$n" -and !$config.services.postgres.ports) 'D25 database must remain private'
            Log "D25_COMPOSE node-$n exit=0 PASS; TEST-NET-2 config fixture only, no Docker up/fault/probe"
        }
    } finally { foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name,$saved[$name],'Process') } }
}

function Self-Tests {
    $passed=0
    foreach ($failure in @('None','Kill','Restart','Partition','Heal','Logs','StatusTimeout','RestartAlways')) {
        $context=New-Context 'Fake' $failure; $failed=$false
        try { Run-Flow $context } catch { $failed=$true; Log "EXPECTED_TEST_FAILURE $failure : $($_.Exception.Message)" }
        Require ($failed -eq ($failure -ne 'None')) "Unexpected outcome in $failure"
        if ($failure -ne 'RestartAlways') { Require ($context.Running.Count -eq 5 -and $context.Links.Count -eq 0) "Recovery skipped in $failure" }
        else { Require ($failed -and $context.Changed.Count -eq 1) 'Persistent recovery failure hidden' }
        Require (@($context.Actions | Where-Object { $_ -like 'Logs *' }).Count -ge 5) "Final collection missing: $failure"
        $passed++; Log "SELFTEST PASS case=$failure"
    }
    $nodes=@(1..5 | ForEach-Object { [PSCustomObject]@{ nodeId="node-$_"; host="10.77.0.$(10+$_)"; repoPath='D:\HTPT'; container="test-backend-$_"; postgres="test-pg-$_" } })
    Validate-Inventory $nodes; $passed++
    foreach ($bad in @('Count','DuplicateHost','Loopback','SixthVoter')) {
        $copy=ConvertFrom-Json -InputObject (ConvertTo-Json -InputObject $nodes -Depth 6)
        switch ($bad) { 'Count' {$copy=$copy[0..3]} 'DuplicateHost' {$copy[1].host=$copy[0].host} 'Loopback' {$copy[0].host='127.0.0.1'} 'SixthVoter' {$copy[4].nodeId='node-6'} }
        $failed=$false; try { Validate-Inventory $copy } catch {$failed=$true}
        Require $failed "Unsafe inventory accepted: $bad"; $passed++
    }
    Log "SELFTEST PASS count=$passed; fixtures only; real fault/SQL/LAN acceptance NOT_RUN"
}

$context=$null; $exitCode=0
try {
    New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
    Log "TV6-D26 mode=$Mode baseline=$((Local 'git' @('-C',$repo,'rev-parse','HEAD')).Text.Trim())"
    Log "ScriptSHA256=$((Get-FileHash -LiteralPath $PSCommandPath -Algorithm SHA256).Hash) D25ComposeSHA256=$((Get-FileHash -LiteralPath (Join-Path $repo 'docker/lan.yml') -Algorithm SHA256).Hash)"
    if ($Mode -eq 'SelfTest') { Self-Tests }
    elseif ($Mode -eq 'DryRun') { Check-D25-Compose; $context=New-Context 'Fake' $InjectFailure; Run-Flow $context; Log 'DRY_RUN PASS; no remote/TCP/firewall/Docker mutations performed' }
    else {
        if (!$InventoryPath -or !$CandidateRevision) { Blocked 'Live requires measured five-host InventoryPath and frozen CandidateRevision; D25 physical hosts are not yet available.' }
        if ($InjectFailure -ne 'None') { Blocked 'Failure injection is test-only, never Live' }
        if ((Local 'git' @('-C',$repo,'rev-parse','HEAD')).Text.Trim() -ne $CandidateRevision) { Blocked 'Local HEAD differs from frozen candidate' }
        $context=New-Context 'Live'; $context.Nodes=@((Get-Content -LiteralPath $InventoryPath -Raw | ConvertFrom-Json).nodes)
        Validate-Inventory $context.Nodes
        $options=New-PSSessionOption -OpenTimeout 5000 -OperationTimeout 10000 -CancelTimeout 2000
        foreach ($node in $context.Nodes) {
            $arguments=@{ ComputerName=$node.host; SessionOption=$options; ErrorAction='Stop' }
            if ($Credential) { $arguments.Credential=$Credential }
            $context.Sessions[$node.nodeId]=New-PSSession @arguments
        }
        Run-Flow $context
    }
} catch [NotSupportedException] { $exitCode=2; Log "BLOCKED: $($_.Exception.Message)" }
catch { $exitCode=1; Log "FAIL: $($_.Exception.Message)" }
finally {
    if ($context) { foreach ($session in $context.Sessions.Values) { Remove-PSSession $session -ErrorAction SilentlyContinue } }
    Log "FINAL mode=$Mode exit=$exitCode; D26 runner/dry-run separate from G6. G6 fault/business/SQL acceptance: CHUA_NGHIEM_THU / NOT_RUN."
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $ResultLog) | Out-Null
    [IO.File]::WriteAllLines($ResultLog,$transcript,$utf8)
    Write-Host "Evidence=$ResultLog"
}
exit $exitCode
