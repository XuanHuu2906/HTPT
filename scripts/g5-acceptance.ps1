<#
.SYNOPSIS
    TV6-D20: separate script/self-test/dry-run gates from selected live G5 cases.
.DESCRIPTION
    DryRun is the default: executes D17/D19 fake contract tests, D16 fake faults,
    and explicitly FAKE SQL/log collector fixtures. Never invokes Docker in DryRun.
    Live is opt-in AFTER D20 PR integration, on an existing, isolated, fresh cluster.
    Live checks TC-AC03, TC-AC05B and TC-AC06 only; it is NOT all AC01-AC10/G5.
    SQL is read-only. No reset, volume deletion, Docker up/down or DB mutations.
    Run without other clients. G05/G06/G07 must initially be available on all DBs.
    Exit 0 = selected gate passed; 1 = assertion/command failed; 2 = blocked/incomplete.
.EXAMPLE
    .\scripts\g5-acceptance.ps1 -Mode SelfTest
.EXAMPLE
    .\scripts\g5-acceptance.ps1 -MavenPath C:\tools\maven\bin\mvn.cmd
.EXAMPLE
    .\scripts\g5-acceptance.ps1 -Mode Live -Project docker -CandidateRevision <40-char-SHA> -MavenPath <mvn.cmd>
.NOTES
    Set JAVA_HOME to the JDK used by the existing parent POM (currently 25).
    Default Compose service names match compose.dev.yml. For the isolated D15
    fixture use ServicePrefix=node, DbPrefix=pg, DbUser=railway_test and its env.
    Client ports are discovered from the supplied project's service mappings.
    Default artifacts go to a new TEMP directory; ResultLog can target formal evidence.
#>
[CmdletBinding()]
param(
    [ValidateSet('SelfTest','DryRun','Live')][string]$Mode = 'DryRun',
    [string]$MavenPath = 'mvn',
    [string]$ComposeFile = 'docker/compose.dev.yml',
    [ValidatePattern('^$|^[a-z0-9][a-z0-9_-]*$')][string]$Project = '',
    [ValidatePattern('^$|^[0-9a-f]{40}$')][string]$CandidateRevision = '',
    [ValidateSet('backend-node','node')][string]$ServicePrefix = 'backend-node',
    [ValidateSet('postgres','pg')][string]$DbPrefix = 'postgres',
    [ValidatePattern('^[A-Za-z_][A-Za-z0-9_]*$')][string]$DbUser = 'railway',
    [ValidatePattern('^[A-Za-z_][A-Za-z0-9_]*$')][string]$DbNamePrefix = 'railway_node',
    [ValidateRange(2,120)][int]$WaitSeconds = 30,
    [string]$OutputDirectory = '',
    [string]$ResultLog = ''
)
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if (!$OutputDirectory) { $OutputDirectory = Join-Path ([IO.Path]::GetTempPath()) ('railway-g5-' + [Guid]::NewGuid().ToString('N')) }
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
if (!$ResultLog) { $ResultLog = Join-Path $OutputDirectory 'result.log' }
$ResultLog = [IO.Path]::GetFullPath($ResultLog)
$report = [Collections.Generic.List[string]]::new()
$utf8 = [Text.UTF8Encoding]::new($false)
$nodes = @(1..5 | ForEach-Object { "node-$_" })
$ports = @{}
$allIds = [Collections.Generic.List[string]]::new()

function Log([string]$Text) {
    $line = [DateTime]::UtcNow.ToString('o') + ' ' + $Text
    $report.Add($line); Write-Host $line
}
function Require([bool]$Condition, [string]$Message) { if (!$Condition) { throw $Message } }
function Blocked([string]$Message) { throw [NotSupportedException]::new($Message) }
function Save([string]$Name, [string]$Text) { [IO.File]::WriteAllText((Join-Path $OutputDirectory $Name), $Text, $utf8) }

function Step([string]$Name, [string]$Exe, [string[]]$Arguments, [int[]]$Allowed = @(0)) {
    $path = Join-Path $OutputDirectory ($Name + '.log')
    Log "COMMAND $Name : $Exe $($Arguments -join ' ')"
    $saved = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
    try { & $Exe @Arguments *> $path; $code = $LASTEXITCODE }
    finally { $ErrorActionPreference = $saved }
    $text = if (Test-Path -LiteralPath $path) { [IO.File]::ReadAllText($path) } else { '' }
    Log "EXIT $Name=$code`n$text"
    if ($code -notin $Allowed) { throw "Command $Name failed: exit=$code" }
    return [PSCustomObject]@{ Code = $code; Text = $text }
}

function Compose([string]$Name, [string[]]$Arguments) {
    Require ($Mode -eq 'Live') 'Docker is forbidden outside Live mode'
    $path = if ([IO.Path]::IsPathRooted($ComposeFile)) { $ComposeFile } else { Join-Path $repo $ComposeFile }
    return Step $Name 'docker' (@('compose','-p',$Project,'-f',$path) + $Arguments)
}

function Parse-Results([string]$Text, [bool]$NormalFixtureOnly = $false) {
    $scenario = ''
    foreach ($line in ($Text -split "`r?`n")) {
        if ($line -match '^MODE=FAKE_TCP scenario=(\w+)') { $scenario = $Matches[1] }
        elseif ($line -match '^MODE=') { $scenario = '' }
        if ($NormalFixtureOnly -and $scenario -ne 'NORMAL') { continue }
        if ($line -match '^client=(\d+) requestId=([0-9a-f-]{36}) messageId=([0-9a-f-]{36}) .*outcome=(\w+) reasonCode=(\S+) appliedLogIndex=(\S+)') {
            [PSCustomObject]@{ client = [int]$Matches[1]; requestId = $Matches[2]; messageId = $Matches[3]
                outcome = $Matches[4]; reason = $Matches[5]; index = $Matches[6] }
        }
    }
}

function Assert-Six($Rows) {
    Require (@($Rows).Count -eq 6) 'Six client result rows required'
    Require (@($Rows.requestId | Sort-Object -Unique).Count -eq 6) 'Six distinct business requestIds required'
    Require (@($Rows | Where-Object outcome -eq 'CONFIRMED').Count -eq 1) 'Expected one confirmed G05 booking'
    Require (@($Rows | Where-Object { $_.outcome -eq 'REJECTED' -and $_.reason -eq 'SEAT_ALREADY_BOOKED' }).Count -eq 5) 'Expected five durable rejections'
}

function Assert-Sql($Rows, [string[]]$ExpectedNodes) {
    Require (@($Rows).Count -eq 5 -and @($Rows.nodeId | Sort-Object -Unique).Count -eq 5) 'Five separate DB observations required'
    foreach ($row in $Rows) {
        Require ($row.nodeId -in $nodes -and $row.seats -eq 10 -and $row.duplicates -eq 0) 'Missing seats, wrong DB identity or duplicate active tickets'
    }
    foreach ($id in $ExpectedNodes) {
        $row = @($Rows | Where-Object nodeId -eq $id)
        Require ($row.Count -eq 1) "Missing DB $id"
    }
}

function Contract-Tests {
    if (Test-Path -LiteralPath $MavenPath -PathType Leaf) { $maven = [IO.Path]::GetFullPath($MavenPath) }
    else {
        $command = Get-Command $MavenPath -ErrorAction SilentlyContinue
        if (!$command) { Blocked 'Maven missing; set MavenPath and JAVA_HOME.' }
        $maven = $command.Source
    }
    $args = @('-B','-ntp','-pl','integration-tests','-am','-Dtest=ConcurrentBookingRunner,ReadBarrierAndRetryTest',
        '-Dsurefire.failIfNoSpecifiedTests=false','-Dcluster.smoke=false','-Dcluster.g4.leader=false','test')
    $run = Step 'contract-fixtures' $maven $args
    foreach ($type in @('ConcurrentBookingRunner','ReadBarrierAndRetryTest')) {
        [xml]$xml = Get-Content -LiteralPath (Join-Path $repo "integration-tests/target/surefire-reports/TEST-vn.nhom6.railway.tests.$type.xml") -Raw
        Require ([int]$xml.testsuite.tests -gt 0 -and [int]$xml.testsuite.failures -eq 0 -and
            [int]$xml.testsuite.errors -eq 0 -and [int]$xml.testsuite.skipped -eq 0) "Fixture tests incomplete: $type"
    }
    Assert-Six @(Parse-Results $run.Text $true)
    Log 'CONTRACT_FIXTURES PASS; actual loopback fixture tests, not a real Raft cluster.'
    return $run.Text
}

function Fault([string]$Name, [string]$Action, [int]$Count) {
    $args = @('-NoProfile','-ExecutionPolicy','Bypass','-File',(Join-Path $PSScriptRoot 'cluster-fault.ps1'),
        '-Action',$Action,'-NodeCount',"$Count",'-LogPath',(Join-Path $OutputDirectory ($Name + '-fault.log')))
    if ($Mode -eq 'Live') {
        $args += @('-Mode','Live','-Project',$Project,'-ComposeFile',$ComposeFile,'-ServicePrefix',$ServicePrefix,'-WaitSeconds',"$WaitSeconds")
    } else { $args += @('-Mode','Fake','-PollIntervalMs','20') }
    return Step $Name 'powershell.exe' $args
}

function Recovering-Case([scriptblock]$Body, [scriptblock]$Restore) {
    $failures = [Collections.Generic.List[Exception]]::new()
    try { & $Body } catch { $failures.Add($_.Exception) }
    finally { try { & $Restore } catch { $failures.Add($_.Exception) } }
    if ($failures.Count) { throw [AggregateException]::new('Scenario/recovery failed', $failures.ToArray()) }
}

function Read-Bytes($Stream, [int]$Length) {
    $bytes = [byte[]]::new($Length); $offset = 0
    while ($offset -lt $Length) {
        $read = $Stream.Read($bytes, $offset, $Length - $offset)
        if ($read -le 0) { throw 'Incomplete TCP frame' }
        $offset += $read
    }
    return ,$bytes
}

function Rpc([string]$Id, [string]$Type, $Payload) {
    Require ($Mode -eq 'Live') 'Live TCP is forbidden outside Live mode'
    $tcp = [Net.Sockets.TcpClient]::new()
    try {
        $connect = $tcp.BeginConnect('127.0.0.1', $ports[$Id], $null, $null)
        try { if (!$connect.AsyncWaitHandle.WaitOne(2000)) { throw 'TCP connect deadline' }; $tcp.EndConnect($connect) }
        finally { $connect.AsyncWaitHandle.Close() }
        $stream = $tcp.GetStream(); $stream.ReadTimeout = 2500; $stream.WriteTimeout = 2500
        $mid = [Guid]::NewGuid().ToString()
        $request = [ordered]@{ protocolVersion = 1; messageId = $mid; correlationId = $null; type = $Type
            senderId = 'client-g5-acceptance'; sentAt = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ss.fffZ'); payload = $Payload }
        $body = [Text.Encoding]::UTF8.GetBytes(($request | ConvertTo-Json -Compress -Depth 8))
        $prefix = [BitConverter]::GetBytes([int]$body.Length)
        if ([BitConverter]::IsLittleEndian) { [Array]::Reverse($prefix) }
        $stream.Write($prefix,0,4); $stream.Write($body,0,$body.Length)
        $header = Read-Bytes $stream 4
        if ([BitConverter]::IsLittleEndian) { [Array]::Reverse($header) }
        $size = [BitConverter]::ToInt32($header,0)
        Require ($size -ge 1 -and $size -le 1048576) 'Invalid TCP frame size'
        $response = [Text.UTF8Encoding]::new($false,$true).GetString((Read-Bytes $stream $size)) | ConvertFrom-Json
        Require ($response.protocolVersion -eq 1 -and $response.correlationId -eq $mid -and $response.senderId -eq $Id) 'Response correlation/peer mismatch'
        Log "LIVE_RPC node=$Id request=$($request | ConvertTo-Json -Compress -Depth 8) response=$($response | ConvertTo-Json -Compress -Depth 8)"
        return $response
    } finally { $tcp.Dispose() }
}

function Status([string[]]$Ids) {
    foreach ($id in $Ids) {
        $reply = Rpc $id 'NODE_STATUS' @{}
        if ($reply.type -ne 'NODE_STATUS_RESPONSE' -or !$reply.payload.PSObject.Properties['term']) {
            Blocked 'SPEC-03 NODE_STATUS term unavailable; integrate D20 candidate first.'
        }
        $p = $reply.payload
        Require ($p.nodeId -eq $id -and $p.storageHealthy -eq $true -and $p.role -in @('LEADER','FOLLOWER','CANDIDATE')) 'Unhealthy status'
        foreach ($field in @('term','commitIndex','lastApplied')) {
            Require (($p.$field -is [int] -or $p.$field -is [long]) -and $p.$field -ge 0) "Untrusted status $field"
        }
        Require ($p.lastApplied -le $p.commitIndex) 'Applied beyond commit'
        $p
    }
}

function Leader([string[]]$Ids) {
    $watch = [Diagnostics.Stopwatch]::StartNew(); $previous = ''; $stable = 0
    while ($watch.Elapsed.TotalSeconds -lt $WaitSeconds) {
        $snapshot = @(Status $Ids); $leaders = @($snapshot | Where-Object role -eq 'LEADER')
        $terms = @($snapshot.term | Sort-Object -Unique)
        if ($leaders.Count -eq 1 -and $terms.Count -eq 1 -and $terms[0] -gt 0) {
            $key = "$($leaders[0].nodeId)@$($terms[0])"
            if ($key -eq $previous) { $stable++ } else { $stable = 1 }; $previous = $key
            if ($stable -ge 3) { return $leaders[0].nodeId }
        } else { $stable = 0 }
        Start-Sleep -Milliseconds 250
    }
    Blocked 'No stable unique Leader; no booking acceptance can be concluded.'
}

function Collect-Logs([string]$Label) {
    foreach ($n in 1..5) {
        if ($Mode -eq 'Live') {
            $log = Compose "$Label-node-$n" @('logs','--no-color','--no-log-prefix','--timestamps','--tail','500',"$ServicePrefix-$n")
            Require (!$log.Text.Trim().Equals('')) "Empty node-$n log"
        } else { Save "$Label-node-$n.log" "FAKE_LOG_ONLY node=node-$n; collector self-check, not Docker output." }
    }
    Log "LOG_COLLECTOR mode=$Mode label=$Label nodes=5"
}

function Sql-Query([int]$Node, [string[]]$Ids) {
    foreach ($id in $Ids) { Require ($id -match '^[0-9a-f-]{36}$') 'Unsafe request UUID' }
    $filter = if ($Ids.Count) { ($Ids | ForEach-Object { "'$_'::uuid" }) -join ',' } else { 'NULL::uuid' }
    return "BEGIN TRANSACTION READ ONLY; SELECT json_build_object('nodeId','node-$Node','seats',(SELECT count(*) FROM seats WHERE trip_id='SE-001'),'duplicates',(SELECT count(*) FROM (SELECT seat_id FROM tickets WHERE trip_id='SE-001' AND status='ACTIVE' GROUP BY seat_id HAVING count(*)>1) d),'g05',(SELECT count(*) FROM tickets WHERE trip_id='SE-001' AND seat_id='G05' AND status='ACTIVE'),'g06',(SELECT count(*) FROM tickets WHERE trip_id='SE-001' AND seat_id='G06' AND status='ACTIVE'),'g07',(SELECT count(*) FROM tickets WHERE trip_id='SE-001' AND seat_id='G07' AND status='ACTIVE'),'lastApplied',(SELECT last_applied FROM raft_apply_progress WHERE singleton=1),'results',COALESCE((SELECT json_agg(r ORDER BY request_id) FROM (SELECT request_id,state,reason_code,ticket_id,applied_log_index FROM request_results WHERE request_id IN ($filter)) r),'[]'::json)); COMMIT;"
}

function Collect-Sql([string]$Label, [string[]]$Ids, [int]$FakeActive = 0) {
    $rows = @()
    foreach ($n in 1..5) {
        $sql = Sql-Query $n $Ids
        Save "$Label-node-$n.sql" $sql
        if ($Mode -eq 'Live') {
            $run = Compose "$Label-db-$n" @('exec','-T',"$DbPrefix-$n",'psql','-X','-qAt','-v','ON_ERROR_STOP=1','-U',$DbUser,'-d',"$DbNamePrefix$n",'-c',$sql)
            $json = @($run.Text -split "`r?`n" | Where-Object { $_.Trim().StartsWith('{') })
            Require ($json.Count -eq 1) "Invalid SQL collector output for node-$n"
            $rows += ($json[0] | ConvertFrom-Json)
        } else {
            $rows += [PSCustomObject]@{ nodeId = "node-$n"; seats = 10; duplicates = 0; g05 = $FakeActive
                g06 = 0; g07 = 0; lastApplied = 100; results = @(); evidenceMode = 'FAKE_SQL_ONLY' }
        }
    }
    Save "$Label-sql.json" ($rows | ConvertTo-Json -Depth 12)
    Assert-Sql $rows $nodes
    Log "SQL_COLLECTOR mode=$Mode label=$Label DBs=5 (read-only; DryRun rows are FAKE)"
    return $rows
}

function Wait-Results([string]$Label, [string[]]$Ids, [string[]]$ActiveNodes) {
    $watch = [Diagnostics.Stopwatch]::StartNew()
    while ($watch.Elapsed.TotalSeconds -lt $WaitSeconds) {
        $rows = @(Collect-Sql $Label $Ids)
        $selected = @($rows | Where-Object { $_.nodeId -in $ActiveNodes })
        $full = @($selected | Where-Object { @($_.results).Count -eq $Ids.Count }).Count -eq $ActiveNodes.Count
        $signatures = @($selected | ForEach-Object { ConvertTo-Json -InputObject @($_.results) -Compress -Depth 8 } | Sort-Object -Unique)
        if ($full -and $signatures.Count -eq 1) { return $rows }
        Start-Sleep -Milliseconds 400
    }
    Blocked 'Applied request results not converged; UNKNOWN is incomplete, not a rejection.'
}

function Assert-Minority($Before, $After, $SqlRows, [string]$RequestId) {
    Require (@($Before).Count -eq 2 -and @($Before.nodeId | Sort-Object -Unique).Count -eq 2 -and @($After).Count -eq 2) 'Two live status observations required'
    Assert-Sql $SqlRows $nodes
    foreach ($old in $Before) {
        $new = @($After | Where-Object nodeId -eq $old.nodeId)
        Require ($new.Count -eq 1 -and $new[0].commitIndex -eq $old.commitIndex) 'Commit advanced while only 2/5 available'
    }
    foreach ($row in $SqlRows) {
        Require (@($row.results | Where-Object request_id -eq $RequestId).Count -eq 0 -and $row.g07 -eq 0) 'Minority request applied before restoring quorum'
    }
}

function Assert-Applied($Rows, [string[]]$ActiveNodes) {
    $status = @(Status $ActiveNodes)
    foreach ($row in @($Rows | Where-Object { $_.nodeId -in $ActiveNodes })) {
        $local = @($status | Where-Object nodeId -eq $row.nodeId)[0]
        foreach ($result in $row.results) {
            Require ($result.applied_log_index -ge 1 -and $result.applied_log_index -le $row.lastApplied -and
                $result.applied_log_index -le $local.lastApplied -and $result.applied_log_index -le $local.commitIndex) 'Request result lacks committed/applied index evidence'
        }
    }
}

function Live-Cases {
    if (!$Project -or !$CandidateRevision) { Blocked 'Live requires explicit Project and frozen CandidateRevision after D20 PR integration.' }
    $head = (git -C $repo rev-parse HEAD).Trim()
    if ($head -ne $CandidateRevision) { Blocked 'CandidateRevision must match checked-out HEAD.' }
    $null = Contract-Tests
    [xml]$xml = Get-Content -LiteralPath (Join-Path $repo 'integration-tests/target/surefire-reports/TEST-vn.nhom6.railway.tests.ConcurrentBookingRunner.xml') -Raw
    $classpath = ($xml.testsuite.properties.property | Where-Object name -eq 'java.class.path').value
    Require (![string]::IsNullOrWhiteSpace($classpath)) 'Runner test classpath unavailable'
    $java = if ($env:JAVA_HOME) { Join-Path $env:JAVA_HOME 'bin/java.exe' } else { 'java' }
    foreach ($n in 1..5) {
        $binding = Compose "port-$n" @('port',"$ServicePrefix-$n",'9100')
        Require ($binding.Text.Trim() -match ':(\d+)\s*$') 'Missing port mapping'
        $ports["node-$n"] = [int]$Matches[1]
    }
    $null = Compose 'images' @('images')
    $leader = Leader $nodes
    $initial = @(Collect-Sql 'initial' @())
    Require (@($initial | Where-Object { $_.g05 -ne 0 -or $_.g06 -ne 0 -or $_.g07 -ne 0 }).Count -eq 0) 'Prepare fresh G05/G06/G07; script never resets DBs.'
    $burst = Step 'six-client-live' $java @('-cp',$classpath,'vn.nhom6.railway.tests.ConcurrentBookingRunner','--live','127.0.0.1',"$($ports[$leader])",$leader,'3000') @(0,2)
    if ($burst.Code -eq 2) { Blocked 'Six-client run contains UNKNOWN; lookup same requestIds before acceptance.' }
    $six = @(Parse-Results $burst.Text); Assert-Six $six
    $six.requestId | ForEach-Object { $allIds.Add($_) }
    $full = @(Wait-Results 'six-client' $allIds.ToArray() $nodes)
    foreach ($row in $full) {
        Require ($row.g05 -eq 1 -and @($row.results | Where-Object state -eq 'APPLIED_SUCCESS').Count -eq 1 -and
            @($row.results | Where-Object { $_.state -eq 'APPLIED_REJECTED' -and $_.reason_code -eq 'SEAT_ALREADY_BOOKED' }).Count -eq 5) 'Six-client SQL decision mismatch'
    }
    Assert-Applied $full $nodes
    Collect-Logs 'six-client'; Log 'LIVE TC-AC03 selected assertions PASS with SQL and log artifacts'
    Recovering-Case {
        $null = Fault 'three-of-five-stop' 'Stop' 2
        $active = @('node-3','node-4','node-5'); $leader = Leader $active
        $id = [Guid]::NewGuid().ToString(); $allIds.Add($id)
        $reply = Rpc $leader 'BOOK_TICKET' @{ requestId=$id; tripId='SE-001'; seatId='G06'; passengerName='Khach G5 demo' }
        Require ($reply.type -eq 'BOOK_TICKET_RESPONSE' -and $reply.payload.requestId -eq $id -and
            $reply.payload.outcome -eq 'CONFIRMED' -and $reply.payload.appliedLogIndex -ge 1) '3/5 did not confirm a new booking'
        $rows = @(Wait-Results 'three-of-five' $allIds.ToArray() $active)
        foreach ($row in @($rows | Where-Object { $_.nodeId -in $active })) {
            Require ($row.g06 -eq 1 -and @($row.results | Where-Object { $_.request_id -eq $id -and $_.state -eq 'APPLIED_SUCCESS' }).Count -eq 1) '3/5 SQL apply missing'
        }
        Assert-Applied $rows $active
        Collect-Logs 'three-of-five'
        Log 'LIVE TC-AC05B selected assertions PASS: new G06 request committed/applied with 3/5.'
    } { $null = Fault 'three-of-five-restore' 'Start' 2 }
    $null = Wait-Results 'recovered-five' $allIds.ToArray() $nodes
    Recovering-Case {
        $null = Fault 'two-of-five-stop' 'Stop' 3
        $active = @('node-4','node-5'); $before = @(Status $active)
        $candidate = @($before | Where-Object role -eq 'LEADER')
        $target = if ($candidate.Count) { $candidate[0].nodeId } else { $active[0] }
        $id = [Guid]::NewGuid().ToString()
        $reply = Rpc $target 'BOOK_TICKET' @{ requestId=$id; tripId='SE-001'; seatId='G07'; passengerName='Khach G5 minority' }
        Require ($reply.payload.requestId -eq $id -and $reply.payload.outcome -ne 'CONFIRMED') 'Minority confirmed a new request'
        if ($reply.type -eq 'ERROR_RESPONSE') { Require ($reply.payload.errorCode -in @('NO_QUORUM','NOT_LEADER','TIMEOUT_UNKNOWN')) 'Unexpected minority routing/contract error' }
        else { Require ($reply.type -eq 'BOOK_TICKET_RESPONSE' -and $reply.payload.outcome -eq 'UNKNOWN') 'Invalid minority response' }
        foreach ($sample in 1..4) {
            Start-Sleep -Milliseconds 500
            $after = @(Status $active); $rows = @(Collect-Sql "two-of-five-$sample" @($id))
            Assert-Minority $before $after $rows $id
        }
        Collect-Logs 'two-of-five'
        Log 'LIVE TC-AC06 selected assertions PASS only within fault window; pending UNKNOWN may commit after recovery.'
    } { $null = Fault 'two-of-five-restore' 'Start' 3 }
    $null = Leader $nodes
    Collect-Logs 'final'
    Log 'SELECTED_LIVE_CASES PASS (AC03/AC05B/AC06 only); full G5/AC01-AC10 acceptance is separate.'
}

function Dry-Run {
    $null = Contract-Tests
    foreach ($count in @(2,3)) {
        $run = Fault "fake-fault-$count" 'Run' $count
        Require ($run.Text.Contains("reachable=$(5-$count)/5") -and $run.Text.Contains('reachable=5/5')) 'Fake fault/recovery observations missing'
    }
    $rows = @(Collect-Sql 'FAKE-collector-check' @() 1)
    Require (@($rows | Where-Object g05 -eq 1).Count -eq 5) 'Fake SQL collector lost node data'
    Collect-Logs 'FAKE-collector-check'
    Log 'DRY_RUN PASS: actual contract fixtures + fake fault runner + FAKE SQL/log collectors. G5_REAL=NOT_RUN; do not copy these artifacts as real SQL/log evidence.'
}

function Self-Test {
    $passed = 0
    $six = @(1..6 | ForEach-Object { [PSCustomObject]@{ requestId=[Guid]::NewGuid().ToString(); outcome=$(if ($_ -eq 1) {'CONFIRMED'} else {'REJECTED'}); reason='SEAT_ALREADY_BOOKED' } })
    Assert-Six $six; $passed++; Log 'SELFTEST PASS six-client normal shape'
    $six[1].outcome='CONFIRMED'; $failed=$false
    try { Assert-Six $six } catch { $failed=$true }; Require $failed 'Multiple confirmations accepted'; $passed++
    $rows = @(1..5 | ForEach-Object { [PSCustomObject]@{ nodeId="node-$_"; seats=10; duplicates=0; g07=0; results=@() } })
    Assert-Sql $rows $nodes; $passed++
    $failed=$false; try { Assert-Sql @($rows | Select-Object -First 4) $nodes } catch { $failed=$true }; Require $failed 'Four DBs accepted'; $passed++
    $rows[0].duplicates=1; $failed=$false
    try { Assert-Sql $rows $nodes } catch { $failed=$true }; Require $failed 'Duplicate active ticket accepted'; $rows[0].duplicates=0; $passed++
    $before=@([PSCustomObject]@{ nodeId='node-4'; commitIndex=10 },[PSCustomObject]@{ nodeId='node-5'; commitIndex=10 })
    Assert-Minority $before $before $rows '11111111-1111-4111-8111-111111111111'; $passed++
    $after=@([PSCustomObject]@{ nodeId='node-4'; commitIndex=11 },$before[1]); $failed=$false
    try { Assert-Minority $before $after $rows '11111111-1111-4111-8111-111111111111' } catch { $failed=$true }; Require $failed 'Minority commit advance accepted'; $passed++
    $trace=[Collections.Generic.List[string]]::new(); $failed=$false
    try { Recovering-Case { $trace.Add('fault'); throw 'fake fault failed' } { $trace.Add('restore') } } catch { $failed=$true }
    Require ($failed -and ($trace -join ',') -eq 'fault,restore') 'Fault failure skipped recovery'; $passed++
    $failed=$false; try { Recovering-Case {} { throw 'fake recovery failed' } } catch { $failed=$true }; Require $failed 'Recovery failure reported success'; $passed++
    $sql = Sql-Query 1 @('11111111-1111-4111-8111-111111111111')
    Require ($sql.StartsWith('BEGIN TRANSACTION READ ONLY; SELECT') -and $sql -notmatch '\b(DELETE|UPDATE|INSERT|TRUNCATE|DROP)\b') 'SQL collector may mutate DB'; $passed++
    $failed=$false; try { Blocked 'fake incomplete gate' } catch [NotSupportedException] { $failed=$true }; Require $failed 'Blocked gate lost its exception type'; $passed++
    Log "SELFTEST PASS count=$passed; injected fixtures only; G5_REAL=NOT_RUN"
}

$exitCode = 0
Push-Location $repo
try {
    New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
    Log "TV6-D20 mode=$Mode revision=$((git rev-parse HEAD).Trim()); G5_REAL=NOT_RUN until explicit post-D20 Live gate."
    Log "ScriptSHA256=$((Get-FileHash -LiteralPath $PSCommandPath -Algorithm SHA256).Hash)"
    if ($Mode -eq 'SelfTest') { Self-Test }
    elseif ($Mode -eq 'DryRun') { Dry-Run }
    else { Live-Cases }
} catch [NotSupportedException] {
    $exitCode = 2; Log "BLOCKED: $($_.Exception.Message)"
} catch {
    $exitCode = 1; Log "FAIL: $($_.Exception.ToString())"
} finally {
    Log "FINAL mode=$Mode exit=$exitCode; DryRun/SelfTest PASS never means G5 real PASS."
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $ResultLog) | Out-Null
    [IO.File]::WriteAllLines($ResultLog, $report, $utf8)
    Pop-Location
    Write-Host "Evidence=$ResultLog"
}
exit $exitCode
