param(
    [string]$MavenPath = 'C:/Users/huung/.m2/wrapper/dists/apache-maven-3.9.16/56ba1f9f/bin/mvn.cmd',
    [string]$JavaHome = 'C:/Users/huung/.vscode/extensions/redhat.java-1.58.0-win32-x64/jre/25.0.4.1-win32-x86_64',
    [switch]$Regression
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
$previousJavaHome = $env:JAVA_HOME
Push-Location $repoRoot
try {
    $env:JAVA_HOME = $JavaHome
    $runName = if ($Regression) { 'raft-regression' } else { 'vote-rules' }
    $testNames = if ($Regression) { 'VoteRulesTest,RaftCoreModelTest,ElectionTimeoutPolicyTest' } else { 'VoteRulesTest' }
    $evidenceFile = Join-Path $PSScriptRoot "TV5-D02-$runName.log"
    $mavenArgs = @('-B', '-ntp', '-o', '-pl', 'backend-node', '-am',
        "-Dtest=$testNames", '-Dsurefire.failIfNoSpecifiedTests=false', 'test')
    @(
        'Task: TV5-D02; owner TV5; core reviewer TV4 (review not claimed).',
        "Date: $(Get-Date -Format o)",
        "Base commit (working tree includes task changes): $(rtk git rev-parse HEAD)",
        'Scope: SPEC-04 section 3; TC-RAFT-02 at unit level; supports FR07/AC07.',
        'NOT_RUN: durable vote/restart, RPC/TCP, live split vote, five-node AC07.',
        'Initial PATH lookup: rtk proxy mvn -version exited 1 (mvn not found). Using existing Maven/JDK paths below.',
        "Command: rtk proxy $MavenPath $($mavenArgs -join ' ')",
        'Source SHA256:'
    ) | Set-Content -Encoding UTF8 $evidenceFile
    Get-FileHash -Algorithm SHA256 -LiteralPath @(
        'backend-node/src/main/java/vn/nhom6/railway/raft/election/VoteRules.java',
        'backend-node/src/test/java/vn/nhom6/railway/raft/election/VoteRulesTest.java',
        'backend-node/src/main/java/vn/nhom6/railway/raft/core/RaftState.java'
    ) | ForEach-Object { "$($_.Hash) $($_.Path)" | Add-Content -Encoding UTF8 $evidenceFile }
    rtk proxy $MavenPath -version 2>&1 | ForEach-Object {
        "$_" | Add-Content -Encoding UTF8 $evidenceFile
        $_
    }
    rtk proxy $MavenPath @mavenArgs 2>&1 | ForEach-Object {
        "$_" | Add-Content -Encoding UTF8 $evidenceFile
        $_
    }
    $testExitCode = $LASTEXITCODE
    "Exit code: $testExitCode" | Add-Content -Encoding UTF8 $evidenceFile
    exit $testExitCode
} finally {
    $env:JAVA_HOME = $previousJavaHome
    Pop-Location
}
