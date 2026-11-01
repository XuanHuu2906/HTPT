param(
    [Parameter(Mandatory = $true)][string]$MavenPath,
    [switch]$UseJdk24,
    [string]$JavaHome
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
Push-Location $repoRoot
$previousJavaHome = $env:JAVA_HOME
try {
    if ($JavaHome) {
        $env:JAVA_HOME = $JavaHome
    }
    $runName = if ($UseJdk24) { 'jdk24-override' } elseif ($JavaHome) { 'jdk25' } else { 'default-release' }
    $evidenceFile = Join-Path $PSScriptRoot "TV5-D01-test-$runName.txt"
    $mavenArgs = @('-B', '-ntp', '-o', '-pl', 'backend-node', '-am',
        '-Dtest=ElectionTimeoutPolicyTest', '-Dsurefire.failIfNoSpecifiedTests=false')
    if ($UseJdk24) {
        $mavenArgs += '-Dmaven.compiler.release=24'
    }
    $mavenArgs += @('clean', 'test')
    "Date: $(Get-Date -Format o)" | Set-Content -Encoding UTF8 $evidenceFile
    "Base commit (working tree contains task changes): $(rtk git rev-parse HEAD)" | Add-Content -Encoding UTF8 $evidenceFile
    "Command: rtk proxy $MavenPath $($mavenArgs -join ' ')" | Add-Content -Encoding UTF8 $evidenceFile
    rtk proxy $MavenPath -version | ForEach-Object {
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
