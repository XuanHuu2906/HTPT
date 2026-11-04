$ErrorActionPreference = 'Continue'
Set-Location (Join-Path $PSScriptRoot '../../..')
$evidence = $PSScriptRoot
& rtk proxy java --version 2>&1 | Out-File (Join-Path $evidence 'environment.log') -Encoding utf8
& rtk proxy mvn -version 2>&1 | Out-File (Join-Path $evidence 'environment.log') -Append -Encoding utf8
& rtk git rev-parse HEAD | Out-File (Join-Path $evidence 'environment.log') -Append -Encoding utf8
& rtk git status --short | Out-File (Join-Path $evidence 'environment.log') -Append -Encoding utf8

$unitArgs = @('-B', '-ntp', '-pl', 'backend-node', '-am', '-Dtest=TimerTest', '-Dsurefire.failIfNoSpecifiedTests=false', 'test')
('Command: rtk proxy mvn ' + ($unitArgs -join ' ')) | Set-Content (Join-Path $evidence 'timer-unit.log') -Encoding utf8
& rtk proxy mvn @unitArgs 2>&1 | ForEach-Object { $_; $_ | Out-File (Join-Path $evidence 'timer-unit.log') -Append -Encoding utf8 }
$testExit = $LASTEXITCODE
Add-Content (Join-Path $evidence 'timer-unit.log') ('Exit code: ' + $testExit) -Encoding utf8
if ($testExit -ne 0) { exit $testExit }

$regressionArgs = @('-B', '-ntp', '-pl', 'backend-node', '-am', 'test')
('Command: rtk proxy mvn ' + ($regressionArgs -join ' ')) | Set-Content (Join-Path $evidence 'timer-regression.log') -Encoding utf8
& rtk proxy mvn @regressionArgs 2>&1 | ForEach-Object { $_; $_ | Out-File (Join-Path $evidence 'timer-regression.log') -Append -Encoding utf8 }
$testExit = $LASTEXITCODE
Add-Content (Join-Path $evidence 'timer-regression.log') ('Exit code: ' + $testExit) -Encoding utf8
exit $testExit
