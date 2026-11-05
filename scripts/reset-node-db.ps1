param(
    [string]$HostName = $env:RAILWAY_DB_HOST,
    [int]$Port = 5432,
    [string[]]$DatabaseNames = @(),
    [string]$Username = $env:RAILWAY_DB_USER,
    [switch]$Execute,
    [switch]$TestSingleDatabase
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$v1Path = Join-Path $repoRoot 'database\migration\V1__schema.sql'
$v2Path = Join-Path $repoRoot 'database\migration\V2__seed.sql'

if (-not $HostName) {
    throw 'Set RAILWAY_DB_HOST or pass -HostName. Only localhost and 127.0.0.1 are allowed.'
}
if ($HostName -notin @('localhost', '127.0.0.1')) {
    throw 'Refusing reset: only localhost or 127.0.0.1 is allowed as database host.'
}
if ($Port -lt 1 -or $Port -gt 65535) {
    throw 'Port must be between 1 and 65535.'
}
if (-not (Test-Path -LiteralPath $v1Path -PathType Leaf) -or
    -not (Test-Path -LiteralPath $v2Path -PathType Leaf)) {
    throw "Required migrations were not found: $v1Path and $v2Path"
}

if ($DatabaseNames.Count -eq 0 -and $env:RAILWAY_DB_NAMES) {
    $DatabaseNames = @($env:RAILWAY_DB_NAMES -split ',' | ForEach-Object { $_.Trim() } |
        Where-Object { $_ -ne '' })
}
$DatabaseNames = @($DatabaseNames | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' })

if ($DatabaseNames.Count -eq 0) {
    throw 'No target databases configured. Pass -DatabaseNames or set RAILWAY_DB_NAMES as a comma-separated list.'
}
if ($DatabaseNames | Where-Object { $_ -notmatch '^[A-Za-z0-9_][A-Za-z0-9_-]*$' }) {
    throw 'Database names may contain only ASCII letters, digits, underscore, and hyphen; the first character must be a letter, digit, or underscore.'
}
if (($DatabaseNames | Select-Object -Unique).Count -ne $DatabaseNames.Count) {
    throw 'Duplicate database names were supplied.'
}
if ($TestSingleDatabase) {
    if ($DatabaseNames.Count -ne 1) {
        throw '-TestSingleDatabase requires exactly one database.'
    }
    Write-Warning 'Single-database reset mode is for isolated tests only; it is not a cluster reset.'
}
elseif ($DatabaseNames.Count -ne 5) {
    throw 'Refusing an incomplete cluster reset: provide exactly five databases, or explicitly pass -TestSingleDatabase for an isolated test database.'
}

Write-Output "Host: $HostName`:$Port"
Write-Output 'Databases that would be reset:'
foreach ($databaseName in $DatabaseNames) {
    Write-Output "  - $databaseName"
}
Write-Output "Migrations: $v1Path -> $v2Path"

if (-not $Execute) {
    Write-Output 'DRY-RUN: no database was changed. Pass -Execute and type the confirmation phrase to proceed.'
    return
}

$psqlCommand = Get-Command psql -ErrorAction SilentlyContinue
if (-not $psqlCommand) {
    throw 'Cannot execute reset: psql was not found on PATH.'
}
if (-not $Username) {
    throw 'Set RAILWAY_DB_USER or pass -Username.'
}
if (-not $env:RAILWAY_DB_PASSWORD) {
    throw 'Set RAILWAY_DB_PASSWORD in the environment. The password is never stored in this script.'
}

$confirmationPhrase = 'RESET-RAILWAY-DBS:' + ($DatabaseNames -join ',')
$typedConfirmation = Read-Host "This destroys the listed database schemas. Type exactly: $confirmationPhrase"
if ($typedConfirmation -cne $confirmationPhrase) {
    throw 'Confirmation did not match; no database was changed.'
}

$oldPgPassword = $env:PGPASSWORD
$env:PGPASSWORD = $env:RAILWAY_DB_PASSWORD
try {
    $commonArguments = @(
        '--no-psqlrc', '--no-password', '--set', 'ON_ERROR_STOP=1',
        '--host', $HostName,
        '--port', "$Port",
        '--username', $Username
    )

    foreach ($databaseName in $DatabaseNames) {
        & $psqlCommand.Source @commonArguments --dbname $databaseName --tuples-only --no-align --command 'SELECT 1'
        if ($LASTEXITCODE -ne 0) {
            throw "Preflight connection failed for database '$databaseName'; no schema has been reset."
        }
    }

    foreach ($databaseName in $DatabaseNames) {
        Write-Output "Resetting $databaseName ..."
        & $psqlCommand.Source @commonArguments `
            --dbname $databaseName `
            --single-transaction `
            --command 'DROP SCHEMA public CASCADE; CREATE SCHEMA public;' `
            --file $v1Path `
            --file $v2Path
        if ($LASTEXITCODE -ne 0) {
            throw "Reset failed for database '$databaseName'. Earlier databases in this run may already have been reset."
        }
        Write-Output "Reset completed for $databaseName."
    }
}
finally {
    if ($null -eq $oldPgPassword) {
        Remove-Item Env:\PGPASSWORD -ErrorAction SilentlyContinue
    }
    else {
        $env:PGPASSWORD = $oldPgPassword
    }
}

Write-Output 'Reset command completed for all listed databases.'
