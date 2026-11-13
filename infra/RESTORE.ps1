param([string]$ProjectName = 'speedrun-data')
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$manifest = Get-Content -LiteralPath manifest.json -Raw | ConvertFrom-Json
foreach ($entry in $manifest.files.PSObject.Properties) {
    if ([System.IO.Path]::GetFileName($entry.Name) -ne $entry.Name) { throw 'Invalid manifest path' }
    $item = Get-Item -LiteralPath $entry.Name
    $hash = (Get-FileHash -LiteralPath $item.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($item.Length -ne $entry.Value.bytes -or $hash -ne $entry.Value.sha256) { throw "Checksum failed: $($entry.Name)" }
}
if (-not (Test-Path -LiteralPath .env)) {
    $lines = foreach ($key in 'POSTGRES_PASSWORD','AIRFLOW_DB_PASSWORD','ODS_DB_PASSWORD','CONTROL_DB_PASSWORD') {
        $bytes = New-Object byte[] 24
        $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
        try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
        "$key=$([System.BitConverter]::ToString($bytes).Replace('-','').ToLowerInvariant())"
    }
    [System.IO.File]::WriteAllLines((Join-Path $PSScriptRoot '.env'), [string[]]$lines)
}
docker compose -p $ProjectName -f compose.transfer.yaml up -d --wait postgres
if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL failed to start' }
$container = docker compose -p $ProjectName -f compose.transfer.yaml ps -q postgres
if ($LASTEXITCODE -ne 0 -or -not $container) { throw 'PostgreSQL container missing' }
# Check BOTH databases before importing either. Never overwrite existing data.
foreach ($database in 'speedrun_ods','speedrun_control') {
    $count = docker exec $container psql -XAt -v ON_ERROR_STOP=1 -U postgres -d $database -c "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname!='information_schema' AND c.relkind IN ('r','v','m','S')"
    if ($LASTEXITCODE -ne 0 -or $count -ne '0') { throw "Target database must be empty: $database" }
}
foreach ($database in 'speedrun_ods','speedrun_control') {
    docker cp "$PSScriptRoot/$database.dump" "${container}:/tmp/$database.dump"
    if ($LASTEXITCODE -ne 0) { throw 'Dump copy failed' }
    docker exec $container pg_restore --exit-on-error --no-owner --no-acl --single-transaction -U $database -d $database "/tmp/$database.dump"
    if ($LASTEXITCODE -ne 0) { throw "Restore failed: $database" }
    docker exec $container rm -- "/tmp/$database.dump"
    if ($LASTEXITCODE -ne 0) { throw 'Temporary dump cleanup failed' }
}
foreach ($database in 'speedrun_ods','speedrun_control') {
    foreach ($entry in $manifest.inventory.$database.PSObject.Properties) {
        $parts = $entry.Name.Split('.', 2)
        $schema = $parts[0].Replace('"','""')
        $table = $parts[1].Replace('"','""')
        $count = docker exec $container psql -XAt -v ON_ERROR_STOP=1 -U postgres -d $database -c "SELECT count(*) FROM `"$schema`".`"$table`""
        if ($LASTEXITCODE -ne 0 -or $count -ne [string]$entry.Value.count) { throw "Row count differs: $database.$($entry.Name)" }
    }
}
Write-Host 'Ready. PostgreSQL: localhost:5433, database/user: speedrun_ods.'
Write-Host 'Local passwords are in .env. See TRANSFER_README.md for example queries.'
