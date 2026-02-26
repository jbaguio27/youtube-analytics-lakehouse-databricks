param(
    [Parameter(Mandatory = $true)]
    [string]$WarehouseId,

    [Parameter(Mandatory = $false)]
    [string]$Profile = "sef_databricks",

    [Parameter(Mandatory = $false)]
    [string]$SqlFile = ".\\lakeflow\\bootstrap_unity_catalog.sql",

    [Parameter(Mandatory = $false)]
    [int]$PollSeconds = 3,

    [Parameter(Mandatory = $false)]
    [int]$MaxWaitSecondsPerStatement = 120
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-DatabricksApiJson {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("get", "post")]
        [string]$Method,

        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $false)]
        [string]$JsonBody
    )

    $args = @("api", $Method, $Path, "--profile", $Profile, "-o", "json")
    if ($JsonBody) {
        $args += @("--json", $JsonBody)
    }

    $raw = & databricks @args
    if ($LASTEXITCODE -ne 0) {
        throw "Databricks CLI call failed: databricks $($args -join ' ')"
    }
    return $raw | ConvertFrom-Json
}

if (-not (Test-Path -LiteralPath $SqlFile)) {
    throw "SQL file not found: $SqlFile"
}

if ($WarehouseId -match "/") {
    throw "WarehouseId must be the raw ID only (example: 6bdcbabcbe866b08), not a URL path."
}

$sqlRaw = Get-Content -LiteralPath $SqlFile -Raw
$statements = $sqlRaw -split ";" |
    ForEach-Object { $_.Trim() } |
    Where-Object { $_ -and ($_ -notmatch "^--") }

if (-not $statements -or $statements.Count -eq 0) {
    throw "No SQL statements found in $SqlFile"
}

Write-Host "Executing $($statements.Count) SQL statements from $SqlFile ..."

$index = 0
foreach ($stmt in $statements) {
    $index += 1
    Write-Host "[$index/$($statements.Count)] Submitting statement..."

    $body = @{
        warehouse_id = $WarehouseId
        statement    = $stmt
        wait_timeout = "10s"
    } | ConvertTo-Json -Depth 5 -Compress

    $submit = Invoke-DatabricksApiJson -Method post -Path "/api/2.0/sql/statements" -JsonBody $body
    $statementId = $submit.statement_id
    if (-not $statementId) {
        throw "Missing statement_id in submit response."
    }

    $elapsed = 0
    while ($true) {
        $status = Invoke-DatabricksApiJson -Method get -Path "/api/2.0/sql/statements/$statementId"
        $state = $status.status.state

        if ($state -eq "SUCCEEDED") {
            Write-Host "[$index/$($statements.Count)] SUCCEEDED"
            break
        }

        if ($state -in @("FAILED", "CANCELED", "CLOSED")) {
            $errMsg = $status.status.error.message
            if (-not $errMsg) {
                $errMsg = "No error message returned by API."
            }
            throw "Statement failed (state=$state, id=$statementId): $errMsg"
        }

        Start-Sleep -Seconds $PollSeconds
        $elapsed += $PollSeconds
        if ($elapsed -ge $MaxWaitSecondsPerStatement) {
            throw "Statement timed out after $MaxWaitSecondsPerStatement seconds (id=$statementId)."
        }
    }
}

Write-Host "Unity Catalog bootstrap completed successfully."
