<#
.SYNOPSIS
    Windows equivalent of the project Makefile.
.EXAMPLE
    .\make.ps1 init
    .\make.ps1 kafka-up
    .\make.ps1 test-unit
#>

param(
    [Parameter(Position=0, Mandatory=$true)]
    [ValidateSet(
        "init","kafka-up","kafka-down",
        "run-producer","run-consumer",
        "test-unit","test-integration",
        "dbt-compile","terraform-plan"
    )]
    [string]$Target
)

$ErrorActionPreference = "Stop"
$Root = $PSScriptRoot

function Import-DotEnv {
    $envFile = "$Root\.env"
    if (Test-Path $envFile) {
        Get-Content $envFile | ForEach-Object {
            if ($_ -match '^\s*([^#\s][^=]*?)\s*=\s*(.*?)\s*$') {
                [System.Environment]::SetEnvironmentVariable($Matches[1], $Matches[2], 'Process')
            }
        }
    }
}

Import-DotEnv

# Prefer Docker Compose V2 (`docker compose`); fall back to the legacy
# standalone `docker-compose` (V1) still shipped on some older installs.
function Get-ComposeCommand {
    $null = & docker compose version 2>$null
    if ($LASTEXITCODE -eq 0) { return @("docker", "compose") }
    return @("docker-compose")
}

function Invoke-Init {
    pip install -r "$Root\requirements.txt"
}

function Invoke-KafkaUp {
    $cmd = Get-ComposeCommand
    & $cmd[0] $cmd[1..($cmd.Count - 1)] -f "$Root\docker-compose.yml" up -d
}

function Invoke-KafkaDown {
    $cmd = Get-ComposeCommand
    & $cmd[0] $cmd[1..($cmd.Count - 1)] -f "$Root\docker-compose.yml" down
}

function Invoke-RunProducer {
    Set-Location $Root
    python -m ingestion.producer
}

function Invoke-RunConsumer {
    Set-Location $Root
    $env:JAVA_TOOL_OPTIONS = "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
    python -m streaming.spark_consumer
}

function Invoke-TestUnit {
    Set-Location $Root
    python -m pytest tests\unit\ -v
}

function Invoke-TestIntegration {
    Set-Location $Root
    python -m pytest tests\integration\ -v
}

function Invoke-DbtCompile {
    Set-Location "$Root\dbt_project"
    dbt compile
}

function Invoke-TerraformPlan {
    Set-Location "$Root\terraform"
    terraform plan
}

switch ($Target) {
    "init"              { Invoke-Init }
    "kafka-up"          { Invoke-KafkaUp }
    "kafka-down"        { Invoke-KafkaDown }
    "run-producer"      { Invoke-RunProducer }
    "run-consumer"      { Invoke-RunConsumer }
    "test-unit"         { Invoke-TestUnit }
    "test-integration"  { Invoke-TestIntegration }
    "dbt-compile"       { Invoke-DbtCompile }
    "terraform-plan"    { Invoke-TerraformPlan }
}
