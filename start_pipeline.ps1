<#
.SYNOPSIS
Start EventSim, stream Kafka to HDFS, and periodically load DuckDB and build dbt.
.DESCRIPTION
Requires Kafka/HDFS infrastructure from infra/docker-compose.yml to be running.
Uses the Poetry environment when available, otherwise Python from PATH.
Each cycle loads HDFS into DuckDB, then runs dbt build for dbt/duckdb_warehouse.
Press Ctrl+C to stop ingestion and the Spark process tree started here.
EventSim keeps the lifecycle defined by eventsim/eventsim_start.ps1.
.EXAMPLE
.\start_pipeline.ps1
.EXAMPLE
.\start_pipeline.ps1 -IngestionIntervalSeconds 30 -PythonExecutable 'C:\venv\Scripts\python.exe'
#>
[CmdletBinding()]
param(
    [ValidateRange(1, 86400)]
    [int]$IngestionIntervalSeconds = 60,

    [string]$PythonExecutable
)

$ErrorActionPreference = 'Stop'
$streamProcess = $null
$previousPythonUtf8 = $env:PYTHONUTF8

Push-Location -LiteralPath $PSScriptRoot
try {
    if (-not $PythonExecutable) {
        if (Get-Command poetry -ErrorAction SilentlyContinue) {
            $poetryPython = & poetry env info --executable
            if ($LASTEXITCODE -eq 0 -and $poetryPython) {
                $PythonExecutable = ($poetryPython | Select-Object -Last 1).Trim()
            }
        }
        if (-not $PythonExecutable) {
            $PythonExecutable = 'python'
        }
    }
    $pythonCommand = Get-Command $PythonExecutable -CommandType Application -ErrorAction Stop
    $PythonExecutable = $pythonCommand.Source
    $env:PYTHONUTF8 = '1'

    # Validate the interpreter before starting services or writing pipeline data.
    & $PythonExecutable -c 'import duckdb, requests, yaml, pyspark, dbt.cli.main, dbt.adapters.duckdb'
    if ($LASTEXITCODE -ne 0) {
        throw 'Python dependencies are missing. Install the project dependencies or pass -PythonExecutable.'
    }

    Write-Host "Python: $PythonExecutable"
    Write-Host '[1/3] Starting EventSim...'
    & (Join-Path $PSScriptRoot 'eventsim/eventsim_start.ps1')

    $logDirectory = Join-Path $PSScriptRoot 'logs/pipeline'
    New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null
    $runId = Get-Date -Format 'yyyyMMdd_HHmmss_fff'
    $stdoutLog = Join-Path $logDirectory "kafka_to_hdfs_${runId}.out.log"
    $stderrLog = Join-Path $logDirectory "kafka_to_hdfs_${runId}.err.log"

    Write-Host '[2/3] Starting Kafka -> HDFS...'
    $streamProcess = Start-Process -FilePath $PythonExecutable `
        -ArgumentList @('-u', '-m', 'spark.streaming.kafka_to_hdfs') `
        -WorkingDirectory $PSScriptRoot -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput $stdoutLog -RedirectStandardError $stderrLog
    Write-Host "Spark PID: $($streamProcess.Id)"
    Write-Host "Spark stdout: $stdoutLog"
    Write-Host "Spark stderr: $stderrLog"

    Write-Host "[3/3] Running HDFS -> DuckDB -> dbt build every $IngestionIntervalSeconds seconds. Press Ctrl+C to stop."
    while ($true) {
        # Monitor Spark while waiting for the next batch, including the first one.
        $nextBatchAt = [DateTime]::UtcNow.AddSeconds($IngestionIntervalSeconds)
        while ([DateTime]::UtcNow -lt $nextBatchAt) {
            $streamProcess.Refresh()
            if ($streamProcess.HasExited) {
                throw "Kafka -> HDFS exited with code $($streamProcess.ExitCode). See $stderrLog"
            }
            Start-Sleep -Seconds 1
        }

        $eventsimState = & docker inspect eventsim --format '{{.State.Status}} {{.State.ExitCode}}'
        if ($LASTEXITCODE -ne 0 -or $eventsimState -notin @('running 0', 'exited 0')) {
            throw "EventSim is unavailable or failed: $eventsimState. Check docker logs eventsim."
        }

        Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Loading HDFS -> DuckDB..."
        & $PythonExecutable -u -m spark.batch.hdfs_to_duckdb
        if ($LASTEXITCODE -ne 0) {
            throw "HDFS -> DuckDB failed with exit code $LASTEXITCODE."
        }

        Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Running dbt build..."
        & $PythonExecutable -u -m dbt.cli.main build --project-dir (Join-Path $PSScriptRoot 'dbt/duckdb_warehouse')
        if ($LASTEXITCODE -ne 0) {
            throw "dbt build failed with exit code $LASTEXITCODE."
        }
    }
}
finally {
    if ($null -ne $streamProcess) {
        $streamProcess.Refresh()
        if (-not $streamProcess.HasExited) {
            Write-Host 'Stopping the Kafka -> HDFS process tree...'
            # Spark launches a Java child; stop the owned tree as well as Python.
            & taskkill.exe /PID $streamProcess.Id /T /F | Out-Null
        }
        $streamProcess.Dispose()
    }
    $env:PYTHONUTF8 = $previousPythonUtf8
    Pop-Location
}
