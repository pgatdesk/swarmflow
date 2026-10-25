<#
.SYNOPSIS
  Start (or stop) the local monitoring stack: swarmflow-hub, Prometheus and
  Grafana, all bound to 127.0.0.1.

.DESCRIPTION
  Tool locations default to D:\DevTools (override with -Tools). Runtime data
  goes to <Tools>\monitoring. The tracker URL defaults to http://127.0.0.1:7000.

  Grafana: http://127.0.0.1:3000/d/swarmflow-overview (anonymous viewer; admin/admin)
  Prometheus: http://127.0.0.1:9090   Hub: http://127.0.0.1:7100/api/peers

.EXAMPLE
  ./scripts/monitoring.ps1            # start
  ./scripts/monitoring.ps1 -Stop      # stop everything this script started
#>
param(
    [string]$Tools = "D:\DevTools",
    [string]$Tracker = "http://127.0.0.1:7000",
    [switch]$Stop
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$data = Join-Path $Tools "monitoring"
$pidFile = Join-Path $data "pids.txt"

if ($Stop) {
    if (Test-Path $pidFile) {
        Get-Content $pidFile | ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }
        Remove-Item $pidFile
    }
    Write-Host "monitoring stopped"
    return
}

$prom = Get-ChildItem $Tools -Directory -Filter "prometheus-*" | Select-Object -First 1
$graf = Get-ChildItem $Tools -Directory -Filter "grafana-*" | Select-Object -First 1
if (-not $prom -or -not $graf) { throw "Prometheus or Grafana not found under $Tools (see docs/monitoring.md)" }
New-Item -ItemType Directory -Force (Join-Path $root "bin"), "$data\prometheus", "$data\grafana\data", "$data\grafana\logs" | Out-Null

if (-not (Get-Command go -ErrorAction SilentlyContinue) -and (Test-Path "$Tools\go\bin\go.exe")) {
    $env:Path = "$Tools\go\bin;$env:Path"
}
Push-Location $root
& go build -o bin ./cmd/...
Pop-Location
if ($LASTEXITCODE -ne 0) { throw "build failed" }

$pids = @()
function Start-Logged($name, $exe, $argList, $workDir) {
    $log = Join-Path $data "$name.log"
    $p = Start-Process -FilePath $exe -ArgumentList $argList -WorkingDirectory $workDir -RedirectStandardError $log `
        -RedirectStandardOutput "$log.out" -NoNewWindow -PassThru
    Write-Host ("started {0,-10} pid {1,-6} log {2}" -f $name, $p.Id, $log)
    return $p.Id
}

$pids += Start-Logged "hub" (Join-Path $root "bin\hub.exe") @("-tracker", $Tracker, "-listen", "127.0.0.1:7100") $root
$pids += Start-Logged "prometheus" (Join-Path $prom.FullName "prometheus.exe") @(
    "--config.file=$root\deploy\prometheus\prometheus.yml", "--storage.tsdb.path=$data\prometheus",
    "--web.listen-address=127.0.0.1:9090", "--storage.tsdb.retention.time=2d") $prom.FullName

$env:GF_PATHS_DATA = "$data\grafana\data"
$env:GF_PATHS_LOGS = "$data\grafana\logs"
$env:GF_PATHS_PLUGINS = "$data\grafana\plugins"
$env:GF_PATHS_PROVISIONING = "$root\deploy\grafana\provisioning"
$env:SWARMFLOW_DASHBOARDS = "$root\deploy\grafana\dashboards"
$env:GF_SERVER_HTTP_ADDR = "127.0.0.1"
$env:GF_SERVER_HTTP_PORT = "3000"
$env:GF_AUTH_ANONYMOUS_ENABLED = "true"
$env:GF_AUTH_ANONYMOUS_ORG_ROLE = "Viewer"
$env:GF_ANALYTICS_REPORTING_ENABLED = "false"
$env:GF_ANALYTICS_CHECK_FOR_UPDATES = "false"
$env:GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH = "$root\deploy\grafana\dashboards\swarmflow.json"
$pids += Start-Logged "grafana" (Join-Path $graf.FullName "bin\grafana.exe") @("server", "--homepath", $graf.FullName) $graf.FullName

$pids | Set-Content $pidFile
Write-Host ""
Write-Host "Grafana:    http://127.0.0.1:3000/d/swarmflow-overview"
Write-Host "Prometheus: http://127.0.0.1:9090/targets"
Write-Host "Hub:        http://127.0.0.1:7100/api/peers"
Write-Host "Stop with:  ./scripts/monitoring.ps1 -Stop"
