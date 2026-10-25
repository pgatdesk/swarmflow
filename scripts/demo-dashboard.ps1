<#
.SYNOPSIS
  Live dashboard demo: tracker, monitoring stack, 3 rate-limited seeds, a replica
  holder and 3 downloaders as separate processes. The fastest seed is hard-killed
  after -KillAfter seconds and a replacement seed joins -ReplaceAfter seconds
  later, so Grafana shows joins, transfers, a failure, reassignment and recovery.

.EXAMPLE
  ./scripts/demo-dashboard.ps1                # leaves everything running
  ./scripts/demo-dashboard.ps1 -Stop          # stop the peers and the monitoring stack
#>
param(
    [string]$Size = "128MiB",
    [int]$KillAfter = 20,
    [int]$ReplaceAfter = 15,
    [switch]$Stop
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$work = Join-Path $root "data/demo-dashboard"
$pidFile = Join-Path $work "pids.txt"

if ($Stop) {
    if (Test-Path $pidFile) { Get-Content $pidFile | ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue } }
    & (Join-Path $PSScriptRoot "monitoring.ps1") -Stop
    return
}

& (Join-Path $PSScriptRoot "monitoring.ps1")   # also builds bin/
$sf = Join-Path $root "bin/swarmflow.exe"
if (Test-Path $work) { Remove-Item -Recurse -Force $work }
New-Item -ItemType Directory -Force $work | Out-Null
$src = Join-Path $work "payload.bin"
& $sf gendata -out $src -size $Size -seed 21
& $sf make-manifest -in $src -out "$src.swarm" | Out-Null

$pids = @()
function Start-Peer($name, [string[]]$argList) {
    $log = Join-Path $work "$name.log"
    $p = Start-Process -FilePath $argList[0] -ArgumentList $argList[1..($argList.Length - 1)] `
        -RedirectStandardError $log -RedirectStandardOutput "$log.out" -NoNewWindow -PassThru
    Write-Host ("started {0,-8} pid {1,-6}" -f $name, $p.Id)
    $script:pids += $p.Id
    $script:pids | Set-Content $pidFile
    return $p
}
$tr = "http://127.0.0.1:7000"
$common = @("-tracker", $tr, "-manifest", "$src.swarm", "-progress", "0", "-pex-interval", "10s")
Start-Peer "tracker" @((Join-Path $root "bin/tracker.exe"), "-listen", "127.0.0.1:7000", "-interval", "5s", "-ttl", "15s") | Out-Null
Start-Sleep -Milliseconds 500
$s1 = Start-Peer "s1-fast" (@($sf, "seed", "-file", $src, "-max-upload-rate", "6MiB") + $common)
Start-Peer "s2" (@($sf, "seed", "-file", $src, "-max-upload-rate", "3MiB", "-replicas", "2") + $common) | Out-Null
Start-Peer "s3-slow" (@($sf, "seed", "-file", $src, "-max-upload-rate", "1MiB") + $common) | Out-Null
Start-Peer "replica" (@($sf, "replica", "-out", (Join-Path $work "replica/payload.bin"), "-max-upload-rate", "2MiB") + $common) | Out-Null
foreach ($i in 1..3) {
    Start-Sleep -Seconds 3
    Start-Peer "d$i" (@($sf, "get", "-out", (Join-Path $work "d$i/payload.bin"), "-max-upload-rate", "2MiB") + $common) | Out-Null
}
Write-Host ""
Write-Host "Open http://127.0.0.1:3000/d/swarmflow-overview  (killing s1-fast in $KillAfter s)"
Start-Sleep -Seconds $KillAfter
Stop-Process -Id $s1.Id -Force   # TerminateProcess: no BYE, sockets reset
Write-Host "killed s1-fast (pid $($s1.Id)); replacement seed in $ReplaceAfter s"
Start-Sleep -Seconds $ReplaceAfter
Start-Peer "s4-new" (@($sf, "seed", "-file", $src, "-max-upload-rate", "6MiB") + $common) | Out-Null
Write-Host ""
Write-Host "Demo running. Stop everything with: ./scripts/demo-dashboard.ps1 -Stop"
