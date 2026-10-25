<#
.SYNOPSIS
  Phase 2 demo: one tracker, one seeder and N downloaders as separate processes
  on this machine; verifies every downloaded file byte-for-byte via SHA-256.

.EXAMPLE
  ./scripts/demo-basic.ps1 -Size 256MiB -Downloaders 3
#>
param(
    [string]$Size = "64MiB",
    [int]$Downloaders = 3,
    [string]$PieceSize = "256KiB",
    [int]$TrackerPort = 7000,
    [int]$SeedPort = 7001,
    [int]$TimeoutSec = 120
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$bin = Join-Path $root "bin"
$work = Join-Path $root "data/demo-basic"

Write-Host "building binaries -> $bin"
New-Item -ItemType Directory -Force $bin | Out-Null
Push-Location $root
& go build -o $bin ./cmd/...
Pop-Location
if ($LASTEXITCODE -ne 0) { throw "build failed" }
$sf = Join-Path $bin "swarmflow.exe"
$tr = Join-Path $bin "tracker.exe"

if (Test-Path $work) { Remove-Item -Recurse -Force $work }
New-Item -ItemType Directory -Force $work | Out-Null
$src = Join-Path $work "payload.bin"
& $sf gendata -out $src -size $Size -seed 7
& $sf make-manifest -in $src -out "$src.swarm" -piece-size $PieceSize
if ($LASTEXITCODE -ne 0) { throw "manifest failed" }

$procs = @()
function Start-Peer($name, $argList) {
    $log = Join-Path $work "$name.log"
    $p = Start-Process -FilePath $argList[0] -ArgumentList $argList[1..($argList.Length-1)] `
        -RedirectStandardError $log -RedirectStandardOutput "$log.out" -NoNewWindow -PassThru
    $null = $p.Handle # cache the handle, or ExitCode is unavailable after exit
    Write-Host ("started {0,-8} pid {1,-6} log {2}" -f $name, $p.Id, $log)
    return $p
}

try {
    $trackerURL = "http://127.0.0.1:$TrackerPort"
    $procs += Start-Peer "tracker" @($tr, "-listen", "127.0.0.1:$TrackerPort")
    $procs += Start-Peer "seeder" @($sf, "seed", "-manifest", "$src.swarm", "-file", $src,
        "-listen", "127.0.0.1:$SeedPort", "-tracker", $trackerURL)
    $start = Get-Date
    $gets = @()
    for ($i = 1; $i -le $Downloaders; $i++) {
        $out = Join-Path $work "d$i/payload.bin"
        $gets += [pscustomobject]@{ Name = "d$i"; Out = $out; Proc = (Start-Peer "d$i" @($sf, "get",
            "-manifest", "$src.swarm", "-out", $out, "-listen", "127.0.0.1:0",
            "-tracker", $trackerURL, "-exit-on-complete")) }
    }
    $procs += $gets.Proc

    foreach ($g in $gets) {
        if (-not $g.Proc.WaitForExit($TimeoutSec * 1000)) { throw "$($g.Name) did not finish in ${TimeoutSec}s" }
    }
    $elapsed = (Get-Date) - $start
    $want = (Get-FileHash $src -Algorithm SHA256).Hash
    $ok = $true
    foreach ($g in $gets) {
        $got = if (Test-Path $g.Out) { (Get-FileHash $g.Out -Algorithm SHA256).Hash } else { "<missing>" }
        $match = $got -eq $want
        $ok = $ok -and $match -and ($g.Proc.ExitCode -eq 0)
        Write-Host ("{0}: exit={1} sha256 {2}" -f $g.Name, $g.Proc.ExitCode, $(if ($match) { "MATCH" } else { "MISMATCH $got" }))
    }
    Write-Host ("all downloaders finished in {0:N2}s (wall clock, including process start-up)" -f $elapsed.TotalSeconds)
    if (-not $ok) { throw "demo FAILED" }
    Write-Host "demo PASSED"
}
finally {
    foreach ($p in $procs) { if ($p -and -not $p.HasExited) { Stop-Process -Id $p.Id -Force } }
}
