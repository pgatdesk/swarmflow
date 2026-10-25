<#
.SYNOPSIS
  Build the Linux binaries and run every scenario in scenarios/required in the
  WSL2 netem lab (as root), writing <scenario>.json/.md to docs/results/required.

.EXAMPLE
  ./scripts/lab/run-required.ps1 -Trials 5
  ./scripts/lab/run-required.ps1 -Trials 1 -Only 05-crash-restart,10-replication-loss
#>
param(
    [int]$Trials = 5,
    [string[]]$Only = @(),
    [string[]]$Sets = @("required", "extra"),   # subdirectories of scenarios/
    [string]$Distro = "Ubuntu"
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not (Get-Command go -ErrorAction SilentlyContinue) -and (Test-Path "D:\DevTools\go\bin\go.exe")) {
    $env:Path = "D:\DevTools\go\bin;$env:Path"
}
Push-Location $root
try {
    $env:GOOS = "linux"; $env:GOARCH = "amd64"; $env:CGO_ENABLED = "0"
    & go build -o bin/linux/ ./cmd/...
    if ($LASTEXITCODE -ne 0) { throw "build failed" }
} finally {
    Remove-Item Env:GOOS, Env:GOARCH, Env:CGO_ENABLED -ErrorAction SilentlyContinue
    Pop-Location
}
$wslRoot = (wsl -d $Distro -e wslpath -a ($root -replace '\\', '/')).Trim()
New-Item -ItemType Directory -Force (Join-Path $root "data/lab-logs") | Out-Null
foreach ($set in $Sets) {
  foreach ($f in Get-ChildItem (Join-Path $root "scenarios/$set") -Filter *.json | Sort-Object Name) {
    if ($Only.Count -and $Only -notcontains $f.BaseName) { continue }
    Write-Host "=== $set/$($f.BaseName) ($Trials trials per variant)"
    $log = Join-Path $root "data/lab-logs/$($f.BaseName).log"
    # The lab reports progress on stderr; in Windows PowerShell 5.1, redirecting a
    # native command's stderr under ErrorActionPreference=Stop would abort the run.
    $ErrorActionPreference = "Continue"
    wsl -d $Distro -u root --cd $wslRoot -e ./bin/linux/swarmlab netem -scenario "scenarios/$set/$($f.Name)" `
        -trials $Trials -out "docs/results/$set" 2>&1 | ForEach-Object { "$_" } | Out-File -Encoding utf8 $log
    $code = $LASTEXITCODE
    $ErrorActionPreference = "Stop"
    if ($code -ne 0) { Write-Host "  swarmlab exited with code $code (see $log)" }
    Get-Content $log | Select-String -Pattern 'error' | Select-Object -First 3 | ForEach-Object { Write-Host "  $($_.Line)" }
  }
}
