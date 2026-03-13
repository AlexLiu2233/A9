# start_nodes.ps1
# Starts 20 local nodes, each in its own terminal window.
# Usage: .\start_nodes.ps1
#
# The first node is started with --seed flag.

param(
    [string]$NodesFile = "server.list",
    [int]$BasePort     = 13400,
    [int]$NodeCount    = 20
)

$ProjectDir    = Split-Path -Parent $MyInvocation.MyCommand.Path
$JarName       = "A9-1.0-SNAPSHOT-jar-with-dependencies.jar"
$JarPath       = Join-Path $ProjectDir $JarName
$NodesFilePath = Join-Path $ProjectDir $NodesFile

if (-not (Test-Path $JarPath)) {
    Write-Error "JAR not found: $JarPath"
    exit 1
}

# Generate nodes file if it doesn't exist
if (-not (Test-Path $NodesFilePath)) {
    Write-Host "Generating $NodesFile with $NodeCount nodes (ports $BasePort-$($BasePort + $NodeCount - 1))..." -ForegroundColor Yellow
    $lines = 0..($NodeCount - 1) | ForEach-Object { "localhost:$($BasePort + $_)" }
    $lines | Out-File -FilePath $NodesFilePath -Encoding utf8
}

Write-Host "Using JAR: $JarPath" -ForegroundColor Cyan
Write-Host "Nodes file: $NodesFilePath" -ForegroundColor Cyan
Write-Host "Starting $NodeCount nodes..." -ForegroundColor Green

for ($i = 0; $i -lt $NodeCount; $i++) {
    $port = $BasePort + $i
    $seedFlag = if ($i -eq 0) { "--seed" } else { "" }
    $title = "Node $i (port $port)"

    Start-Process -FilePath "cmd.exe" `
        -ArgumentList "/k", "title $title && java -Xmx512m -jar `"$JarPath`" $port `"$NodesFilePath`" $seedFlag"

    Write-Host "  Started node $i on port $port$(if ($i -eq 0) {' [SEED]'})" -ForegroundColor Gray

    # Small delay so the seed node initializes first
    if ($i -eq 0) { Start-Sleep -Seconds 2 }
}

Write-Host "`nAll $NodeCount nodes launched." -ForegroundColor Green
Write-Host "To stop all nodes: Get-Process java | Stop-Process" -ForegroundColor Yellow
