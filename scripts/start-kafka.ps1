# ============================================================
# FinStream — Kafka Startup Script (Windows 11 / KRaft mode)
# ============================================================
# Usage: .\scripts\start-kafka.ps1
#
# What this does:
#   1. Sets JAVA_HOME and KAFKA_HOME environment variables
#   2. Sets KAFKA_HEAP_OPTS to bypass the removed 'wmic' command
#      (wmic was removed in Windows 11 22H2; Kafka's .bat scripts
#       still reference it for architecture detection)
#   3. Starts the Kafka broker in KRaft standalone mode
#
# KRaft mode: Kafka 4.x uses KRaft (Kafka Raft) for metadata
# management — no separate ZooKeeper process is needed.
# This node acts as both broker (port 9092) and controller (9093).
#
# Stop Kafka: Close this PowerShell window, or press Ctrl+C

$env:JAVA_HOME       = "C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
$env:KAFKA_HOME      = "C:\tools\kafka\kafka_2.13-4.2.2"
$env:PATH            = "$env:JAVA_HOME\bin;$env:KAFKA_HOME\bin\windows;" + $env:PATH

# Pre-set heap memory to bypass 'wmic' check (removed in Windows 11 22H2)
# -Xmx1G : maximum heap 1 GB  |  -Xms1G : initial heap 1 GB
$env:KAFKA_HEAP_OPTS = "-Xmx1G -Xms1G"

$config = "$env:KAFKA_HOME\config\server-finstream.properties"

Write-Host ""
Write-Host "======================================================"
Write-Host "  FinStream — Starting Kafka 4.2.2 (KRaft mode)"
Write-Host "======================================================"
Write-Host "  Broker  : localhost:9092  (producers/consumers)"
Write-Host "  Controller: localhost:9093 (internal KRaft only)"
Write-Host "  Config  : $config"
Write-Host "  Log dir : C:\tools\kafka-logs"
Write-Host ""
Write-Host "  Stop: Press Ctrl+C in this window"
Write-Host "======================================================"
Write-Host ""

if (-not (Test-Path $config)) {
    Write-Host "ERROR: Config file not found: $config" -ForegroundColor Red
    Write-Host "Run the setup steps in README.md first." -ForegroundColor Red
    exit 1
}

& "$env:KAFKA_HOME\bin\windows\kafka-server-start.bat" $config
