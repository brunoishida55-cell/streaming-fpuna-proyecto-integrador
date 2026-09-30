$ErrorActionPreference = "Stop"

# Ir automáticamente a la raíz del proyecto
$ProjectRoot = Split-Path $PSScriptRoot -Parent
Set-Location $ProjectRoot

Write-Host ""
Write-Host "============================================"
Write-Host " Preparando Apache Kafka"
Write-Host "============================================"
Write-Host ""

Write-Host "1. Levantando Kafka..."
docker compose up -d

Write-Host ""
Write-Host "2. Esperando a que Kafka esté disponible..."

$maxAttempts = 30
$attempt = 0
$ready = $false

while (-not $ready -and $attempt -lt $maxAttempts) {

    $attempt++

    docker exec kafka `
        /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 `
        --list *> $null

    if ($LASTEXITCODE -eq 0) {
        $ready = $true
    }
    else {
        Write-Host "   Esperando Kafka... intento $attempt/$maxAttempts"
        Start-Sleep -Seconds 2
    }
}

if (-not $ready) {
    Write-Error "Kafka no estuvo disponible dentro del tiempo esperado."
    exit 1
}

Write-Host "   Kafka está listo."

Write-Host ""
Write-Host "3. Creando tópico de entrada..."

docker exec kafka `
    /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --create `
    --if-not-exists `
    --topic sensor.events.v1 `
    --partitions 3 `
    --replication-factor 1

Write-Host ""
Write-Host "4. Creando tópico de salida..."

docker exec kafka `
    /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --create `
    --if-not-exists `
    --topic sensor.aggregates.v1 `
    --partitions 3 `
    --replication-factor 1 `
    --config cleanup.policy=compact

Write-Host ""
Write-Host "5. Verificando configuración compactada del tópico de salida..."

docker exec kafka `
    /opt/kafka/bin/kafka-configs.sh `
    --bootstrap-server localhost:9092 `
    --entity-type topics `
    --entity-name sensor.aggregates.v1 `
    --alter `
    --add-config cleanup.policy=compact

Write-Host ""
Write-Host "6. Tópicos disponibles:"

docker exec kafka `
    /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --list

Write-Host ""
Write-Host "============================================"
Write-Host " Kafka preparado correctamente"
Write-Host "============================================"
Write-Host ""
