$ErrorActionPreference = "Stop"

# Ir automaticamente a la raiz del proyecto
$ProjectRoot = Split-Path $PSScriptRoot -Parent
Set-Location $ProjectRoot

Write-Host ""
Write-Host "============================================"
Write-Host " Preparando Apache Kafka"
Write-Host "============================================"
Write-Host ""

Write-Host "1. Levantando Kafka..."
docker compose up -d

# Dar unos segundos al contenedor para iniciar
Start-Sleep -Seconds 3

Write-Host ""
Write-Host "2. Esperando a que Kafka este disponible..."

$maxAttempts = 30
$attempt = 0
$ready = $false

while (-not $ready -and $attempt -lt $maxAttempts) {

    $attempt++

    & docker exec kafka `
        /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 `
        --list 2>$null | Out-Null

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

Write-Host "   Kafka esta listo."

Write-Host ""
Write-Host "3. Creando topico de entrada..."

docker exec kafka `
    /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --create `
    --if-not-exists `
    --topic sensor.events.v1 `
    --partitions 3 `
    --replication-factor 1

Write-Host ""
Write-Host "4. Creando topico de salida..."

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
Write-Host "5. Verificando configuracion compactada del topico de salida..."

docker exec kafka `
    /opt/kafka/bin/kafka-configs.sh `
    --bootstrap-server localhost:9092 `
    --entity-type topics `
    --entity-name sensor.aggregates.v1 `
    --alter `
    --add-config cleanup.policy=compact

Write-Host ""
Write-Host "6. Topicos disponibles:"

docker exec kafka `
    /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --list

Write-Host ""
Write-Host "============================================"
Write-Host " Kafka preparado correctamente"
Write-Host "============================================"
Write-Host ""
