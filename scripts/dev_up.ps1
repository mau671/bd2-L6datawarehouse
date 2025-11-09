# ================================================================
# Script de desarrollo para Data Warehouse
# Gestión de bases de datos Source y Data Warehouse
# Versión para PowerShell (Windows)
#
# Uso:
#   .\scripts\dev_up.ps1 [comando]
#
# Comandos:
#   Up         Levantar las bases de datos (default)
#   Down       Detener y remover contenedores
#   Init       Ejecutar contenedores de inicialización (scripts)
#   Help       Mostrar esta ayuda
#
# Ejemplos:
#   .\scripts\dev_up.ps1              # Levantar todas las BD
#   .\scripts\dev_up.ps1 Up           # Levantar todas las BD (explícito)
#   .\scripts\dev_up.ps1 Down         # Detener todas las BD
#   .\scripts\dev_up.ps1 Init         # Reinicializar todas las BD
# ================================================================

param(
    [Parameter(Position=0)]
    [string]$Command = "Up"
)

$ErrorActionPreference = "Stop"

# ================================================================
# Configuración
# ================================================================
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$ComposeDir = Join-Path (Join-Path $ProjectRoot "infra") "compose"
$NetworkName = "dw_net"

# Determinar archivo de variables de entorno
$EnvFileLocal = Join-Path $ProjectRoot ".env.local"
$EnvFile = Join-Path $ProjectRoot ".env"

if (Test-Path $EnvFileLocal) {
    $EnvFileToUse = $EnvFileLocal
} elseif (Test-Path $EnvFile) {
    $EnvFileToUse = $EnvFile
} else {
    $EnvFileToUse = $null
}

# ================================================================
# Colores para output
# ================================================================
$Colors = @{
    Info    = "Cyan"
    Success = "Green"
    Warning = "Yellow"
    Error   = "Red"
}

# ================================================================
# Funciones de utilidad
# ================================================================
function Write-Log {
    param(
        [string]$Message,
        [ValidateSet("Info", "Success", "Warning", "Error")]
        [string]$Type = "Info"
    )
    
    $prefix = switch ($Type) {
        "Info"    { "[INFO]" }
        "Success" { "[SUCCESS]" }
        "Warning" { "[WARNING]" }
        "Error"   { "[ERROR]" }
    }
    
    Write-Host "$prefix $Message" -ForegroundColor $Colors[$Type]
}

function Show-Help {
    $helpText = @"
Script de desarrollo para Data Warehouse
Gestión de bases de datos Source y Data Warehouse

Uso:
  .\scripts\dev_up.ps1 [comando]

Comandos:
  Up         Levantar las bases de datos (default)
  Down       Detener y remover contenedores
    Init       Ejecutar contenedores de inicialización (scripts)
  Help       Mostrar esta ayuda

Ejemplos:
  .\scripts\dev_up.ps1              # Levantar todas las BD
  .\scripts\dev_up.ps1 Up           # Levantar todas las BD (explícito)
  .\scripts\dev_up.ps1 Down         # Detener todas las BD
  .\scripts\dev_up.ps1 Init         # Reinicializar todas las BD

Bases de datos:
  - MSSQL Source (puerto 14331)
  - MSSQL Data Warehouse (puerto 14332)
 
Archivos de configuración requeridos:
    - .env.local (variables de entorno)
    - infra\compose\mssql_source.yaml
    - infra\compose\mssql_dw.yaml
"@
        Write-Host $helpText
}

function Test-Dependencies {
    Write-Log "Verificando dependencias..." -Type Info
    
    try {
        $dockerVersion = docker --version 2>$null
        if (-not $dockerVersion) {
            throw "Docker no está instalado"
        }
        Write-Log "Docker encontrado: $dockerVersion" -Type Success
    }
    catch {
        Write-Log "Docker no está instalado o no está en PATH" -Type Error
        exit 1
    }
    
    try {
        $composeVersion = docker compose version 2>$null
        if (-not $composeVersion) {
            throw "Docker Compose no está disponible"
        }
        Write-Log "Docker Compose encontrado" -Type Success
    }
    catch {
        Write-Log "Docker Compose no está disponible" -Type Error
        exit 1
    }
}

function Ensure-Network {
    Write-Log "Verificando red Docker: $NetworkName" -Type Info
    
    $networkExists = docker network inspect $NetworkName 2>$null
    
    if (-not $networkExists) {
        Write-Log "Creando red Docker: $NetworkName" -Type Info
        docker network create $NetworkName
        Write-Log "Red $NetworkName creada" -Type Success
    }
    else {
        Write-Log "Red $NetworkName ya existe" -Type Info
    }
}

function Start-Services {
    Write-Log "Levantando bases de datos (solo motores)..." -Type Info
    
    Ensure-Network
    
    Push-Location $ComposeDir
    
    try {
        if ($EnvFileToUse) {
            Write-Log "Usando archivo de entorno: $EnvFileToUse" -Type Info
            Write-Log "Levantando MSSQL Source (motor)..." -Type Info
            docker compose --env-file "$EnvFileToUse" -f mssql_source.yaml up -d mssql_source
            
            Write-Log "Levantando MSSQL Data Warehouse (motor)..." -Type Info
            docker compose --env-file "$EnvFileToUse" -f mssql_dw.yaml up -d mssql_dw
        } else {
            Write-Log "No se encontró .env.local ni .env, usando variables de entorno del sistema" -Type Warning
            Write-Log "Levantando MSSQL Source (motor)..." -Type Info
            docker compose -f mssql_source.yaml up -d mssql_source
            
            Write-Log "Levantando MSSQL Data Warehouse (motor)..." -Type Info
            docker compose -f mssql_dw.yaml up -d mssql_dw
        }
        
        Write-Log "Motores levantados correctamente" -Type Success
        Write-Log "MSSQL Source: localhost:14331" -Type Info
        Write-Log "MSSQL Data Warehouse: localhost:14332" -Type Info
    }
    finally {
        Pop-Location
    }
}

function Initialize-Services {
    Write-Log "Ejecutando contenedores de inicialización (scripts)..." -Type Info
    
    Ensure-Network
    Push-Location $ComposeDir
    
    try {
        if ($EnvFileToUse) {
            Write-Log "Usando archivo de entorno: $EnvFileToUse" -Type Info
            Write-Log "Asegurando motores arriba..." -Type Info
            docker compose --env-file "$EnvFileToUse" -f mssql_source.yaml up -d mssql_source
            docker compose --env-file "$EnvFileToUse" -f mssql_dw.yaml up -d mssql_dw
            Write-Log "Lanzando init Source..." -Type Info
            docker compose --env-file "$EnvFileToUse" -f mssql_source.yaml up --build init_source
            Write-Log "Lanzando init Data Warehouse..." -Type Info
            docker compose --env-file "$EnvFileToUse" -f mssql_dw.yaml up --build init_dw
        } else {
            Write-Log "Asegurando motores arriba..." -Type Info
            docker compose -f mssql_source.yaml up -d mssql_source
            docker compose -f mssql_dw.yaml up -d mssql_dw
            Write-Log "Lanzando init Source..." -Type Info
            docker compose -f mssql_source.yaml up --build init_source
            Write-Log "Lanzando init Data Warehouse..." -Type Info
            docker compose -f mssql_dw.yaml up --build init_dw
        }
        
        Write-Log "Inicialización completada" -Type Success
    }
    finally {
        Pop-Location
    }
}

# ================================================================
# Función principal
# ================================================================
function Main {
    $normalizedCommand = $Command.ToLower()
    
    switch ($normalizedCommand) {
        "up" {
            Test-Dependencies
            Start-Services
        }
        "down" {
            Test-Dependencies
            Stop-Services
        }
        "init" {
            Test-Dependencies
            Initialize-Services
        }
        { $_ -in @("help", "-h", "--help", "-help") } {
            Show-Help
            exit 0
        }
        default {
            Write-Log "Comando desconocido: $Command" -Type Error
            Show-Help
            exit 1
        }
    }
}

# ================================================================
# Ejecutar función principal
# ================================================================
Main
