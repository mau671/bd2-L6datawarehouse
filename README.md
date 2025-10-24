# Taller datawarehouse

**Instituto Tecnológico de Costa Rica**  
Campus Tecnológico Central Cartago  
Escuela de Ingeniería en Computación  

**Curso**: IC4302 Bases de datos II  
**Profesor**: Diego Andres Mora Rojas  
**Semestre**: II Semestre, 2025  

## Integrantes

- Mauricio González Prendas
- Susana Feng Liu
- Ximena Molina Portilla
- Aarón Vásquez Báñez

---

## Prerrequisitos

- Docker y Docker Compose instalados.
- `uv` como gestor de entornos y dependencias Python.
- ODBC Driver 18 for SQL Server (ver guía oficial para [Linux](https://learn.microsoft.com/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server) o [Windows](https://learn.microsoft.com/sql/connect/odbc/windows/microsoft-odbc-driver-for-sql-server-on-windows)).

### Instalación de `uv`

Linux/macOS:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Windows (PowerShell):

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Pip/pipx:

```bash
pipx install uv
pip install uv
```

### Preparación del entorno

```bash
uv sync
```

Las variables se leen en el siguiente orden: `.env.local`, `.env` y finalmente el entorno del sistema.

---

## Infraestructura local

### Crear red Docker

```bash
docker network create dw_net || true
```

Agregar archivo DB_SALES.bak en `infra/backups/`.

### Base de datos fuente (`DB_SALES`)

```bash
docker compose --env-file .env.local -f infra/compose/mssql_source.yaml up -d mssql_source
docker logs -f mssql_source
docker compose --env-file .env.local -f infra/compose/mssql_source.yaml up --build --force-recreate init_source
```

#### Notas (`DB_SALES`)

- Linux/macOS: comprobar permisos de ejecución (`chmod +x infra/db/init_source/run-init.sh`) y de lectura para `infra/backups/DB_SALES.bak` si se usa el backup (`chmod a+r infra/backups/DB_SALES.bak`).
- Windows (PowerShell):

  ```powershell
  docker network create dw_net 2>$null
  docker compose --env-file .env.local -f infra/compose/mssql_source.yaml up -d mssql_source
  docker logs -f mssql_source
  docker compose --env-file .env.local -f infra/compose/mssql_source.yaml up --build --force-recreate init_source
  ```

### Data Warehouse (`DW_SALES`)

```bash
docker compose --env-file .env.local -f infra/compose/mssql_dw.yaml up -d mssql_dw
docker logs -f mssql_dw
docker compose --env-file .env.local -f infra/compose/mssql_dw.yaml up --build --force-recreate init_dw
```

#### Notas (`DW_SALES`)

- Linux/macOS: `chmod +x infra/db/init_dw/run-init.sh`.
- Windows (PowerShell):

  ```powershell
  docker network create dw_net 2>$null
  docker compose --env-file .env.local -f infra/compose/mssql_dw.yaml up -d mssql_dw
  docker logs -f mssql_dw
  docker compose --env-file .env.local -f infra/compose/mssql_dw.yaml up --build --force-recreate init_dw
  ```

### Reinicializar desde cero

```bash
docker compose --env-file .env.local -f infra/compose/mssql_source.yaml down --volumes --remove-orphans
docker compose --env-file .env.local -f infra/compose/mssql_dw.yaml down --volumes --remove-orphans
docker volume rm mssql_source_data || true
docker volume rm mssql_dw_data || true
chmod +x infra/db/init_source/run-init.sh
chmod +x infra/db/init_dw/run-init.sh
docker compose --env-file .env.local -f infra/compose/mssql_source.yaml up -d mssql_source
docker compose --env-file .env.local -f infra/compose/mssql_dw.yaml up -d mssql_dw
docker logs -f mssql_source
docker logs -f mssql_dw
docker compose --env-file .env.local -f infra/compose/mssql_source.yaml up --build --force-recreate init_source
docker compose --env-file .env.local -f infra/compose/mssql_dw.yaml up --build --force-recreate init_dw
```

Cuando las rutas o nombres de servicios cambien, ajustar los comandos. En algunos entornos Linux puede requerirse `sudo` delante de `docker`.

### Conexión manual

```bash
sqlcmd -S localhost,14331 -U sa -P "PassSuperSegura!"
sqlcmd -S localhost,14332 -U sa -P "PassSuperSegura!"
```

Si se actualizan las contraseñas, modificar las variables de entorno correspondientes.

---

## Ejecución del ETL

```bash
uv run main.py --reset
```

La opción `--reset` recrea el esquema del DW antes de cargar datos. Existen banderas adicionales (`--skip-sql`, `--skip-json`, `--skip-fx`, `--json-path`, `--fx-path`, `--fx-sheet`) para ejecutar pasos específicos.

---

## Decisiones de diseño ETL

### Reglas generales

- Los scripts emplean `infra/db/init_dw` como definición única del esquema.
- Las claves sustitutas se generan en SQL Server; los procesos solo envían claves de negocio o derivadas (`idDate`).
- `dw.FACT_SALES` registra `source_system` y `source_doc_id` para distinguir lotes de SAP y del JSON.

### `db_mssql.py`

- Consolida facturas (`OINV/INV1`) y notas de crédito (`ORIN/RIN1`) en un único DataFrame, invirtiendo signo para devoluciones.
- Normaliza monedas: `COL` se guarda como `CRC` en `total_crc`; montos USD permanecen en `total_usd` para conversiones posteriores.
- Asigna `idCountry` en `DIM_CUSTOMERS` usando `OCRD.Country` o la zona asociada; el hecho no almacena país.
- Completa `day`, `quarter` y `month_name` para `DIM_TIME` antes de insertar.
- Almacenes sin correspondencia se mapean al SK `0` (`UNK`) para mantener integridad referencial.

### `ETL_Json.py`

- Garantiza la existencia de un cliente sintético (`C_JSON`) y crea productos faltantes con marca `AGG_JSON`.
- Vincula cada mes al primer día correspondiente, creando registros en `DIM_TIME` con atributos derivados y `tc_usd_crc` nulo hasta su actualización.
- Carga montos en USD (`total_usd`) y calcula `total_crc` solo cuando existe tipo de cambio válido.

### `db_excel.py`

- Pueblan `DIM_TIME` con `idDate`, `year`, `month`, `day`, `quarter`, `month_name` y `tc_usd_crc` desde Excel o CSV.
- `convert_currency_fact_sales` recalcula `total_crc` para filas con monto USD cuando la fecha posee tipo de cambio.

---

## Estructura del repositorio

```text
├─ README.md
├─ .env.example
├─ .env.local        # opcional, no versionado
├─ data/
│  └─ raw/
├─ docs/
│  └─ instrucciones/
├─ infra/
│  ├─ compose/
│  └─ db/
├─ src/
│  ├─ db_config.py
│  ├─ db_mssql.py
│  ├─ db_excel.py
│  └─ ETL_Json.py
├─ main.py
├─ pyproject.toml
└─ tests/
```
