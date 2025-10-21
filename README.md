# Taller datawarehouse

**Instituto Tecnológico de Costa Rica**  
Campus Tecnológico Central Cartago  
Escuela de Ingeniería en Computación  

**Curso**: IC4302 Bases de datos II  
**Profesor**: Diego Andres Mora Rojas  
**Semestre**: II Semestre, 2025  

**Integrantes**:

- Mauricio González Prendas
- Susana Feng Liu
- Ximena Molina Portilla
- Aarón Vásquez Báñez

# Ejecución del proyecto

Se recomienda usar `uv` como gestor de entorno virtual y dependencias, uv es una herramienta moderna para gestionar proyectos y dependencias en Python, escrito en Rust por lo que es muy rápido.

Instalación de uv:

Linux y MacOS:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Windows (PowerShell):

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Usando pip/pipx

```bash
pipx install uv

pip install uv
```

Luego, para crear el entorno virtual y descargar las dependencias, ejecutar:

```bash
uv sync
```

Inicializar DB_SALES:

docker compose -f infra/compose/mssql_source.yaml up -d mssql_source
docker logs -f mssql_source   # espera "ready"
docker compose -f infra/compose/mssql_source.yaml up --build --force-recreate init_source

Inicializar DB_DW:
docker compose -f infra/compose/mssql_dw.yaml up -d mssql_dw
docker logs -f mssql_dw   # espera "ready"
docker compose -f infra/compose/mssql_dw.yaml up --build --force-recreate init_dw


## Reinicializar desde cero (borrar y recrear bases de datos)

Estos pasos eliminan todos los datos y recrean las bases de datos desde cero.

1. Detener y eliminar los contenedores y volúmenes:

```bash
docker compose -f infra/compose/mssql_source.yaml down --volumes --remove-orphans
docker compose -f infra/compose/mssql_dw.yaml down --volumes --remove-orphans
```

2. Eliminar y recrear los directorios de datos:

```bash
rm -rf infra/volumes/mssql_source/data
rm -rf infra/volumes/mssql_dw/data
mkdir -p infra/volumes/mssql_source/data
mkdir -p infra/volumes/mssql_dw/data
```

3. Asignar permisos adecuados a los directorios de datos:

```bash
chmod -R 777 infra/volumes/mssql_source/data
chmod -R 777 infra/volumes/mssql_dw/data
```

4. Asignar permisos de ejecución a los scripts de inicialización:

```bash
chmod +x infra/db/init_source/run-init.sh
chmod +x infra/db/init_dw/run-init.sh
```

5. Levantar los servicios base y esperar a que estén listos:

```bash
docker compose -f infra/compose/mssql_source.yaml up -d mssql_source
docker compose -f infra/compose/mssql_dw.yaml up -d mssql_dw
```

Verificar que ambos servicios estén listos antes de continuar. Se puede usar:

```bash
docker logs -f mssql_source
docker logs -f mssql_dw
```

6. Ejecutar la inicialización de las bases de datos:

```bash
docker compose -f infra/compose/mssql_source.yaml up --build --force-recreate init_source
docker compose -f infra/compose/mssql_dw.yaml up --build --force-recreate init_dw
```

Notas:

- Si cambiaste rutas o nombres de servicios en los archivos de compose, ajusta los comandos según corresponda.
- Puede ser necesario usar `sudo` para los comandos docker en algunos sistemas.

## Credenciales y conexión

Parámetros de conexión para los contenedores de SQL Server definidos en los archivos de compose:

- Servicio origen (DB_SALES / mssql_source):
  - Host (desde la máquina host): localhost
  - Puerto: 14331
  - Usuario: sa
  - Contraseña: PassSuperSegura!

- Servicio destino (DW / mssql_dw):
  - Host (desde la máquina host): localhost
  - Puerto: 14332
  - Usuario: sa
  - Contraseña: PassSuperSegura!

- Conexión por terminal usando sqlcmd (dentro del contenedor o con el cliente instalado en host):

```bash
# Conectar al servicio origen
sqlcmd -S localhost,14331 -U sa -P "PassSuperSegura!"

# Conectar al servicio DW
sqlcmd -S localhost,14332 -U sa -P "PassSuperSegura!"
```

Si las variables de entorno `MSSQL_SOURCE_SA_PASSWORD` o `MSSQL_DW_SA_PASSWORD` cambian en el archivo `.env`, actualizar las contraseñas en el cliente o usar las variables en los comandos de conexión.


## Estructura del proyecto
dw-proyecto/
├─ pyproject.toml
├─ uv.lock
├─ README.md
├─ .gitignore
├─ .env.example                  # DSNs, credenciales, rutas de data
├─ data/
│  ├─ raw/                       # insumos tal cual: .bak, XLSX tipos de cambio, JSON mensual
│  ├─ external/                  # copias originales inmutables
│  ├─ interim/                   # datos normalizados/staging
│  └─ processed/                 # outputs listos para carga o auditoría
├─ docs/
│  ├─ er/                        # diagrama E-R del DW (drawio, png, mermaid)
│  ├─ decisiones/                # ADRs: Star vs Snowflake, moneda, incremental, etc.
│  ├─ runbook.md                 # cómo ejecutar cargas end-to-end
│  └─ entregables.md             # checklist de entregables
├─ infra/
│  ├─ compose/
│  │  ├─ mssql_source.yml        # instancia para DB_SALES (.bak)
│  │  └─ mssql_dw.yml            # instancia del DW
│  ├─ db/
│  │  ├─ init_dw/                # scripts de creación del DW al levantar contenedor
│  │  └─ init_staging/           # tablas staging si aplican
│  └─ odbc/                      # odbcinst.ini, instrucciones Driver 18 SQL Server en Linux
├─ notebooks/
│  ├─ 01_eda_db_sales.ipynb
│  ├─ 02_mapeo_fuentes_a_dw.ipynb
│  └─ 03_validaciones_carga.ipynb
├─ sql/
│  ├─ exploration/               # consultas para entender DB_SALES
│  ├─ staging/
│  │  ├─ stg_oinv.sql            # facturas
│  │  ├─ stg_inv1.sql            # detalle
│  │  ├─ stg_orin.sql            # notas crédito
│  │  └─ stg_rin1.sql
│  └─ dw/                        # DDL del DW (Star o Snowflake)
│     ├─ 00_schema.sql
│     ├─ 10_dim_date.sql
│     ├─ 11_dim_product.sql
│     ├─ 12_dim_customer.sql
│     ├─ 13_dim_brand.sql
│     ├─ 14_dim_country.sql
│     ├─ 15_dim_salesperson.sql
│     ├─ 16_dim_warehouse.sql
│     ├─ 17_dim_currency_or_fx.sql
│     └─ 20_fact_sales.sql
├─ src/
│  └─ dw_etl/
│     ├─ __init__.py
│     ├─ cli.py                  # Typer/Rich: etl full-load, etl incremental, seed, etc.
│     ├─ config/
│     │  └─ settings.py          # Pydantic Settings lee .env
│     ├─ db/
│     │  ├─ connections.py       # SQLAlchemy + pyodbc a source y DW
│     │  └─ models.py            # metadata opcional del DW
│     ├─ extract/
│     │  ├─ db_sales.py          # OINV/INV1, ORIN/RIN1, OITM, OCRD, OSLP, OWHS, etc.
│     │  ├─ exchange_rates_xlsx.py# XLSX 2024–2025
│     │  └─ agg_sales_json.py    # JSON mensual por producto
│     ├─ transform/
│     │  ├─ sales_normalize.py   # join factura-detalle, signo de notas crédito, impuestos
│     │  ├─ currency.py          # CRC↔USD usando tipos de cambio o dim moneda
│     │  └─ dims/
│     │     ├─ dim_date.py
│     │     ├─ dim_product.py
│     │     ├─ dim_customer.py
│     │     ├─ dim_brand.py
│     │     ├─ dim_country.py
│     │     ├─ dim_salesperson.py
│     │     └─ dim_warehouse.py
│     ├─ load/
│     │  ├─ to_dw.py             # upsert/merge dims y fact
│     │  └─ helpers.py
│     └─ pipelines/
│        ├─ full_load.py         # orquesta extract→transform→load de las 3 fuentes
│        └─ incremental.py       # por fecha DocDate y mes del JSON
├─ tests/
│  ├─ test_transform_sales.py
│  └─ test_currency_conversions.py
└─ scripts/
   └─ seed_fake_clients.py       # p.ej. cliente “AGG_JSON” para el archivo mensual
