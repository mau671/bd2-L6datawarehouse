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