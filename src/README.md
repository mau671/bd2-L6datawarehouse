# 🚀 ETL de SQL Server (MSSQL) a Data Warehouse

Este proyecto realiza un proceso **ETL (Extract, Transform, Load)** que extrae datos desde una base de datos **MSSQL**, los transforma con **pandas** y los carga en un **Data Warehouse**.

---

## 🧩 Requisitos previos

- **Python 3.9 o superior**
- **SQL Server ODBC Driver 18**

---

## 🧱 1. Crear y activar entorno virtual

Es recomendable usar un entorno virtual para mantener las dependencias aisladas del sistema.

### 🔹 Windows (PowerShell)
```bash
python -m venv .venv
.\.venv\Scripts\activate
```
### Linux
```bash
python3 -m venv .venv
source .venv/bin/activate
```

## 🧰 2. Verificar o instalar pip

Si al ejecutar pip o python -m pip aparece el error:
```bash
No module named pip
```

Ejecuta el siguiente comando para instalarlo dentro del entorno virtual:
```bash
python -m ensurepip --upgrade
```

Luego confirma que se instaló correctamente:
```bash
python -m pip --version
```
## 📦 3. Instalar dependencias del proyecto

Ejecuta el siguiente comando dentro del entorno virtual:
```bash
python -m pip install pandas numpy pyodbc sqlalchemy
```

### ⚠️ Importante:
Si este paso se ejecuta fuera del entorno virtual, las librerías se instalarán en el Python global, y tu script podría seguir mostrando errores de “módulo no encontrado”.

## 💾 4. Instalar ODBC Driver para SQL Server

pyodbc requiere un driver ODBC compatible con SQL Server.

🪟 Windows

Instala el ODBC Driver 18 for SQL Server:
https://learn.microsoft.com/es-es/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver17

Reinicia tu terminal o VS Code.

🐧 Linux (Debian/Ubuntu)
```bash
sudo apt-get update
sudo apt-get install unixodbc unixodbc-dev
sudo apt-get install msodbcsql18
```

## 🧠 5. Verificar instalación

Prueba que todo esté correctamente instalado:
```bash
python
```

Y dentro del intérprete:
```bash
import pyodbc
import pandas as pd
import numpy as np
import sqlalchemy

print("✅ Todas las librerías están correctamente instaladas")
```

Si no aparece ningún error, el entorno está listo. 🎉