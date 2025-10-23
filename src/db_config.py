import os
from pathlib import Path

import pyodbc
from dotenv import load_dotenv


# Cargar variables dando prioridad a .env.local (si existe) sin sobrescribir las del entorno real
_ROOT_DIR = Path(__file__).resolve().parents[1]
_ENV_LOCAL = _ROOT_DIR / ".env.local"
_ENV_DEFAULT = _ROOT_DIR / ".env"

if _ENV_LOCAL.exists():
    load_dotenv(dotenv_path=_ENV_LOCAL, override=False)

if _ENV_DEFAULT.exists():
    load_dotenv(dotenv_path=_ENV_DEFAULT, override=False)


def _env(name: str, default: str) -> str:
    """Recupera una variable de entorno con valor por defecto."""
    return os.getenv(name, default)


ODBC_DRIVER = _env("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

SERVER_ORIGEN = _env("SOURCE_DB_SERVER", "msql.local.maugp.com,14331")
DB_ORIGEN = _env("SOURCE_DB_NAME", "DB_SALES")
USUARIO_ORIGEN = _env("SOURCE_DB_USER", "sa")
PASSWORD_ORIGEN = _env("SOURCE_DB_PASSWORD", "PassSuperSegura!")

SERVER_DW = _env("DW_DB_SERVER", "dwmsql.local.maugp.com,14332")
DB_DW = _env("DW_DB_NAME", "DW_SALES")
USUARIO_DW = _env("DW_DB_USER", "sa")
PASSWORD_DW = _env("DW_DB_PASSWORD", "PassSuperSegura!")


BASE_CONN_STR = (
    f"DRIVER={{{ODBC_DRIVER}}};"
    "Encrypt=yes;TrustServerCertificate=yes;"
)

SOURCE_CONN_STR = (
    f"{BASE_CONN_STR}SERVER={SERVER_ORIGEN};DATABASE={DB_ORIGEN};"
    f"UID={USUARIO_ORIGEN};PWD={PASSWORD_ORIGEN};"
)

DW_CONN_STR = (
    f"{BASE_CONN_STR}SERVER={SERVER_DW};DATABASE={DB_DW};"
    f"UID={USUARIO_DW};PWD={PASSWORD_DW};"
)


def connect_to_db(conn_str: str):
    """Establece y devuelve una conexión de base de datos."""
    return pyodbc.connect(conn_str)