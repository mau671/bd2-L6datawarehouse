import pyodbc

SERVER_ORIGEN = 'msql.local.maugp.com,14331'
DB_ORIGEN = 'DB_SALES'
USUARIO_ORIGEN = 'sa'
PASSWORD_ORIGEN = 'PassSuperSegura!'

SERVER_DW = 'dwmsql.local.maugp.com,14332'
DB_DW = 'DW_SALES'
USUARIO_DW = 'sa'
PASSWORD_DW = 'PassSuperSegura!'

# Cadenas de conexión (Usando ODBC Driver 18 y TrustServerCertificate)
BASE_CONN_STR = (
    'DRIVER={ODBC Driver 18 for SQL Server};'
    'Encrypt=yes;TrustServerCertificate=yes;'
)

SOURCE_CONN_STR = (
    f'{BASE_CONN_STR}SERVER={SERVER_ORIGEN};DATABASE={DB_ORIGEN};'
    f'UID={USUARIO_ORIGEN};PWD={PASSWORD_ORIGEN};'
)

DW_CONN_STR = (
    f'{BASE_CONN_STR}SERVER={SERVER_DW};DATABASE={DB_DW};'
    f'UID={USUARIO_DW};PWD={PASSWORD_DW};'
)

def connect_to_db(conn_str):
    """Establece y devuelve una conexión de base de datos."""
    return pyodbc.connect(conn_str)