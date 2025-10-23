"""Utilidades para recrear el esquema del Data Warehouse."""

from pathlib import Path


SCRIPT_ORDER = [
    "01_schema_dw.sql",
    "99_seed.sql",
]


def _split_batches(sql_text):
    """Divide un archivo SQL en lotes usando GO como separador."""
    batches = []
    current = []
    for line in sql_text.splitlines():
        if line.strip().upper() == "GO":
            batch = "\n".join(current).strip()
            if batch:
                batches.append(batch)
            current = []
        else:
            current.append(line)
    tail = "\n".join(current).strip()
    if tail:
        batches.append(tail)
    return batches


def _execute_script(dw_conn, script_path):
    cursor = dw_conn.cursor()
    sql_text = script_path.read_text(encoding="utf-8")
    for batch in _split_batches(sql_text):
        cursor.execute(batch)
    dw_conn.commit()


def create_dw_schema(dw_conn):
    """Ejecuta los scripts SQL del directorio infra/db/init_dw en orden."""

    script_dir = Path(__file__).resolve().parents[1] / "infra" / "db" / "init_dw"
    print("Creando o recreando el esquema del Data Warehouse...")

    for script_name in SCRIPT_ORDER:
        path = script_dir / script_name
        if not path.exists():
            raise FileNotFoundError(f"No se encontró el script requerido: {path}")
        try:
            print(f"  -> Ejecutando script {path.name}")
            _execute_script(dw_conn, path)
        except Exception as exc:
            dw_conn.rollback()
            raise RuntimeError(f"Error al ejecutar {path.name}: {exc}") from exc

    print("Esquema DW creado correctamente.")