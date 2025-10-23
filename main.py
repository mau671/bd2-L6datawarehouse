"""Punto de entrada para orquestar los procesos ETL del proyecto."""

import argparse
from pathlib import Path

from src.db_config import DW_CONN_STR, connect_to_db
from src.db_create_tables import create_dw_schema
from src.db_excel import etl_dim_time_from_excel, convert_currency_fact_sales
from src.db_mssql import run_etl as run_mssql_etl
from src.ETL_Json import run as run_json_etl

DEFAULT_JSON = Path("data/raw/ventas_resumen_2024_2025.json")
DEFAULT_FX = Path("data/raw/TiposCambio_USD_CRC_2024_2025.xlsx")


def _load_time_dimension(fx_path: Path, sheet_name: str | None):
    if not fx_path.exists():
        print(f"⚠️ Archivo de tipos de cambio no encontrado, se omite: {fx_path}")
        return

    dw_conn = connect_to_db(DW_CONN_STR)
    try:
        etl_dim_time_from_excel(str(fx_path), dw_conn, sheet_name=sheet_name)
    finally:
        dw_conn.close()


def _recalc_fact_totals():
    dw_conn = connect_to_db(DW_CONN_STR)
    try:
        updated = convert_currency_fact_sales(dw_conn)
        if updated:
            print(f"🔁 Conversión de montos completada para {updated} filas en dw.FACT_SALES.")
    finally:
        dw_conn.close()


def run_pipeline(
    reset: bool,
    skip_sql: bool,
    skip_json: bool,
    skip_fx: bool,
    json_path: Path,
    fx_path: Path,
    fx_sheet: str | None,
):
    if reset:
        dw_conn = connect_to_db(DW_CONN_STR)
        try:
            create_dw_schema(dw_conn)
        finally:
            dw_conn.close()

    if not skip_fx:
        _load_time_dimension(fx_path, fx_sheet)

    if not skip_sql:
        run_mssql_etl(recreate_schema=False)

    if not skip_json:
        if not json_path.exists():
            raise FileNotFoundError(f"No se encontró el archivo JSON de ventas agregadas: {json_path}")
        run_json_etl(json_path=str(json_path))

    if not skip_fx:
        _recalc_fact_totals()


def main():
    parser = argparse.ArgumentParser(description="Orquestador de los ETL del DW de ventas")
    parser.add_argument("--reset", action="store_true", help="Recrea el esquema DW antes de ejecutar los ETL")
    parser.add_argument("--skip-sql", action="store_true", help="Omite el ETL desde DB_SALES")
    parser.add_argument("--skip-json", action="store_true", help="Omite el ETL del JSON agregado")
    parser.add_argument("--skip-fx", action="store_true", help="Omite los procesos relacionados al tipo de cambio")
    parser.add_argument("--json-path", default=str(DEFAULT_JSON), help="Ruta del JSON agregado mensual")
    parser.add_argument("--fx-path", default=str(DEFAULT_FX), help="Ruta del archivo de tipos de cambio (CSV/Excel)")
    parser.add_argument(
        "--fx-sheet",
        default="Sheet1",
        help="Nombre de hoja del Excel de tipos de cambio (si aplica)",
    )

    args = parser.parse_args()

    json_path = Path(args.json_path)
    fx_path = Path(args.fx_path)

    run_pipeline(
        reset=args.reset,
        skip_sql=args.skip_sql,
        skip_json=args.skip_json,
        skip_fx=args.skip_fx,
        json_path=json_path,
        fx_path=fx_path,
        fx_sheet=args.fx_sheet,
    )


if __name__ == "__main__":
    main()
