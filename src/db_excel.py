from pathlib import Path

import pandas as pd
import numpy as np
import pyodbc
from src.db_config import DW_CONN_STR, connect_to_db

def etl_dim_time_from_excel(excel_path, dw_conn, sheet_name=None):
    """
    ETL para cargar la dimensión DIM_TIME desde un archivo Excel.
    - Lee un Excel con una columna de fechas.
    - Genera columnas derivadas (idDate, year, month).
    - Valida datos y elimina duplicados.
    - Inserta en dw.DIM_TIME usando pyodbc.
    """

    print("🚀 Iniciando ETL de DIM_TIME desde Excel...")
    
    # ====================================================
    # 1. EXTRACCIÓN
    # ====================================================
    try:
        path = Path(excel_path)
        if not path.exists():
            raise FileNotFoundError(f"No se encontró el archivo de tipos de cambio: {path}")

        if path.suffix.lower() == '.csv':
            df_time = pd.read_csv(path)
        else:
            read_kwargs = {}
            if sheet_name is not None:
                read_kwargs['sheet_name'] = sheet_name
            df_time = pd.read_excel(path, **read_kwargs)

        print(f"✅ Archivo '{path}' leído correctamente ({len(df_time)} filas).")

        # 🔍 Mostrar columnas detectadas
        print("🔍 Columnas detectadas en el Excel:")
        print(df_time.columns.tolist())

        # 🧩 Normalizar nombres (quita espacios y pasa a minúsculas)
        df_time.columns = (
            df_time.columns.str.strip()
                           .str.lower()
                           .str.replace(" ", "_")
        )

        # 🧩 Renombrar columna de tipo de cambio al formato del DW
        if "tipocambio_usd_crc" in df_time.columns:
            df_time = df_time.rename(columns={"tipocambio_usd_crc": "tc_usd_crc"})
        else:
            print("⚠️ No se encontró la columna 'TipoCambio_USD_CRC' en el Excel; se llenará vacía más adelante.")

    except Exception as e:
        raise RuntimeError(f"❌ Error al leer el Excel: {e}")

    # ====================================================
    # 2. TRANSFORMACIÓN
    # ====================================================
    # Asegurar que exista una columna de fecha
    date_col = None
    for cand in ['date', 'Date', 'fecha', 'Fecha']:
        if cand in df_time.columns:
            date_col = cand
            break
    if not date_col:
        raise ValueError("❌ No se encontró ninguna columna de fecha ('date' o 'fecha') en el Excel.")

    df_time = df_time.rename(columns={date_col: 'date'})
    df_time['date'] = pd.to_datetime(df_time['date'], errors='coerce')

    # Eliminar filas con fecha nula
    df_time = df_time[df_time['date'].notna()].copy()

    # Generar claves derivadas
    df_time['idDate'] = df_time['date'].dt.strftime('%Y%m%d').astype(int)
    df_time['year'] = df_time['date'].dt.year
    df_time['month'] = df_time['date'].dt.month.astype('int8')
    df_time['day'] = df_time['date'].dt.day.astype('int8')
    df_time['quarter'] = df_time['date'].dt.quarter.astype('int8')
    df_time['month_name'] = df_time['date'].dt.strftime('%B')

    # Si no existe tc_usd_crc, agregarla vacía
    if 'tc_usd_crc' not in df_time.columns:
        df_time['tc_usd_crc'] = np.nan

    df_time['tc_usd_crc'] = pd.to_numeric(df_time['tc_usd_crc'], errors='coerce')

    # Eliminar duplicados por idDate
    df_time = df_time.drop_duplicates(subset=['idDate'])

    print(f"📆 Transformación completada: {len(df_time)} fechas únicas encontradas.")

    # ====================================================
    # 3. VALIDACIÓN DE DATOS
    # ====================================================
    # Validar rango de fechas y formato idDate
    min_date, max_date = df_time['date'].min(), df_time['date'].max()
    print(f"📅 Rango de fechas: {min_date.date()} → {max_date.date()}")

    if df_time['idDate'].duplicated().any():
        raise ValueError("⚠️ Se detectaron idDate duplicados. Revisar datos del Excel.")

    # ====================================================
    # 4. CARGA AL DATA WAREHOUSE
    # ====================================================
    cols = ['idDate', 'date', 'year', 'month', 'day', 'quarter', 'month_name', 'tc_usd_crc']
    df_to_load = df_time[cols].copy()

    try:
        cursor = dw_conn.cursor()
        existing_ids_df = pd.read_sql("SELECT idDate FROM dw.DIM_TIME", dw_conn)
        existing_ids = set(existing_ids_df['idDate'].astype(int).tolist()) if not existing_ids_df.empty else set()

        df_to_insert = df_to_load[~df_to_load['idDate'].isin(existing_ids)].copy()
        df_to_update = df_to_load[df_to_load['idDate'].isin(existing_ids)].copy()

        inserted = 0
        updated = 0

        if not df_to_insert.empty:
            insert_sql = (
                "INSERT INTO dw.DIM_TIME (idDate, date, year, month, day, quarter, month_name, tc_usd_crc) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            )
            cursor.fast_executemany = True
            data_to_insert = [tuple(x) for x in df_to_insert.to_numpy()]
            cursor.executemany(insert_sql, data_to_insert)
            inserted = len(data_to_insert)

        if not df_to_update.empty:
            update_sql = (
                "UPDATE dw.DIM_TIME "
                "SET date = ?, year = ?, month = ?, day = ?, quarter = ?, month_name = ?, tc_usd_crc = ? "
                "WHERE idDate = ?"
            )
            cursor.fast_executemany = True
            data_to_update = [
                (
                    row['date'], int(row['year']), int(row['month']), int(row['day']),
                    int(row['quarter']), row['month_name'], row['tc_usd_crc'], int(row['idDate'])
                )
                for _, row in df_to_update.iterrows()
            ]
            cursor.executemany(update_sql, data_to_update)
            updated = len(data_to_update)

        dw_conn.commit()

        print(f"✅ Carga completada: {inserted} registros insertados y {updated} actualizados en dw.DIM_TIME.")

    except pyodbc.Error as e:
        print(f"❌ Error al cargar DIM_TIME: {e}")
        dw_conn.rollback()
    except Exception as e:
        print(f"❌ Error inesperado durante la carga: {e}")
        dw_conn.rollback()

    # ====================================================
    # 5. RETORNO / LOG
    # ====================================================
    return df_to_load

# ============================================================
# Actualización de la columna total_crc en dw.FACT_SALES
# ============================================================
def convert_currency_fact_sales(dw_conn):
    """
    Actualiza la columna total_crc en dw.FACT_SALES
    usando el tipo de cambio tc_usd_crc desde dw.DIM_TIME.
    """
    print("🚀 Iniciando actualización de total_crc en FACT_SALES...")

    try:
        # ====================================================
        # 1️⃣ Extraer datos necesarios
        # ====================================================
        query = """
            SELECT 
                fs.id,
                fs.idDate,
                fs.total_usd,
                fs.total_crc,
                dt.tc_usd_crc
            FROM dw.FACT_SALES fs
            INNER JOIN dw.DIM_TIME dt ON fs.idDate = dt.idDate
            WHERE fs.total_usd IS NOT NULL
              AND (fs.total_crc IS NULL OR fs.total_crc = 0)
              AND dt.tc_usd_crc IS NOT NULL
        """

        df = pd.read_sql(query, dw_conn)
        print(f"✅ {len(df)} registros encontrados para actualizar.")

        if df.empty:
            print("⚠️ No hay registros pendientes de conversión.")
            return 0

        # ====================================================
        # 2️⃣ Calcular conversión
        # ====================================================
        df['total_crc_calc'] = df['total_usd'] * df['tc_usd_crc']

        # ====================================================
        # 3️⃣ Actualizar FACT_SALES
        # ====================================================
        cursor = dw_conn.cursor()

        update_sql = """
            UPDATE dw.FACT_SALES
            SET total_crc = ?
            WHERE id = ?
        """

        data_to_update = [(row.total_crc_calc, int(row.id)) for _, row in df.iterrows()]

        cursor.fast_executemany = True
        cursor.executemany(update_sql, data_to_update)
        dw_conn.commit()

        print(f"✅ Conversión completada: {len(data_to_update)} filas actualizadas en dw.FACT_SALES.")
        return len(data_to_update)

    except Exception as e:
        dw_conn.rollback()
        print(f"❌ Error durante la conversión: {e}")
        raise

# ============================================================
# 🚀 Función principal
# ============================================================
def main():
    dw_conn = None
    
    try:
        # 1. Conectar a las Bases de Datos
        dw_conn = connect_to_db(DW_CONN_STR)
        print("Conexiones a DB establecidas.")

        # 2️⃣ Pedir la ruta al Excel
        excel_path = "../data/raw/TiposCambio_USD_CRC_2024_2025.xlsx"

        # 3️⃣ Pedir el nombre de la hoja (opcional)
        sheet_name = "Sheet1"

        # 4️⃣ Ejecutar el ETL
        print("\n⚙️ Ejecutando proceso ETL para DIM_TIME...")
        df_result = etl_dim_time_from_excel(excel_path, dw_conn, sheet_name)

        # 4️⃣ Ejecutar la actualización
        print("\nSe han actualizado las columnas de total_crc del dw.FACT_SALES")
        convert_currency_fact_sales(dw_conn)

        # 5️⃣ Mostrar los primeros registros cargados
        print("\n✅ ETL completado correctamente. Primeros registros cargados:")
        print(df_result.head())

    except Exception as e:
        print(f"❌ Error durante la ejecución del ETL: {e}")

    finally:
        # 6️⃣ Cerrar conexión
        if dw_conn:
            dw_conn.close()
            print("\n🔒 Conexión cerrada correctamente.")

# ============================================================
# 🧭 Punto de entrada
# ============================================================
if __name__ == "__main__":
    main()

