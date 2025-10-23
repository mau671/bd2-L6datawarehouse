# etl_json_to_dw.py
import json
import datetime as dt
from decimal import Decimal, ROUND_HALF_UP
import pyodbc
import pandas as pd
from db_config import ( DW_CONN_STR, connect_to_db )

#DW_CONN_STR = "Driver={ODBC Driver 18 for SQL Server};Server=YOUR_SERVER;Database=YOUR_DW;Trusted_Connection=yes;"  # <- ajusta

SOURCE_SYSTEM = "AGG_VENTAS_USD_JSON"
SYNTH_CUSTOMER_CODE = "C_JSON"
SYNTH_CUSTOMER_NAME = "Cliente agregado JSON"
SYNTH_CUSTOMER_ZONA = "Z_JSON"
SYNTH_BRAND = "AGG_JSON"
USD_CODE = "USD"

# ---------- helpers ----------
def as_decimal(x, scale=10):
    if x is None:
        return None
    d = Decimal(str(x))
    return d.quantize(Decimal("1." + "0"*scale), rounding=ROUND_HALF_UP)

def yyyymmdd(d: dt.date) -> int:
    return d.year*10000 + d.month*100 + d.day

def first_day(year, month):
    return dt.date(year, month, 1)

# ---------- conexión ----------
def get_conn():
    return pyodbc.connect(DW_CONN_STR, autocommit=False)

# ---------- lecturas DW a memoria ----------
def fetch_dim_maps(conn):
    maps = {}

    def read_df(sql, key):
        try:
            return pd.read_sql(sql, conn)
        except Exception:
            return pd.DataFrame(columns=[key, "id"])

    dfs = {}
    dfs["currency"] = read_df("SELECT idCurrency AS id, code FROM dw.DIM_CURRENCY", "code")
    dfs["product"]  = read_df("SELECT idProduct AS id, itemCode, name FROM dw.DIM_PRODUCTS", "itemCode")
    dfs["customer"] = read_df("SELECT idCustomer AS id, cardCode, name FROM dw.DIM_CUSTOMERS", "cardCode")
    dfs["time"]     = read_df("SELECT idDate AS id, date, tc_usd_crc FROM dw.DIM_TIME", "date")

    return dfs

# ---------- upserts de dimensiones mínimas ----------
def ensure_currency_usd(conn, df_currency):
    if (df_currency["code"] == USD_CODE).any():
        row = df_currency.loc[df_currency["code"] == USD_CODE].iloc[0]
        return int(row["id"])
    cursor = conn.cursor()
    cursor.execute("INSERT INTO dw.DIM_CURRENCY(code, name) VALUES (?, ?); SELECT SCOPE_IDENTITY();", USD_CODE, "US Dollar")
    id_cur = int(cursor.fetchone()[0])
    df_currency.loc[len(df_currency)] = [id_cur, USD_CODE]
    return id_cur

def ensure_customer_json(conn, df_customer):
    if (df_customer["cardCode"] == SYNTH_CUSTOMER_CODE).any():
        return int(df_customer.loc[df_customer["cardCode"] == SYNTH_CUSTOMER_CODE, "id"].iloc[0])
    cursor = conn.cursor()
    query = """
            INSERT INTO dw.DIM_CUSTOMERS(cardCode, name, zona)
            OUTPUT INSERTED.idCustomer
            VALUES (?, ?, ?);
        """
    cursor.execute(query, SYNTH_CUSTOMER_CODE, SYNTH_CUSTOMER_NAME, SYNTH_CUSTOMER_ZONA)
    new_id = int(cursor.fetchone()[0])
    print(new_id)
    id_cust = int(new_id)
    df_customer.loc[len(df_customer)] = [id_cust, SYNTH_CUSTOMER_CODE, SYNTH_CUSTOMER_NAME]
    return id_cust

def ensure_products(conn, df_products_dim, items_from_json):
    """
    Asegura que cada item del JSON exista en DIM_PRODUCTS.
    Si no existe, lo crea con name=itemCode y brand=AGG_JSON.
    Devuelve dict itemCode -> idProduct
    """
    cursor = conn.cursor()
    idmap = {}
    # actuales
    for _, r in df_products_dim.iterrows():
        idmap[str(r["itemCode"]).strip().upper()] = int(r["id"])

    for item in sorted(set(items_from_json)):
        k = str(item).strip().upper()
        if k in idmap:
            continue
        cursor.execute(
            "INSERT INTO dw.DIM_PRODUCTS(itemCode, name, brand) VALUES (?, ?, ?); SELECT SCOPE_IDENTITY();",
            k, k, SYNTH_BRAND
        )
        new_id = int(cursor.fetchone()[0])
        idmap[k] = new_id
        df_products_dim.loc[len(df_products_dim)] = [new_id, k, k]

    return idmap

def ensure_time_rows(conn, df_time_dim, dates_needed):
    """
    Asegura que existan en DIM_TIME todas las fechas 'dates_needed' (date, idDate, year, month).
    No toca tc_usd_crc (se asume que si existe, ya está cargado por otro flujo).
    Devuelve dict date -> (idDate, tc_usd_crc or None)
    """
    cursor = conn.cursor()
    # index rápido de los que ya están
    present = {pd.to_datetime(r["date"]).date(): (int(r["id"]), r.get("tc_usd_crc", None)) for _, r in df_time_dim.iterrows()}
    out = {}

    for d in sorted(dates_needed):
        if d in present:
            out[d] = present[d]
            continue
        idDate = yyyymmdd(d)
        cursor.execute(
            "INSERT INTO dw.DIM_TIME(idDate, date, year, month) VALUES (?, ?, ?, ?);",
            idDate, d, d.year, d.month
        )
        out[d] = (idDate, None)
        # agrega también al df en memoria
        df_time_dim.loc[len(df_time_dim)] = [idDate, pd.Timestamp(d), None]

    return out

# ---------- construcción del hecho desde JSON ----------
def build_fact_rows(json_path, idCustomer_json, idCurrency_usd, time_index, prod_map):
    """
    Devuelve DataFrame con columnas del FACT_SALES (mínimas y compatibles)
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []
    for mblock in data:  # cada mes
        year = int(mblock["anio"])
        month = int(mblock["mes"])
        vdate = first_day(year, month)
        idDate, tc = time_index[vdate]  # tc puede ser None
        for v in mblock["ventas"]:
            item = str(v["item"]).strip().upper()
            qty = float(v["cantidad"])
            price = float(v["precio"])
            total_usd = qty * price
            idProduct = prod_map[item]

            rows.append({
                "idDate": int(idDate),
                "idCustomer": int(idCustomer_json),
                "idProduct": int(idProduct),
                "idSalesperson": None,
                "idWarehouse": 0,         # UNK por compatibilidad
                "idCountry": None,
                "idCurrency": int(idCurrency_usd),
                "quantity": as_decimal(qty),
                "total_usd": as_decimal(total_usd),
                "total_crc": (as_decimal(total_usd * float(tc)) if tc not in (None, "", 0) else None),
                "source_system": SOURCE_SYSTEM,
                "source_doc_id": f"{year:04d}-{month:02d}-{item}",
            })

    # orden de columnas estándar
    cols = ["idDate","idCustomer","idProduct","idSalesperson","idWarehouse","idCountry",
            "idCurrency","quantity","total_usd","total_crc","source_system","source_doc_id"]
    df = pd.DataFrame(rows, columns=cols)
    return df

# ---------- carga a FACT ----------
def load_fact_sales(conn, df_fact):
    """
    Inserta en dw.FACT_SALES. Valida mínimos y usa executemany.
    """
    req = ["idDate","idCustomer","idProduct","quantity","total_usd","idCurrency"]
    for c in req:
        if df_fact[c].isna().any():
            miss = df_fact[df_fact[c].isna()].head(5)
            raise ValueError(f"Faltan valores requeridos en columna {c}. Ejemplos:\n{miss}")

    # casteos para pyodbc
    tuples = []
    for _, r in df_fact.iterrows():
        tuples.append((
            int(r["idDate"]),
            int(r["idCustomer"]),
            int(r["idProduct"]),
            (None if pd.isna(r["idSalesperson"]) else int(r["idSalesperson"])),
            int(r["idWarehouse"]),
            (None if pd.isna(r["idCountry"]) else int(r["idCountry"])),
            int(r["idCurrency"]),
            r["quantity"], r["total_usd"], r["total_crc"],
            r["source_system"], r["source_doc_id"]
        ))

    sql = """
    INSERT INTO dw.FACT_SALES
    (idDate, idCustomer, idProduct, idSalesperson, idWarehouse, idCountry, idCurrency,
     quantity, total_usd, total_crc, source_system, source_doc_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    cursor = conn.cursor()
    cursor.fast_executemany = True
    cursor.executemany(sql, tuples)

# ---------- main ----------
def run(json_path="../data/raw/ventas_resumen_2024_2025.json"):
    #conn = get_conn()
    conn = connect_to_db(DW_CONN_STR)
    try:
        dims = fetch_dim_maps(conn)

        # asegurar USD y cliente sintético
        idCurrency_usd = ensure_currency_usd(conn, dims["currency"])
        idCustomer_json = ensure_customer_json(conn, dims["customer"])

        # items distintos presentes en el JSON
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        items = [str(v["item"]).strip().upper() for m in data for v in m["ventas"]]

        prod_map = ensure_products(conn, dims["product"], items)

        # fechas requeridas (primer día del mes)
        dates_needed = {first_day(int(m["anio"]), int(m["mes"])) for m in data}
        time_index = ensure_time_rows(conn, dims["time"], dates_needed)

        # construir DF de hechos
        df_fact = build_fact_rows(json_path, idCustomer_json, idCurrency_usd, time_index, prod_map)

        # cargar
        load_fact_sales(conn, df_fact)

        conn.commit()
        print(f"Cargadas {len(df_fact)} filas a dw.FACT_SALES desde {json_path}.")
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    run()
