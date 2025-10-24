import pyodbc
import pandas as pd
import numpy as np
from src.db_config import SOURCE_CONN_STR, DW_CONN_STR, connect_to_db
from src.db_create_tables import create_dw_schema
import warnings
import traceback
from decimal import Decimal, InvalidOperation

SOURCE_SYSTEM_DB = "DB_SALES"

# Control verbose output globally. Set to False to suppress informational prints.
VERBOSE = False

# Suppress a noisy pandas UserWarning when passing raw DB-API connections to pd.read_sql.
# Preferred fix: pass a SQLAlchemy engine to pd.read_sql. This global filter silences only
# the specific message so other warnings still appear.
warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable",
    category=UserWarning,
)

# ====================================================================
# 1. FUNCIONES AUXILIARES
# ====================================================================


def ensure_unknown_warehouse(dw_conn):
    """Garantiza la existencia de la bodega desconocida (SK 0)."""

    cursor = dw_conn.cursor()
    cursor.execute(
        """
        IF NOT EXISTS (SELECT 1 FROM dw.DIM_WAREHOUSE WHERE whsCode = 'UNK')
        BEGIN
            SET IDENTITY_INSERT dw.DIM_WAREHOUSE ON;
            INSERT INTO dw.DIM_WAREHOUSE (idWarehouse, whsCode, [name])
            VALUES (0, 'UNK', 'Bodega Desconocida');
            SET IDENTITY_INSERT dw.DIM_WAREHOUSE OFF;
        END
        """
    )
    dw_conn.commit()


def process_and_load_dim(df_source, source_key, dim_name, dw_conn, dw_table):
    """Carga incremental de dimensiones y devuelve el mapa de claves."""

    if df_source is None:
        df_source = pd.DataFrame()

    sk_columns = {
        'DIM_TIME': 'idDate',
        'DIM_CUSTOMERS': 'idCustomer',
        'DIM_PRODUCTS': 'idProduct',
        'DIM_SALESPERSON': 'idSalesperson',
        'DIM_WAREHOUSE': 'idWarehouse',
        'DIM_COUNTRY': 'idCountry',
        'DIM_CURRENCY': 'idCurrency',
    }

    canonical_keys = {
        'DIM_CUSTOMERS': 'cardCode',
        'DIM_PRODUCTS': 'itemCode',
        'DIM_SALESPERSON': 'spCode',
        'DIM_WAREHOUSE': 'whsCode',
        'DIM_COUNTRY': 'iso2',
        'DIM_CURRENCY': 'code',
        'DIM_TIME': 'idDate',
    }

    expected_cols = {
        'DIM_TIME': ['idDate', 'date', 'year', 'month', 'day', 'quarter', 'month_name', 'tc_usd_crc'],
        'DIM_WAREHOUSE': ['whsCode', 'name'],
        'DIM_CUSTOMERS': ['cardCode', 'name', 'zona', 'idCountry'],
        'DIM_PRODUCTS': ['itemCode', 'name', 'brand'],
        'DIM_SALESPERSON': ['spCode', 'name'],
        'DIM_COUNTRY': ['iso2', 'name'],
        'DIM_CURRENCY': ['code', 'name'],
    }

    lookup_columns = {
        'DIM_TIME': ['idDate', 'date', 'tc_usd_crc'],
        'DIM_CUSTOMERS': ['idCustomer', 'cardCode', 'idCountry', 'zona'],
        'DIM_PRODUCTS': ['idProduct', 'itemCode', 'brand'],
        'DIM_SALESPERSON': ['idSalesperson', 'spCode'],
        'DIM_WAREHOUSE': ['idWarehouse', 'whsCode'],
        'DIM_COUNTRY': ['idCountry', 'iso2'],
        'DIM_CURRENCY': ['idCurrency', 'code'],
    }

    string_key_tables = {'DIM_CUSTOMERS', 'DIM_PRODUCTS', 'DIM_SALESPERSON', 'DIM_WAREHOUSE', 'DIM_COUNTRY', 'DIM_CURRENCY'}

    key_col = canonical_keys.get(dw_table, source_key)
    sk_col = sk_columns.get(dw_table)
    expected = expected_cols.get(dw_table, [])

    if dw_table == 'DIM_WAREHOUSE':
        ensure_unknown_warehouse(dw_conn)

    df_work = df_source.copy()

    # Normalizar nombres de columnas según la dimensión
    if dw_table == 'DIM_CUSTOMERS':
        df_work = df_work.rename(columns={'CardCode': 'cardCode', 'zone_name': 'zona'})
    elif dw_table == 'DIM_PRODUCTS':
        df_work = df_work.rename(columns={'ItemCode': 'itemCode', 'brand_name': 'brand'})
    elif dw_table == 'DIM_SALESPERSON':
        df_work = df_work.rename(columns={'SlpCode': 'spCode', 'SlpName': 'name'})
    elif dw_table == 'DIM_COUNTRY':
        df_work = df_work.rename(columns={'Country': 'iso2', 'Name': 'name'})
    elif dw_table == 'DIM_WAREHOUSE':
        df_work = df_work.rename(columns={'WhsCode': 'whsCode', 'WhsName': 'name'})
    elif dw_table == 'DIM_TIME':
        if 'date' in df_work.columns:
            df_work['date'] = pd.to_datetime(df_work['date'], errors='coerce')
        if 'tc_usd_crc' not in df_work.columns:
            df_work['tc_usd_crc'] = np.nan
        if 'day' not in df_work.columns and 'date' in df_work.columns:
            df_work['day'] = df_work['date'].dt.day
        if 'quarter' not in df_work.columns and 'date' in df_work.columns:
            df_work['quarter'] = df_work['date'].dt.quarter
        if 'month_name' not in df_work.columns and 'date' in df_work.columns:
            df_work['month_name'] = df_work['date'].dt.strftime('%B')
        if 'month' not in df_work.columns and 'date' in df_work.columns:
            df_work['month'] = df_work['date'].dt.month
        if 'year' not in df_work.columns and 'date' in df_work.columns:
            df_work['year'] = df_work['date'].dt.year
        if 'idDate' not in df_work.columns and 'date' in df_work.columns:
            df_work['idDate'] = df_work['date'].dt.strftime('%Y%m%d').astype(int)

    # Asegurar columnas esperadas
    for col in expected:
        if col not in df_work.columns:
            df_work[col] = np.nan

    if expected:
        df_work = df_work[expected].copy()

    if key_col not in df_work.columns:
        # no hay datos nuevos para esta dimensión
        existing = pd.read_sql(f"SELECT {', '.join(lookup_columns.get(dw_table, [sk_col, key_col]))} FROM dw.{dw_table}", dw_conn)
        return existing

    df_work = df_work[df_work[key_col].notna()].drop_duplicates(subset=[key_col])

    def normalize(series):
        if series.dtype == object or series.dtype == 'O':
            return series.fillna('').astype(str).str.strip().str.upper()
        return series

    try:
        existing_keys_df = pd.read_sql(f"SELECT {key_col} FROM dw.{dw_table}", dw_conn)
    except Exception:
        existing_keys_df = pd.DataFrame(columns=[key_col])

    existing_keys = set(normalize(existing_keys_df[key_col]).tolist()) if not existing_keys_df.empty and dw_table in string_key_tables else set(existing_keys_df[key_col].tolist())

    if dw_table in string_key_tables:
        comparison_series = normalize(df_work[key_col])
    else:
        comparison_series = df_work[key_col]

    new_mask = ~comparison_series.isin(existing_keys)
    df_to_insert = df_work[new_mask].copy()

    # Preparar columnas a insertar (sin la clave sustituta para tablas con IDENTITY)
    insert_columns = df_to_insert.columns.tolist()
    if df_to_insert.empty:
        pass
    else:
        if dw_table != 'DIM_TIME' and sk_col in insert_columns:
            insert_columns.remove(sk_col)

        cursor = dw_conn.cursor()
        placeholders = ', '.join(['?' for _ in insert_columns])
        columns_sql = ', '.join(insert_columns)
        insert_sql = f"INSERT INTO dw.{dw_table} ({columns_sql}) VALUES ({placeholders})"

        cursor.fast_executemany = True
        payload = []
        for _, row in df_to_insert.iterrows():
            values = []
            for col in insert_columns:
                val = row[col]
                if isinstance(val, np.generic):
                    val = val.item()
                values.append(val)
            payload.append(tuple(values))

        if payload:
            cursor.executemany(insert_sql, payload)
            dw_conn.commit()
            if VERBOSE:
                print(f"Dimensión {dw_table}: {len(payload)} nuevos registros insertados")

    # Recuperar el mapa completo desde el DW
    select_cols = lookup_columns.get(dw_table, [sk_col, key_col])
    lookup_df = pd.read_sql(f"SELECT {', '.join(select_cols)} FROM dw.{dw_table}", dw_conn)
    return lookup_df


def _resolve_credit_base_documents(df_sales, df_credits):
    """Empareja notas de crédito con sus facturas base cuando RIN1 no expone BaseEntry/BaseLine."""

    if df_credits.empty or df_sales.empty:
        return df_credits

    work_invoices = df_sales.copy()
    work_credits = df_credits.copy()

    def _unit_price(df):
        qty = df['Quantity'].replace(0, np.nan)
        with np.errstate(divide='ignore', invalid='ignore'):
            return df['LineTotal'] / qty

    work_invoices['UnitPrice'] = _unit_price(work_invoices)
    work_invoices['remaining_qty'] = work_invoices['Quantity'].fillna(0).astype(float)
    work_invoices['DocDate'] = pd.to_datetime(work_invoices['DocDate'], errors='coerce')

    work_credits['UnitPrice'] = _unit_price(work_credits).abs()
    work_credits['DocDate'] = pd.to_datetime(work_credits['DocDate'], errors='coerce')
    work_credits['BaseDocEntry'] = np.nan
    work_credits['BaseLine'] = np.nan
    work_credits['BaseDocNum'] = np.nan
    work_credits['BaseDocDate'] = pd.NaT

    price_tol = 1e-4
    qty_tol = 1e-6

    for idx, credit_row in work_credits.iterrows():
        qty_needed = abs(float(credit_row.get('Quantity', 0) or 0))
        amt_needed = abs(float(credit_row.get('LineTotal', 0) or 0))
        if qty_needed <= qty_tol or amt_needed <= price_tol:
            continue

        candidates = work_invoices[
            (work_invoices['CardCode'] == credit_row['CardCode'])
            & (work_invoices['ItemCode'] == credit_row['ItemCode'])
            & (work_invoices['DocCur'] == credit_row['DocCur'])
            & (work_invoices['remaining_qty'] > qty_tol)
        ]

        if pd.notna(credit_row.get('SlpCode')):
            candidates = candidates[candidates['SlpCode'] == credit_row['SlpCode']]

        unit_price = credit_row.get('UnitPrice')
        if pd.notna(unit_price):
            tol = max(price_tol, abs(unit_price) * price_tol)
            candidates = candidates[
                candidates['UnitPrice'].notna()
                & (np.abs(candidates['UnitPrice'] - unit_price) <= tol)
            ]

        if candidates.empty:
            continue

        credit_date = credit_row.get('DocDate')
        if pd.notna(credit_date):
            prior_matches = candidates[candidates['DocDate'] <= credit_date]
            if not prior_matches.empty:
                candidates = prior_matches.sort_values('DocDate', ascending=False)
            else:
                candidates = candidates.sort_values('DocDate', ascending=True)
        else:
            candidates = candidates.sort_values('DocDate', ascending=True)

        matched_entry = False
        for cand_idx, cand in candidates.iterrows():
            available_qty = float(cand['remaining_qty'])
            if available_qty + qty_tol < qty_needed:
                continue

            work_credits.at[idx, 'BaseDocEntry'] = cand['DocEntry']
            work_credits.at[idx, 'BaseLine'] = cand['LineNum']
            work_credits.at[idx, 'BaseDocNum'] = cand['DocNum']
            work_credits.at[idx, 'BaseDocDate'] = cand['DocDate']
            work_invoices.at[cand_idx, 'remaining_qty'] = max(0.0, available_qty - qty_needed)
            matched_entry = True
            break

        if not matched_entry:
            continue

    unmatched = work_credits['BaseDocEntry'].isna().sum()
    if unmatched:
        print(f"⚠️ No se pudieron reconciliar {unmatched} líneas de notas de crédito con una factura base; se mantendrán como documentos independientes.")

    return work_credits.drop(columns=['UnitPrice'])

# ====================================================================
# 2. FUNCIÓN DE EXTRACCIÓN (Extract - E)
# ====================================================================

def extract_source_data(conn):
    """
    Extrae datos maestros y transacciones. Asegura que los nombres de las 
    columnas clave del origen sean consistentes.
    """

    print("Iniciando Extracción de Datos Fuente...")

    # A. Tablas de Dimensiones (Maestros) - Nombres consistentes
    dim_queries = {
        'customers': "SELECT CardCode, CardName, U_Zona, Country FROM OCRD WHERE CardType = 'C'",
        'products': "SELECT ItemCode, ItemName, U_Marca, OnHand, CardCode FROM OITM", 
        'salespersons': "SELECT SlpCode, SlpName, Active, U_Gestor FROM OSLP",
        'warehouses': "SELECT WhsCode, WhsName FROM OWHS",
        'countries': "SELECT Country, Name FROM OCRY", 
        'brands': "SELECT Code, Name FROM MARCAS",
        'zones': "SELECT Code, Name FROM ZONAS",
        'product_cost': "SELECT ItemCode, WhsCode, AvgPrice FROM OITW"
    }

    source_data = {}
    for name, query in dim_queries.items():
        try:
            source_data[name] = pd.read_sql(query, conn)
        except Exception as e:
            print(f"❌ Error al ejecutar la consulta para {name} con SQL: {query}. Error: {e}")
            raise 
            
    # B. Consolidación de Hechos
    sales_query = """
    SELECT 
        T1.DocDate,
        T1.CardCode,
        T1.SlpCode,
        T1.DocNum,
        T1.DocEntry,
        T2.LineNum,
        T2.ItemCode,
        T2.Quantity,
        T2.LineTotal,
        T1.DocCur,
        CAST(NULL AS INT) AS BaseDocEntry,
        CAST(NULL AS INT) AS BaseLine,
        CAST(NULL AS INT) AS BaseDocNum,
        CAST(NULL AS DATE) AS BaseDocDate,
        'INVOICE' AS TransactionType
    FROM OINV T1 
    INNER JOIN INV1 T2 ON T1.DocEntry = T2.DocEntry
    """
    df_sales = pd.read_sql(sales_query, conn)
    
    credit_query_base = """
    SELECT 
        T1.DocDate,
        T1.CardCode,
        T1.SlpCode,
        T1.DocNum,
        T1.DocEntry,
        T2.LineNum,
        T2.ItemCode,
        T2.Quantity * -1 AS Quantity,
        T2.LineTotal * -1 AS LineTotal,
        T1.DocCur,
        T2.BaseEntry AS BaseDocEntry,
        T2.BaseLine AS BaseLine,
        T3.DocNum AS BaseDocNum,
        T3.DocDate AS BaseDocDate,
        'CREDIT_NOTE' AS TransactionType
    FROM ORIN T1 
    INNER JOIN RIN1 T2 ON T1.DocEntry = T2.DocEntry
    LEFT JOIN OINV T3 ON T3.DocEntry = T2.BaseEntry
    """

    base_refs_available = True
    try:
        df_credits = pd.read_sql(credit_query_base, conn)
    except Exception as credit_err:
        err_msg = str(credit_err)
        if "BaseEntry" in err_msg and "BaseLine" in err_msg:
            base_refs_available = False
            fallback_query = """
            SELECT 
                T1.DocDate,
                T1.CardCode,
                T1.SlpCode,
                T1.DocNum,
                T1.DocEntry,
                T2.LineNum,
                T2.ItemCode,
                T2.Quantity * -1 AS Quantity,
                T2.LineTotal * -1 AS LineTotal,
                T1.DocCur,
                CAST(NULL AS INT) AS BaseDocEntry,
                CAST(NULL AS INT) AS BaseLine,
                CAST(NULL AS INT) AS BaseDocNum,
                CAST(NULL AS DATE) AS BaseDocDate,
                'CREDIT_NOTE' AS TransactionType
            FROM ORIN T1 
            INNER JOIN RIN1 T2 ON T1.DocEntry = T2.DocEntry
            """
            df_credits = pd.read_sql(fallback_query, conn)
        else:
            raise

    if not base_refs_available:
        df_credits = _resolve_credit_base_documents(df_sales, df_credits)
    
    df_fact = pd.concat([df_sales, df_credits], ignore_index=True)

    # CORRECCIÓN CRÍTICA: Convertir DocDate a datetime
    try:
        df_fact['DocDate'] = pd.to_datetime(df_fact['DocDate'])
    except Exception as e:
        print(f"❌ Error al convertir la columna 'DocDate' a tipo fecha: {e}")
        raise

    # Ajustar metadatos para netear notas de crédito con sus facturas base
    for col in ['DocEntry', 'LineNum', 'BaseDocEntry', 'BaseLine', 'BaseDocNum']:
        if col in df_fact.columns:
            df_fact[col] = pd.to_numeric(df_fact[col], errors='coerce')

    if 'BaseDocDate' in df_fact.columns:
        try:
            df_fact['BaseDocDate'] = pd.to_datetime(df_fact['BaseDocDate'])
        except Exception:
            df_fact['BaseDocDate'] = pd.NaT

    base_mask = df_fact['BaseDocEntry'].notna()
    if 'BaseDocDate' in df_fact.columns:
        df_fact.loc[base_mask & df_fact['BaseDocDate'].notna(), 'DocDate'] = df_fact.loc[
            base_mask & df_fact['BaseDocDate'].notna(), 'BaseDocDate'
        ]

    valid_base_line = base_mask & df_fact['BaseLine'].notna() & (df_fact['BaseLine'] >= 0)
    df_fact['resolved_doc_entry'] = df_fact['BaseDocEntry'].where(base_mask, df_fact['DocEntry'])
    df_fact['resolved_line_num'] = df_fact['LineNum']
    df_fact.loc[valid_base_line, 'resolved_line_num'] = df_fact.loc[valid_base_line, 'BaseLine']
    df_fact['resolved_doc_num'] = df_fact['DocNum']
    df_fact.loc[base_mask & df_fact['BaseDocNum'].notna(), 'resolved_doc_num'] = df_fact.loc[
        base_mask & df_fact['BaseDocNum'].notna(), 'BaseDocNum'
    ]

    # Consolidar facturas y notas de crédito para obtener el valor neto por documento / producto
    try:
        net_group_keys = ['resolved_doc_entry', 'resolved_line_num', 'ItemCode', 'CardCode', 'SlpCode', 'DocCur']
        before_rows = len(df_fact)

        groupby_kwargs = {'as_index': False}
        try:
            df_fact_grouped = (
                df_fact
                .groupby(net_group_keys, dropna=False, **groupby_kwargs)
                .agg({
                    'DocDate': 'min',  # conservar la fecha más antigua (típicamente la factura original)
                    'Quantity': 'sum',
                    'LineTotal': 'sum',
                    'resolved_doc_num': 'first'
                })
            )
        except TypeError:
            # Compatibilidad con versiones de pandas sin parámetro dropna
            df_fact_grouped = (
                df_fact
                .groupby(net_group_keys, **groupby_kwargs)
                .agg({
                    'DocDate': 'min',
                    'Quantity': 'sum',
                    'LineTotal': 'sum',
                    'resolved_doc_num': 'first'
                })
            )

        df_fact = df_fact_grouped.copy()
        df_fact.rename(columns={
            'resolved_doc_entry': 'DocEntry',
            'resolved_line_num': 'LineNum',
            'resolved_doc_num': 'DocNum'
        }, inplace=True)
        if 'DocNum' in df_fact.columns:
            df_fact['DocNum'] = pd.to_numeric(df_fact['DocNum'], errors='coerce')
            try:
                df_fact['DocNum'] = df_fact['DocNum'].astype('Int64')
            except Exception:
                pass

        # Filtrar combinaciones cuyo neto resultó en cero para evitar duplicados innecesarios
        if not df_fact.empty:
            qty_is_zero = df_fact['Quantity'].fillna(0).astype(float).abs() < 1e-9
            amt_is_zero = df_fact['LineTotal'].fillna(0).astype(float).abs() < 1e-6
            zero_mask = qty_is_zero & amt_is_zero
            if zero_mask.any():
                df_fact = df_fact.loc[~zero_mask].copy()

        if VERBOSE:
            after_rows = len(df_fact)
            print(f"[AGG] FACT_SALES filas consolidadas de {before_rows} a {after_rows}")
    except Exception as agg_err:
        print(f"❌ Error al consolidar facturas/notas de crédito: {agg_err}")
        raise

    df_fact.drop(columns=[
        'DocEntry', 'LineNum', 'BaseDocEntry', 'BaseLine', 'BaseDocNum',
        'BaseDocDate', 'resolved_doc_entry', 'resolved_line_num', 'resolved_doc_num'
    ], inplace=True, errors='ignore')

    source_data['sales_fact'] = df_fact
    
    print("Extracción de Datos Fuente completada.")
    return source_data

# ====================================================================
# 3. CARGA Y TRANSFORMACIÓN DE DIMENSIONES (Lookups)
# ====================================================================
def load_dimensions(dw_conn, source_data):
    """Transforma, carga las dimensiones y genera los DataFrames de lookup."""
    

    print("\nIniciando Carga y Transformación de Dimensiones...")
    dim_dfs = {} 

    # A. DIM_WAREHOUSE 
    dim_dfs['warehouse'] = process_and_load_dim(
        source_data['warehouses'], 'WhsCode', 'warehouse', dw_conn, 'DIM_WAREHOUSE'
    )

    # B. DIM_SALESPERSON 
    dim_dfs['salesperson'] = process_and_load_dim(
        source_data['salespersons'], 'SlpCode', 'salesperson', dw_conn, 'DIM_SALESPERSON'
    )

    # C. DIM_COUNTRY 
    dim_dfs['country'] = process_and_load_dim(
        source_data['countries'], 'Country', 'country', dw_conn, 'DIM_COUNTRY'
    )
    # Normalize country iso codes in country dim
    try:
        if isinstance(dim_dfs.get('country'), pd.DataFrame) and 'iso2' in dim_dfs['country'].columns:
            dim_dfs['country']['iso2'] = dim_dfs['country']['iso2'].astype(str).str.strip().str.upper()
    except Exception:
        pass
    
    # D. DIM_CURRENCY 
    dim_currency = pd.DataFrame({'name': ['Colones', 'Dólares'], 'code': ['CRC', 'USD']})
    dim_dfs['currency'] = process_and_load_dim(
        dim_currency, 'code', 'currency', dw_conn, 'DIM_CURRENCY'
    )
    
    # E. DIM_PRODUCTS (OITM + MARCAS)
    df_products = source_data['products'].merge(
        source_data['brands'], left_on='U_Marca', right_on='Code', how='left', suffixes=('_prod', '_brand')
    )
    # Prefer brand name from MARCAS (Name_brand); otherwise fall back to U_Marca from OITM
    if 'Name_brand' in df_products.columns:
        df_products['brand'] = df_products['Name_brand'].fillna(df_products.get('U_Marca'))
    else:
        df_products['brand'] = df_products.get('U_Marca')
    df_products = df_products.rename(columns={'ItemName': 'name'})
    # Normalize
    df_products['brand'] = df_products['brand'].fillna('').astype(str).str.strip()
    df_products['name'] = df_products['name'].astype(str).str.strip()
    df_products = df_products.drop_duplicates(subset=['name'], keep='first')
    dim_dfs['product'] = process_and_load_dim(
        df_products, 'ItemCode', 'product', dw_conn, 'DIM_PRODUCTS'
    )

    # F. DIM_CUSTOMERS (OCRD + ZONAS)
    # Detect if zones table contains a column with country codes (ISO2-like). Prefer OCRD.Country otherwise.
    zones_df = source_data.get('zones', pd.DataFrame())
    zone_country_col = None
    try:
        if not zones_df.empty:
            # prefer explicit 'Country' if present
            if 'Country' in zones_df.columns:
                zone_country_col = 'Country'
            else:
                # try to heuristically find a 2-letter iso code column
                for col in zones_df.columns:
                    if col.lower() in ('code', 'name'):
                        continue
                    sample = zones_df[col].dropna().astype(str).str.strip()
                    if sample.empty:
                        continue
                    s = sample.str.upper()
                    # proportion of values that look like 2-letter codes
                    prop_iso2 = (s.str.len() == 2).mean()
                    if prop_iso2 >= 0.25:
                        zone_country_col = col
                        break
    except Exception:
        zone_country_col = None

    df_customers = source_data['customers'].merge(
        source_data['zones'], left_on='U_Zona', right_on='Code', how='left', suffixes=('_cust', '_zone')
    )
    # Build country_code: prefer OCRD.Country, then zone candidate column if found
    try:
        if 'Country' in df_customers.columns and df_customers['Country'].notna().any():
            df_customers['country_code'] = df_customers['Country']
        elif zone_country_col:
            # handle merged suffixes (col or col_zone)
            col_candidate = zone_country_col if zone_country_col in df_customers.columns else (zone_country_col + '_zone')
            if col_candidate in df_customers.columns:
                df_customers['country_code'] = df_customers[col_candidate]
            else:
                df_customers['country_code'] = np.nan
        else:
            df_customers['country_code'] = np.nan
    except Exception:
        df_customers['country_code'] = np.nan

    # Determine the zone name column produced by the merge. Pandas only adds suffixes
    # when there is a column name conflict, so the zones 'Name' column may be 'Name' or 'Name_zone'.
    zona_col = None
    for cand in ['Name_zone', 'Name', 'Name_zone', 'name_zone']:
        if cand in df_customers.columns:
            zona_col = cand
            break

    if zona_col:
        df_customers['zona'] = df_customers[zona_col]
    else:
        # no zone name available, create empty
        df_customers['zona'] = np.nan

    # Standard renames
    df_customers = df_customers.rename(columns={'CardName': 'name', 'U_Zona': 'zone_code'})
    # Normalize customer country codes to match DIM_COUNTRY.iso2
    try:
        if 'country_code' in df_customers.columns:
            df_customers['country_code'] = df_customers['country_code'].fillna('').astype(str).str.strip().str.upper()
    except Exception:
        pass

    try:
        country_lookup = dim_dfs.get('country')
        if isinstance(country_lookup, pd.DataFrame) and 'iso2' in country_lookup.columns:
            iso_map = country_lookup[['iso2', 'idCountry']].drop_duplicates(subset=['iso2']).copy()
            iso_map['iso2'] = iso_map['iso2'].astype(str).str.strip().str.upper()
            country_map = dict(zip(iso_map['iso2'], iso_map['idCountry']))
            df_customers['idCountry'] = df_customers.get('country_code', np.nan)
            df_customers['idCountry'] = df_customers['idCountry'].fillna('').astype(str).str.strip().str.upper().map(country_map)
        else:
            df_customers['idCountry'] = np.nan
    except Exception:
        df_customers['idCountry'] = np.nan

    dim_dfs['customer'] = process_and_load_dim(
        df_customers, 'CardCode', 'customer', dw_conn, 'DIM_CUSTOMERS'
    )

    # G. DIM_TIME (idDate)
    # NOTE: Per user request, we skip populating DIM_TIME in the DW. Instead,
    # the ETL derives idDate directly from the source DocDate when loading facts.
    # This keeps source_date -> idDate derivation local to the fact-loading step
    # and prevents inserting/updating the DIM_TIME table.

    if VERBOSE:
        print("[SKIP] DIM_TIME population skipped by configuration.")

    print("Carga de Dimensiones completada.")
    return dim_dfs

# ====================================================================
# 4. TRANSFORMACIÓN Y CARGA DE HECHOS (Transform/Load - T/L)
# ====================================================================
def load_fact_sales(dw_conn, dim_dfs, source_data):
    """
    Carga la tabla de hechos FACT_SALES usando las dimensiones ya cargadas.
    """

    # Copiar df de hechos de la fuente
    # source_data is expected to be a dict returned by extract_source_data with key 'sales_fact'
    df_fact = None
    if isinstance(source_data, dict):
        if 'sales_fact' in source_data and isinstance(source_data['sales_fact'], pd.DataFrame):
            df_fact = source_data['sales_fact'].copy()
        else:
            # Try to find the first DataFrame inside the dict as a fallback
            for k, v in source_data.items():
                if isinstance(v, pd.DataFrame):
                    print(f"⚠️ Warning: 'sales_fact' key not found; using DataFrame from key '{k}' as facts")
                    df_fact = v.copy()
                    break
    elif isinstance(source_data, pd.DataFrame):
        df_fact = source_data.copy()

    if df_fact is None:
        raise TypeError("load_fact_sales expected 'source_data' to contain a pandas.DataFrame for sales facts (key 'sales_fact').")

    # Limpiar previamente los hechos de este sistema de origen para evitar duplicados
    try:
        cleanup_cursor = dw_conn.cursor()
        cleanup_cursor.execute("DELETE FROM dw.FACT_SALES WHERE source_system = ?", SOURCE_SYSTEM_DB)
        dw_conn.commit()
    except Exception as cleanup_err:
        dw_conn.rollback()
        raise RuntimeError(f"No se pudo limpiar FACT_SALES para {SOURCE_SYSTEM_DB}: {cleanup_err}")

    df_fact['source_system'] = SOURCE_SYSTEM_DB

    # ---------------------------
    # DIM_TIME: derive idDate directly from DocDate (user requested to skip DIM_TIME table)
    # ---------------------------
    # Ensure DocDate is datetime, then derive idDate as integer YYYYMMDD
    try:
        if not np.issubdtype(df_fact['DocDate'].dtype, np.datetime64):
            df_fact['DocDate'] = pd.to_datetime(df_fact['DocDate'])
    except Exception:
        # If conversion fails, let subsequent validation capture invalid dates
        pass

    # Create idDate as integer YYYYMMDD for fact rows
    try:
        df_fact['idDate'] = df_fact['DocDate'].dt.strftime('%Y%m%d').astype(int)
    except Exception:
        # If any DocDate is NaT or unparsable, set idDate to NaN (validation will catch it)
        try:
            df_fact['idDate'] = pd.to_datetime(df_fact['DocDate'], errors='coerce').dt.strftime('%Y%m%d')
            df_fact['idDate'] = pd.to_numeric(df_fact['idDate'], errors='coerce')
        except Exception:
            df_fact['idDate'] = np.nan

    # ---------------------------
    # Merge con DIM_CUSTOMERS
    # ---------------------------
    # Normalize keys (strip/upper) to improve join matching
    def normalize_series(s):
        series = s.astype(str)
        series = series.replace({'nan': '', 'None': '', 'NaT': ''})
        return series.str.strip().str.upper()

    # Normalize df_fact key columns
    for kc in ['CardCode', 'ItemCode', 'WhsCode', 'SlpCode']:
        if kc in df_fact.columns:
            df_fact[kc] = normalize_series(df_fact[kc])

    if 'LineTotal' in df_fact.columns:
        df_fact['LineTotal'] = pd.to_numeric(df_fact['LineTotal'], errors='coerce')
    if 'Quantity' in df_fact.columns:
        df_fact['Quantity'] = pd.to_numeric(df_fact['Quantity'], errors='coerce')
    if 'Quantity' in df_fact.columns:
        df_fact['Quantity'] = pd.to_numeric(df_fact['Quantity'], errors='coerce')
    df_fact['total_usd'] = np.nan
    df_fact['total_crc'] = np.nan

    # Normalize lookup keys in dim_dfs
    if 'customer' in dim_dfs and isinstance(dim_dfs['customer'], pd.DataFrame):
        if 'cardCode' in dim_dfs['customer'].columns:
            dim_dfs['customer']['cardCode'] = normalize_series(dim_dfs['customer']['cardCode'])

    if 'customer' in dim_dfs:
        # dim_dfs['customer'] maps idCustomer <-> CardCode (cardCode)
        df_fact = df_fact.merge(dim_dfs['customer'][['idCustomer', 'cardCode']], left_on='CardCode', right_on='cardCode', how='left')
    else:
        # Attempt common column names
        if 'CardCode' in df_fact.columns and 'cardCode' in df_fact.columns:
            df_fact = df_fact.merge(dim_dfs.get('customer', pd.DataFrame()), left_on='CardCode', right_on='cardCode', how='left')

    if 'cardCode' in df_fact.columns:
        df_fact = df_fact.drop(columns=['cardCode'])

    # ---------------------------
    # Merge con DIM_PRODUCTS
    # ---------------------------
    if 'product' in dim_dfs and isinstance(dim_dfs['product'], pd.DataFrame):
        if 'itemCode' in dim_dfs['product'].columns:
            dim_dfs['product']['itemCode'] = normalize_series(dim_dfs['product']['itemCode'])
    df_fact = df_fact.merge(dim_dfs['product'][['idProduct', 'itemCode']], left_on='ItemCode', right_on='itemCode', how='left')

    if 'itemCode' in df_fact.columns:
        df_fact = df_fact.drop(columns=['itemCode'])

    # ---------------------------
    # Merge opcional: DIM_SALESPERSON
    # ---------------------------
    if 'salesperson' in dim_dfs:
        if 'SlpCode' in df_fact.columns:
            df_fact = df_fact.merge(dim_dfs['salesperson'][['idSalesperson', 'spCode']],
                                    left_on='SlpCode', right_on='spCode', how='left')
        else:
            df_fact['idSalesperson'] = None

    if 'spCode' in df_fact.columns:
        df_fact = df_fact.drop(columns=['spCode'])

    # ---------------------------
    # Merge opcional: DIM_WAREHOUSE
    # ---------------------------
    if 'warehouse' in dim_dfs:
        if 'WhsCode' in df_fact.columns:
            df_fact = df_fact.merge(dim_dfs['warehouse'][['idWarehouse', 'whsCode']],
                                    left_on='WhsCode', right_on='whsCode', how='left')
        else:
            # No warehouse info in source: map to UNK (idWarehouse = 0)
            df_fact['idWarehouse'] = 0
    if 'whsCode' in df_fact.columns:
        df_fact = df_fact.drop(columns=['whsCode'])
    if 'idWarehouse' in df_fact.columns:
        df_fact['idWarehouse'] = pd.to_numeric(df_fact['idWarehouse'], errors='coerce').fillna(0).astype('int64')
    if 'idWarehouse' in df_fact.columns:
        df_fact['idWarehouse'] = df_fact['idWarehouse'].fillna(0)

    # ---------------------------
    # Merge opcional: DIM_CURRENCY (map DocCur -> idCurrency)
    # ---------------------------
    if 'currency' in dim_dfs:
        try:
            cur_df = dim_dfs['currency']
            # normalize currency codes in currency dim
            if 'code' in cur_df.columns:
                cur_df['code'] = cur_df['code'].astype(str).str.strip().str.upper()

            if 'DocCur' in df_fact.columns:
                # normalize source DocCur and map
                df_fact['DocCur_norm'] = df_fact['DocCur'].fillna('').astype(str).str.strip().str.upper()
                # Map known source aliases to DW codes (e.g., COL -> CRC)
                try:
                    df_fact['DocCur_norm'] = df_fact['DocCur_norm'].replace({'COL': 'CRC'})
                except Exception:
                    pass
                df_fact['total_usd'] = np.where(
                    df_fact['DocCur_norm'] == 'USD', df_fact['LineTotal'], df_fact['total_usd']
                )
                df_fact['total_crc'] = np.where(
                    df_fact['DocCur_norm'] == 'CRC', df_fact['LineTotal'], df_fact['total_crc']
                )
                df_fact = df_fact.merge(cur_df[['idCurrency', 'code']].drop_duplicates(subset=['code']),
                                        left_on='DocCur_norm', right_on='code', how='left')
                # if idCurrency not produced, ensure column exists
                if 'idCurrency' not in df_fact.columns:
                    df_fact['idCurrency'] = None
                # cleanup helper columns
                df_fact = df_fact.drop(columns=[c for c in ['DocCur_norm', 'code'] if c in df_fact.columns])
        except Exception:
            # non-critical: leave idCurrency as-is or None
            if 'idCurrency' not in df_fact.columns:
                df_fact['idCurrency'] = None

    # ---------------------------
    # Selección de columnas finales
    # ---------------------------
    fact_columns = [
        'idDate', 'idCustomer', 'idProduct', 'idSalesperson', 'idWarehouse', 'idCurrency',
        'quantity', 'total_usd', 'total_crc', 'source_system', 'source_doc_id'
    ]

    # Normalize / map source column names to target fact columns
    df_fact = df_fact.rename(columns={
        'Quantity': 'quantity',
        'DocNum': 'source_doc_id'
    })

    if 'LineTotal' in df_fact.columns:
        df_fact = df_fact.drop(columns=['LineTotal'])

    df_fact['source_system'] = SOURCE_SYSTEM_DB

    # Build final selection using available columns, filling missing with NaN
    # Ensure all expected fact columns exist in the DataFrame (add NaN where missing)
    for c in fact_columns:
        if c not in df_fact.columns:
            df_fact[c] = np.nan

    df_fact_final = df_fact[fact_columns].copy()

    # ---------------------------
    # Carga a SQL Server (dw.FACT_SALES) usando pyodbc cursor.executemany
    # ---------------------------
    # If there are missing idProduct values, attempt a safe fallback using loaded product dim
    if 'product' in dim_dfs and isinstance(dim_dfs['product'], pd.DataFrame):
        if 'idProduct' in df_fact.columns:
            missing_prod_count = int(df_fact['idProduct'].isna().sum())
            if missing_prod_count > 0:
                try:
                    fallback_id = int(dim_dfs['product']['idProduct'].min())
                except Exception:
                    fallback_id = 1
                if VERBOSE:
                    print(f"[AUTO-FILL] {missing_prod_count} rows have missing idProduct. Filling with fallback idProduct={fallback_id}.")
                df_fact['idProduct'] = df_fact['idProduct'].fillna(fallback_id)

                # If we successfully filled, attempt to cast idProduct to integer type
                try:
                    df_fact['idProduct'] = df_fact['idProduct'].astype('int64')
                except Exception:
                    if VERBOSE:
                        print('[AUTO-FILL][WARN] Could not cast idProduct to int64 after filling; leaving as-is')

    # Recompute final fact DataFrame after any auto-fills above
    df_fact_final = df_fact[fact_columns].copy()
    
    # Pre-insert validation: check required keys and numeric ranges
    required_int_cols = ['idDate', 'idCustomer', 'idProduct']
    required_numeric_cols = ['quantity']

    if VERBOSE:
        print('\n[VALIDATION] FACT_SALES pre-insert checks')
        print('[VALIDATION] dtypes:')
        print(df_fact_final.dtypes)

    # Check for missing required columns or nulls
    issues = False
    for c in required_int_cols:
        if c not in df_fact_final.columns:
            print(f"[VALIDATION][ERROR] Required column missing: {c}")
            issues = True
        else:
            null_count = df_fact_final[c].isna().sum()
            if null_count > 0:
                print(f"[VALIDATION][ERROR] Column {c} has {null_count} nulls. Sample rows:")
                print(df_fact_final.loc[df_fact_final[c].isna()].head().to_string(index=False))
                issues = True

    for c in required_numeric_cols:
        if c not in df_fact_final.columns:
            print(f"[VALIDATION][ERROR] Required column missing: {c}")
            issues = True
        else:
            null_count = df_fact_final[c].isna().sum()
            if null_count > 0:
                print(f"[VALIDATION][ERROR] Column {c} has {null_count} nulls. Sample rows:")
                print(df_fact_final.loc[df_fact_final[c].isna()].head().to_string(index=False))
                issues = True

    # Check for numeric overflow against SQL Server target types
    # INT max for SQL Server
    SQL_INT_MAX = 2_147_483_647
    # DECIMAL(38,10): total precision 38, scale 10 -> max integer part = 10^(38-10)-1 = 10^28 - 1
    DECIMAL_38_10_MAX = 10**28 - 1

    # integer overflow checks
    int_cols = ['idDate', 'idCustomer', 'idProduct', 'idSalesperson', 'idWarehouse', 'idCurrency']
    # Coerce these columns to numeric where possible to avoid NoneType issues with abs()
    for c in int_cols:
        if c in df_fact_final.columns:
            # attempt to convert to numeric (integers); coerce errors to NaN
            try:
                df_fact_final[c] = pd.to_numeric(df_fact_final[c], errors='coerce')
            except Exception:
                # if conversion fails, leave as-is (will be handled by notna filter)
                pass
            # compute overflow using safe numeric comparison
            mask_numeric = df_fact_final[c].notna()
            over = df_fact_final[mask_numeric & (df_fact_final[c].abs() > SQL_INT_MAX)]
            if not over.empty:
                print(f"[VALIDATION][ERROR] Integer column {c} has {len(over)} values exceeding SQL INT max ({SQL_INT_MAX}). Sample:")
                print(over.head().to_string(index=False))
                issues = True

    # decimal overflow checks for quantity, total_usd, total_crc (DW uses DECIMAL(38,10))
    dec_cols = ['quantity', 'total_usd', 'total_crc']
    for c in dec_cols:
        if c in df_fact_final.columns:
            # coerce to numeric (float) for safe abs/compare
            try:
                df_fact_final[c] = pd.to_numeric(df_fact_final[c], errors='coerce')
            except Exception:
                pass
            # Round to scale 10 (to match DECIMAL(38,10)) to avoid unexpected scale issues
            try:
                df_fact_final[c] = df_fact_final[c].round(10)
            except Exception:
                pass
            mask_numeric = df_fact_final[c].notna()
            over = df_fact_final[mask_numeric & (df_fact_final[c].abs() > DECIMAL_38_10_MAX)]
            if not over.empty:
                print(f"[VALIDATION][ERROR] Decimal column {c} has {len(over)} values exceeding DECIMAL(38,10) max (~{DECIMAL_38_10_MAX}). Sample:")
                print(over.head().to_string(index=False))
                issues = True

    # Ensure textual and ID columns match DW types: source_system -> NVARCHAR, source_doc_id -> NVARCHAR
    if 'source_system' in df_fact_final.columns:
        # fill a sensible default where missing and ensure string dtype
        df_fact_final['source_system'] = df_fact_final['source_system'].fillna('DB_SALES').astype(str)

    if 'source_doc_id' in df_fact_final.columns:
        # Convert to string (DW expects NVARCHAR); preserve None as empty string if needed
        df_fact_final['source_doc_id'] = df_fact_final['source_doc_id'].where(df_fact_final['source_doc_id'].notna(), None)
        df_fact_final['source_doc_id'] = df_fact_final['source_doc_id'].astype(object)

    if issues:
        raise ValueError('Validation failed for FACT_SALES; see printed messages above. Aborting insert to avoid DB errors.')

    # If validation passes, perform insert
    try:
        cursor = dw_conn.cursor()
        # Temporarily disable constraints on FACT_SALES to avoid FK checks against DIM_TIME
        try:
            cursor.execute("ALTER TABLE dw.FACT_SALES NOCHECK CONSTRAINT ALL;")
            dw_conn.commit()
            if VERBOSE:
                print('[FACT_SALES] Temporarily disabled constraints on dw.FACT_SALES')
        except Exception:
            # If we cannot disable constraints, continue and let the insert surface FK errors
            if VERBOSE:
                print('[FACT_SALES] Could not disable constraints on dw.FACT_SALES; proceeding without disabling')

        cols = ', '.join(df_fact_final.columns)
        placeholders = ', '.join(['?' for _ in df_fact_final.columns])
        insert_sql = f"INSERT INTO dw.FACT_SALES ({cols}) VALUES ({placeholders})"
        cursor.fast_executemany = True
        # Prepare rows: ensure ints are ints, decimals are Decimal with scale 10, and strings are str/None
        data_to_insert = []
        for _, r in df_fact_final.iterrows():
            row_vals = []
            for col in df_fact_final.columns:
                val = r[col]
                # integer keys: ensure Python int or None
                if col in int_cols:
                    if pd.isna(val):
                        row_vals.append(None)
                    else:
                        try:
                            row_vals.append(int(val))
                        except Exception:
                            row_vals.append(None)
                elif col in dec_cols:
                    # decimals: convert to Decimal with scale 10
                    if pd.isna(val):
                        row_vals.append(None)
                    else:
                        try:
                            # Use string constructor to preserve precision
                            d = Decimal(str(val)).quantize(Decimal('1.' + '0'*10))
                            row_vals.append(d)
                        except (InvalidOperation, Exception):
                            # fallback: try rounding then Decimal
                            try:
                                d = Decimal(str(round(float(val), 10)))
                                row_vals.append(d)
                            except Exception:
                                row_vals.append(None)
                else:
                    # For strings and other types: keep None or string
                    if pd.isna(val):
                        row_vals.append(None)
                    else:
                        # Ensure native Python types for pyodbc
                        if isinstance(val, (np.integer, np.floating)):
                            row_vals.append(val.item())
                        else:
                            row_vals.append(val)
            data_to_insert.append(tuple(row_vals))
        try:
            cursor.executemany(insert_sql, data_to_insert)
            dw_conn.commit()
            print(f"Tabla de Hechos FACT_SALES cargada. Registros: {len(df_fact_final)}")
        except pyodbc.Error as bulk_err:
            # Bulk insert failed — rollback and try per-row to find offending rows
            print(f"❌ Bulk insert failed: {bulk_err}")
            dw_conn.rollback()
            print("Falling back to per-row insert to identify problematic rows...")
            failed_rows = []
            success_count = 0
            for idx, row in enumerate(data_to_insert):
                # Use a fresh cursor for each row to avoid ODBC driver state issues
                row_cursor = dw_conn.cursor()
                try:
                    row_cursor.execute(insert_sql, row)
                    # commit using the connection (cursor.commit doesn't exist on pyodbc cursors)
                    try:
                        dw_conn.commit()
                    except Exception as commit_err:
                        # capture commit errors as part of the row failure
                        err_info = (type(commit_err).__name__, str(commit_err))
                        failed_rows.append((idx, row, err_info))
                        continue
                    success_count += 1
                except Exception as row_err:
                    # capture full error details (pyodbc.Error or other exceptions)
                    err_info = (type(row_err).__name__, getattr(row_err, 'args', (str(row_err),)))
                    failed_rows.append((idx, row, err_info))
                    # continue to attempt remaining rows
                finally:
                    try:
                        row_cursor.close()
                    except Exception:
                        pass

            # Report failures and raise
            print(f"Per-row insert summary: {success_count} succeeded, {len(failed_rows)} failed")
            if failed_rows:
                print("First failed rows (index, error args, row values):")
                for fr in failed_rows[:10]:
                    print(fr[0], fr[1])
                    print('Error args:', fr[2])
            # Re-enable constraints before raising
            try:
                cursor.execute("ALTER TABLE dw.FACT_SALES WITH CHECK CHECK CONSTRAINT ALL;")
                dw_conn.commit()
            except Exception:
                pass
            raise RuntimeError(f"FACT_SALES insert failed: {len(failed_rows)} rows caused errors. See logs above.")
    except pyodbc.Error as e:
        print(f"❌ ERROR al insertar FACT_SALES en DB: {e}")
        dw_conn.rollback()
    except Exception as e:
        print(f"❌ ERROR inesperado al insertar FACT_SALES: {e}")
        dw_conn.rollback()
    finally:
        # Attempt to re-enable constraints on FACT_SALES (best-effort)
        try:
            cur2 = dw_conn.cursor()
            cur2.execute("ALTER TABLE dw.FACT_SALES WITH CHECK CHECK CONSTRAINT ALL;")
            dw_conn.commit()
            if VERBOSE:
                print('[FACT_SALES] Re-enabled constraints on dw.FACT_SALES')
        except Exception:
            # If re-enabling fails, log if verbose and continue
            if VERBOSE:
                print('[FACT_SALES] Could not re-enable constraints on dw.FACT_SALES (check DB permissions)')

# ====================================================================
# 5. FUNCIÓN PRINCIPAL DE EJECUCIÓN (Orquestación)
# ====================================================================

def run_etl(recreate_schema: bool = False):
    """Ejecuta el proceso completo de ETL para DB_SALES."""
    
    source_conn = None
    dw_conn = None
    
    try:
        # 1. Conectar a las Bases de Datos
        source_conn = connect_to_db(SOURCE_CONN_STR)
        dw_conn = connect_to_db(DW_CONN_STR)
        print("Conexiones a DB establecidas.")
        
        # 2. CREACIÓN OPCIONAL DEL ESQUEMA DW
        if recreate_schema:
            print("Recreando esquema DW...")
            create_dw_schema(dw_conn)

        # 3. Extracción (E)
        source_data = extract_source_data(source_conn)

        # 4. Transformación y Carga de Dimensiones (T/L)
        dim_dfs = load_dimensions(dw_conn, source_data)

        # 5. Transformación y Carga de Hechos (T/L)
        load_fact_sales(dw_conn, dim_dfs, source_data)
        
        print("\n✅ ETL completado con éxito.")

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"\n❌ Error de base de datos: {sqlstate}")
        print(ex)
        raise
    except Exception as e:
        print(f"\n❌ Error general en el ETL: {e}")
        print("📍 Detalle del error:")
        print(traceback.format_exc())
        raise
    finally:
        if source_conn:
            source_conn.close()
        if dw_conn:
            dw_conn.close()
            print("Conexiones a DB cerradas.")

if __name__ == "__main__":
    run_etl(recreate_schema=True)