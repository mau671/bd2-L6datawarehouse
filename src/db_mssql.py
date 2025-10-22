import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime
from db_config import (  SOURCE_CONN_STR, DW_CONN_STR, connect_to_db )
from db_create_tables import create_dw_schema
import warnings
import traceback
from decimal import Decimal, InvalidOperation

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


def process_and_load_dim(df_source, source_key, dim_name, dw_conn, dw_table):
    """
    Genera SK, maneja 'UNK' para WAREHOUSE, mapea columnas y carga la dimensión
    al DW, respetando las propiedades IDENTITY y la convención de nombres.
    """
    
    # 1. Determinación de la Clave Sustituta (SK)
    
    if dw_table == 'DIM_TIME':
        sk_column_name = 'idDate'
    else:
        sk_column_name = f'id{dim_name.capitalize()}'
    
    # 2. Lógica de UNK y Generación de SK
    
    if dw_table == 'DIM_WAREHOUSE':
        
        # 2a. Normalizar el nombre de la columna de nombre a 'name'
        if 'WhsName' in df_source.columns:
             df_source = df_source.rename(columns={'WhsName': 'name'})
        else:
             df_source['name'] = df_source[source_key]
             
        # 2b. Crear la fila 'UNK'
        unknown_row = pd.DataFrame({
            sk_column_name: [0], 
            source_key: ['UNK'], 
            'name': ['Bodega Desconocida']
        })
        
        # 2c. Filtrar el df_source a las claves de negocio y concatenar
        df_source_filtered = df_source[[source_key, 'name']].drop_duplicates()
        
        df_lookup = pd.concat([unknown_row, df_source_filtered], ignore_index=True)
        df_lookup[sk_column_name] = df_lookup.index
        
        df_source = df_lookup # df_source ahora tiene idWarehouse

    else:
        # Lógica para las demás dimensiones.
        # - Para DIM_TIME debemos PRESERVAR la SK (idDate) que viene del origen
        #   (es un entero YYYYMMDD). No generar un id secuencial.
        # - Para las demás dimensiones con IDENTITY generamos SK secuencial.
        if dw_table == 'DIM_TIME':
            # Asegurarnos de que la columna idDate exista y tenga el formato correcto
            df_source = df_source.reset_index(drop=True)
            if sk_column_name not in df_source.columns:
                # Si no existe, intentar inferirla desde la columna 'date'
                if 'date' in df_source.columns:
                    df_source[sk_column_name] = df_source['date'].dt.strftime('%Y%m%d').astype(int)
                else:
                    # Crear una SK secuencial como último recurso
                    df_source[sk_column_name] = (df_source.index + 1)
        else:
            # Generar SK para dimensiones con IDENTITY (idCustomer, idProduct, ...)
            if sk_column_name in df_source.columns:
                df_source = df_source.drop(columns=[sk_column_name])

            df_source = df_source.reset_index(drop=True)
            df_source[sk_column_name] = (df_source.index + 1)

    
    # 3. Preparación de Carga y Mapeo Estricto de Columnas
    
    df_to_load = df_source.copy()
    
    # Definir las columnas esperadas en el DW (ESTRICTO y en el orden de inserción)
    expected_cols = {
        'DIM_TIME': ['idDate', 'date', 'year', 'month', 'tc_usd_crc'],
        'DIM_WAREHOUSE': ['idWarehouse', 'whsCode', 'name'], 
        'DIM_CUSTOMERS': ['cardCode', 'name', 'zona'],
        'DIM_PRODUCTS': ['itemCode', 'name', 'brand'],
        'DIM_SALESPERSON': ['spCode', 'name'],
        'DIM_COUNTRY': ['iso2', 'name'],
        'DIM_CURRENCY': ['code', 'name']
    }
    
    # Mapeo y renombre explícito (para coincidir con el esquema dw.)
    if dw_table == 'DIM_CUSTOMERS':
        df_to_load = df_to_load.rename(columns={'CardCode': 'cardCode', 'zone_name': 'zona'})
    elif dw_table == 'DIM_PRODUCTS':
        df_to_load = df_to_load.rename(columns={'ItemCode': 'itemCode', 'brand_name': 'brand'})
    elif dw_table == 'DIM_SALESPERSON':
        df_to_load = df_to_load.rename(columns={'SlpCode': 'spCode', 'SlpName': 'name'})
    elif dw_table == 'DIM_COUNTRY':
        df_to_load = df_to_load.rename(columns={'Country': 'iso2', 'Name': 'name'})
    elif dw_table == 'DIM_WAREHOUSE':
        df_to_load = df_to_load.rename(columns={'WhsCode': 'whsCode'})
    elif dw_table == 'DIM_TIME':

        if 'tc_usd_crc' not in df_to_load.columns:
            df_to_load['tc_usd_crc'] = np.nan
        
    # Filtrar estrictamente solo las columnas del DW
    cols_to_keep = expected_cols.get(dw_table, [])
    
    df_to_load = df_to_load[[col for col in cols_to_keep if col in df_to_load.columns]].copy()

    # 4. Carga al DW (Manejo de IDENTITY)
    try:
        cursor = dw_conn.cursor()
        
        cols = ', '.join(df_to_load.columns)
        placeholders = ', '.join(['?' for _ in df_to_load.columns])

        if dw_table == 'DIM_WAREHOUSE':

            insert_sql = (
                f"SET IDENTITY_INSERT dw.{dw_table} ON; "
                f"INSERT INTO dw.{dw_table} ({cols}) VALUES ({placeholders}); "
                f"SET IDENTITY_INSERT dw.{dw_table} OFF;"
            )
        elif dw_table == 'DIM_TIME':
      
            # DIM_TIME: Sin IDENTITY, se inserta la SK
            insert_sql = f"INSERT INTO dw.{dw_table} ({cols}) VALUES ({placeholders})"
        else: 
            # Tablas con IDENTITY(1,1): La SK se excluyó de 'cols', por lo que SQL Server la genera.
            insert_sql = f"INSERT INTO dw.{dw_table} ({cols}) VALUES ({placeholders})"
        
        cursor.fast_executemany = True
        data_to_insert = [tuple(row) for row in df_to_load.values]
        cursor.executemany(insert_sql, data_to_insert)
        dw_conn.commit()
        print(f"Dimensión {dw_table} cargada. Registros: {len(data_to_insert)}")
        
    except pyodbc.Error as e:
        # El error 2601 (duplicate key) es un error de datos y se reporta como advertencia.
        print(f"ADVERTENCIA: Falló la simulación de carga para {dw_table}. Error DB: {e.args[0]}")
        dw_conn.rollback()
    except Exception as e:
        print(f"ADVERTENCIA: Falló la simulación de carga para {dw_table}. Error Python: {e}")
        dw_conn.rollback()

    # 5. Retornar el mapa de lookup
    # Evitar devolver dos columnas con el mismo nombre (p. ej. idDate,idDate)
    if sk_column_name == source_key:
        # Devolver solo la SK si la clave de negocio es la misma
        ret = df_source[[sk_column_name]].drop_duplicates()
        return ret
    else:
        # Devolver mapa SK <-> business_key
        cols = [sk_column_name, source_key]
        cols = [c for c in cols if c in df_source.columns]
        ret = df_source[cols].drop_duplicates(subset=[source_key])

        # Normalizar el nombre de la columna de clave de negocio para un uso consistente
        canonical_names = {
            'DIM_CUSTOMERS': 'cardCode',
            'DIM_PRODUCTS': 'itemCode',
            'DIM_WAREHOUSE': 'whsCode',
            'DIM_SALESPERSON': 'spCode',
            'DIM_COUNTRY': 'iso2',
            'DIM_CURRENCY': 'code',
            'DIM_TIME': 'vDate'
        }

        canonical = canonical_names.get(dw_table)
        if canonical and source_key != canonical and source_key in ret.columns:
            ret = ret.rename(columns={source_key: canonical})

        return ret

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
        T1.DocDate, T1.CardCode, T1.SlpCode, T1.DocNum, 
        T2.ItemCode, T2.Quantity, T2.LineTotal, T1.DocCur,
        'INVOICE' AS TransactionType
    FROM OINV T1 
    INNER JOIN INV1 T2 ON T1.DocEntry = T2.DocEntry
    """
    df_sales = pd.read_sql(sales_query, conn)
    
    credit_query = """
    SELECT 
        T1.DocDate, T1.CardCode, T1.SlpCode, T1.DocNum, 
        T2.ItemCode, T2.Quantity * -1 AS Quantity, 
        T2.LineTotal * -1 AS LineTotal, T1.DocCur,
        'CREDIT_NOTE' AS TransactionType
    FROM ORIN T1 
    INNER JOIN RIN1 T2 ON T1.DocEntry = T2.DocEntry
    """
    df_credits = pd.read_sql(credit_query, conn)
    
    df_fact = pd.concat([df_sales, df_credits], ignore_index=True)
    
    # CORRECCIÓN CRÍTICA: Convertir DocDate a datetime
    try:
        df_fact['DocDate'] = pd.to_datetime(df_fact['DocDate'])
    except Exception as e:
        print(f"❌ Error al convertir la columna 'DocDate' a tipo fecha: {e}")
        raise 
        
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

    df_customers = df_customers.rename(columns={
        'Name_zone': 'zona', 'CardName': 'name', 'U_Zona': 'zone_code'
    })
    # Normalize customer country codes to match DIM_COUNTRY.iso2
    try:
        if 'country_code' in df_customers.columns:
            df_customers['country_code'] = df_customers['country_code'].fillna('').astype(str).str.strip().str.upper()
    except Exception:
        pass

    dim_dfs['customer'] = process_and_load_dim(
        df_customers, 'CardCode', 'customer', dw_conn, 'DIM_CUSTOMERS'
    )
    # Enrich the returned customer lookup with country_code so we can resolve idCountry later
    try:
        if isinstance(dim_dfs.get('customer'), pd.DataFrame):
            # Ensure df_customers exposes a 'cardCode' column to match the canonical name
            if 'CardCode' in df_customers.columns and 'cardCode' not in df_customers.columns:
                df_customers['cardCode'] = df_customers['CardCode']
            # If the customer lookup has 'cardCode' and df_customers has 'country_code', merge them
            if 'cardCode' in dim_dfs['customer'].columns and 'country_code' in df_customers.columns:
                dim_dfs['customer'] = dim_dfs['customer'].merge(
                    df_customers[['cardCode', 'country_code']].drop_duplicates(subset=['cardCode']),
                    on='cardCode', how='left'
                )
    except Exception:
        # non-critical: if enrichment fails, continue without country_code in customer lookup
        pass

    # G. DIM_TIME (idDate)
    df_dates = source_data['sales_fact'][['DocDate']].copy() 
    df_dates['idDate'] = df_dates['DocDate'].dt.strftime('%Y%m%d').astype(int)

    df_dates = df_dates.drop_duplicates(subset=['idDate']).copy()

    df_dates['year'] = df_dates['DocDate'].dt.year
    df_dates['month'] = df_dates['DocDate'].dt.month.astype('int8')

    # Renombrar columna original
    df_dates = df_dates.rename(columns={'DocDate': 'date'})

    # Inicializar columna requerida por el esquema, si no está en el origen
    df_dates['tc_usd_crc'] = np.nan

    # Para merge con fact table
    df_dates['vDate'] = df_dates['date']

    # Procesar y cargar al DW
    # NOTE: usamos 'vDate' como business key para que el lookup devuelva idDate <-> vDate
    dim_dfs['time'] = process_and_load_dim(df_dates, 'vDate', 'date', dw_conn, 'DIM_TIME')

    # Ahora dim_dfs['time'] conserva todas las columnas necesarias

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

    # ---------------------------
    # DIM_TIME: preparar lookup y merge
    # ---------------------------
    dim_time = dim_dfs['time'].copy()
    # Ensure we have idDate <-> vDate mapping
    if 'vDate' in dim_time.columns and 'idDate' in dim_time.columns:
        # Remove duplicates on business key vDate
        dim_time = dim_time.drop_duplicates(subset=['vDate'])
        # Convert vDate to datetime if necessary
        if not np.issubdtype(dim_time['vDate'].dtype, np.datetime64):
            dim_time['vDate'] = pd.to_datetime(dim_time['vDate'])
        # Merge using the business key vDate
        df_fact = df_fact.merge(dim_time[['idDate', 'vDate']], left_on='DocDate', right_on='vDate', how='left')
    elif 'idDate' in dim_time.columns:
        # Fallback: if only idDate exists, convert and merge on constructed date
        dim_time = dim_time.drop_duplicates(subset=['idDate'])
        dim_time['date'] = pd.to_datetime(dim_time['idDate'].astype(str), format='%Y%m%d')
        df_fact = df_fact.merge(dim_time[['idDate', 'date']], left_on='DocDate', right_on='date', how='left')
    else:
        raise ValueError('DIM_TIME lookup does not contain idDate or vDate')

    # ---------------------------
    # Merge con DIM_CUSTOMERS
    # ---------------------------
    # Normalize keys (strip/upper) to improve join matching
    def normalize_series(s):
        if s.dtype == object:
            return s.fillna('').astype(str).str.strip().str.upper()
        return s

    # Normalize df_fact key columns
    for kc in ['CardCode', 'ItemCode', 'WhsCode', 'SlpCode']:
        if kc in df_fact.columns:
            df_fact[kc] = normalize_series(df_fact[kc])

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

    # --- Attempt to derive idCountry via the customer lookup's country_code when source fact has no Country column ---
    try:
        if 'customer' in dim_dfs and isinstance(dim_dfs['customer'], pd.DataFrame):
            # if the customer lookup contains country_code, bring it into df_fact using idCustomer
            if 'country_code' in dim_dfs['customer'].columns and 'idCustomer' in df_fact.columns:
                # normalize country codes
                try:
                    dim_dfs['customer']['country_code'] = dim_dfs['customer']['country_code'].astype(str).str.strip().str.upper()
                except Exception:
                    pass
                df_fact = df_fact.merge(dim_dfs['customer'][['idCustomer', 'country_code']], on='idCustomer', how='left')
                # now map country_code (iso2) to idCountry using dim_dfs['country'] if available
                if 'country' in dim_dfs and isinstance(dim_dfs['country'], pd.DataFrame):
                    try:
                        dim_dfs['country']['iso2'] = dim_dfs['country']['iso2'].astype(str).str.strip().str.upper()
                    except Exception:
                        pass
                    df_fact = df_fact.merge(dim_dfs['country'][['idCountry', 'iso2']], left_on='country_code', right_on='iso2', how='left')
                    # If merge produced idCountry from right side, keep it. If not, ensure column exists
                    if 'idCountry' not in df_fact.columns:
                        df_fact['idCountry'] = None
    except Exception:
        # non-critical: leave idCountry as-is or None
        pass

    # Propagate country information from the customer lookup into the fact, then map to idCountry
    try:
        cust_df = dim_dfs.get('customer')
        country_df = dim_dfs.get('country')
        # Merge country_code from customer lookup using idCustomer (if present)
        if isinstance(cust_df, pd.DataFrame) and 'country_code' in cust_df.columns and 'idCustomer' in df_fact.columns:
            df_fact = df_fact.merge(
                cust_df[['idCustomer', 'country_code']].drop_duplicates(subset=['idCustomer']),
                on='idCustomer', how='left'
            )

        # Also attempt to merge country_code by business CardCode (safer if idCustomer was missing)
        if isinstance(cust_df, pd.DataFrame) and 'cardCode' in cust_df.columns and 'CardCode' in df_fact.columns:
            # normalize both sides for matching
            try:
                df_fact['CardCode_norm'] = df_fact['CardCode'].fillna('').astype(str).str.strip().str.upper()
                cust_codes = cust_df[['cardCode', 'country_code']].drop_duplicates(subset=['cardCode']).copy()
                cust_codes['cardCode'] = cust_codes['cardCode'].fillna('').astype(str).str.strip().str.upper()
                df_fact = df_fact.merge(cust_codes, left_on='CardCode_norm', right_on='cardCode', how='left', suffixes=('', '_from_card'))
                # coalesce any country_code values
                if 'country_code' in df_fact.columns and 'country_code_from_card' in df_fact.columns:
                    df_fact['country_code'] = df_fact['country_code'].fillna(df_fact['country_code_from_card'])
                    df_fact = df_fact.drop(columns=['country_code_from_card', 'cardCode'])
                # drop helper
                df_fact = df_fact.drop(columns=['CardCode_norm'])
            except Exception:
                pass

        # Now map country_code (ISO2) to idCountry using country dim
        if isinstance(country_df, pd.DataFrame) and 'iso2' in country_df.columns:
            try:
                country_map = country_df[['iso2', 'idCountry']].drop_duplicates(subset=['iso2']).copy()
                country_map['iso2'] = country_map['iso2'].astype(str).str.strip().str.upper()
                if 'country_code' in df_fact.columns:
                    df_fact['country_code'] = df_fact['country_code'].fillna('').astype(str).str.strip().str.upper()
                    df_fact = df_fact.merge(country_map, left_on='country_code', right_on='iso2', how='left')
                    # If idCountry already exists (unlikely), coalesce
                    if 'idCountry' in df_fact.columns:
                        # keep existing idCountry if present, else use mapped idCountry
                        df_fact['idCountry'] = df_fact['idCountry'].where(df_fact['idCountry'].notna(), df_fact['idCountry'])
            except Exception:
                pass
    except Exception:
        # non-critical
        pass

    # ---------------------------
    # Merge con DIM_PRODUCTS
    # ---------------------------
    if 'product' in dim_dfs and isinstance(dim_dfs['product'], pd.DataFrame):
        if 'itemCode' in dim_dfs['product'].columns:
            dim_dfs['product']['itemCode'] = normalize_series(dim_dfs['product']['itemCode'])
    df_fact = df_fact.merge(dim_dfs['product'][['idProduct', 'itemCode']], left_on='ItemCode', right_on='itemCode', how='left')

    # ---------------------------
    # Merge opcional: DIM_SALESPERSON
    # ---------------------------
    if 'salesperson' in dim_dfs:
        if 'SlpCode' in df_fact.columns:
            df_fact = df_fact.merge(dim_dfs['salesperson'][['idSalesperson', 'spCode']],
                                    left_on='SlpCode', right_on='spCode', how='left')
        else:
            df_fact['idSalesperson'] = None

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

    # ---------------------------
    # Merge opcional: DIM_COUNTRY
    # ---------------------------
    if 'country' in dim_dfs:
        if 'Country' in df_fact.columns:
            df_fact = df_fact.merge(dim_dfs['country'][['idCountry', 'iso2']],
                                    left_on='Country', right_on='iso2', how='left')
        else:
            df_fact['idCountry'] = None

    # ---------------------------
    # Merge opcional: DIM_CURRENCY
    # ---------------------------
    if 'currency' in dim_dfs and 'DocCur' in df_fact.columns:
        df_fact = df_fact.merge(dim_dfs['currency'][['idCurrency', 'code']],
                                left_on='DocCur', right_on='code', how='left')

    # ---------------------------
    # Selección de columnas finales
    # ---------------------------
    fact_columns = [
        'idDate', 'idCustomer', 'idProduct', 'idSalesperson', 'idWarehouse', 'idCountry', 'idCurrency',
        'quantity', 'total_usd', 'total_crc', 'source_system', 'source_doc_id'
    ]

    # Normalize / map source column names to target fact columns
    df_fact = df_fact.rename(columns={
        'Quantity': 'quantity',
        'LineTotal': 'total_usd',
        'DocNum': 'source_doc_id'
    })

    # Build final selection using available columns, filling missing with NaN
    available_cols = [c for c in fact_columns if c in df_fact.columns]
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
    int_cols = ['idDate', 'idCustomer', 'idProduct', 'idSalesperson', 'idWarehouse', 'idCountry', 'idCurrency']
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
            raise RuntimeError(f"FACT_SALES insert failed: {len(failed_rows)} rows caused errors. See logs above.")
    except pyodbc.Error as e:
        print(f"❌ ERROR al insertar FACT_SALES en DB: {e}")
        dw_conn.rollback()
    except Exception as e:
        print(f"❌ ERROR inesperado al insertar FACT_SALES: {e}")
        dw_conn.rollback()

# ====================================================================
# 5. FUNCIÓN PRINCIPAL DE EJECUCIÓN (Orquestación)
# ====================================================================

def run_etl():
    """Ejecuta el proceso completo de ETL."""
    
    source_conn = None
    dw_conn = None
    
    try:
        # 1. Conectar a las Bases de Datos
        source_conn = connect_to_db(SOURCE_CONN_STR)
        dw_conn = connect_to_db(DW_CONN_STR)
        print("Conexiones a DB establecidas.")
        
        # 2. CREACIÓN DEL ESQUEMA DW
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
    except Exception as e:
            print(f"\n❌ Error general en el ETL: {e}")
            print("📍 Detalle del error:")
            print(traceback.format_exc())
    finally:
        if source_conn:
            source_conn.close()
        if dw_conn:
            dw_conn.close()
            print("Conexiones a DB cerradas.")

if __name__ == "__main__":
    run_etl()