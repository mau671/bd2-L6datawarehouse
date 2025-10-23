# Decisiones ETL

## Reglas generales

- Todas las cargas usan las definiciones SQL de `infra/db/init_dw`, evitando divergencias entre scripts y código Python.
- Las claves sustitutas se generan en SQL Server; los ETL sólo envían las business keys e identificadores precomputados cuando aplica (ej. `idDate`).
- Se preserva la trazabilidad en `dw.FACT_SALES` mediante `source_system` y `source_doc_id` para distinguir lotes provenientes de `DB_SALES` y del JSON agregado.

## `db_mssql.py`

- Facturas (`OINV/INV1`) y notas de crédito (`ORIN/RIN1`) se consolidan en un único DataFrame; las notas se cargan con cantidades y montos negativos.
- El tipo de cambio se normaliza: monedas `COL` se registran como `CRC` y se almacenan en `total_crc`; las transacciones USD permanecen en `total_usd` para conversión posterior mediante la vista `dw.FACT_SALES_CRC` o el proceso de ajuste.
- `DIM_CUSTOMERS` recibe `idCountry` al momento de la carga utilizando `OCRD.Country` o, en su defecto, la zona asociada; el hecho deja de almacenar país para evitar inconsistencias.
- `DIM_TIME` requiere `day`, `quarter` y `month_name`; si faltan, se calculan a partir de la columna `date` antes del insert.
- Los almacenes sin correspondencia quedan en la llave sintética `0` (`UNK`), garantizando integridad referencial en `FACT_SALES`.

## `ETL_Json.py`

- Se fuerza la existencia de un cliente sintético (`C_JSON`) y se crean los productos faltantes con marca `AGG_JSON`.
- Cada registro mensual se ancla al día 1 del mes correspondiente; si el día no existe en `DIM_TIME`, se inserta con los atributos derivados y `tc_usd_crc` nulo hasta que el proceso de tipos de cambio lo actualice.
- Los montos del JSON se almacenan en USD (`total_usd`); la conversión a CRC se realiza sólo cuando exista tipo de cambio configurado para la fecha.

## `db_excel.py`

- El ETL desde Excel (o CSV homólogo) completa `DIM_TIME` generando `idDate`, `year`, `month`, `day`, `quarter`, `month_name` y la columna `tc_usd_crc`.
- Después de poblar la dimensión, la función `convert_currency_fact_sales` recalcula `total_crc` para filas con monto en USD y tipo de cambio disponible.

## TODO

- [ ] Volver a ejecutar el ETL de tipos de cambio con el archivo definitivo (`TiposCambio_USD_CRC_2024_2025.csv`) para poblar `tc_usd_crc` en la nueva estructura.
- [ ] Validar con datos reales que todos los clientes quedaron asociados a un país y revisar posibles valores nulos en `idCountry`.
- [ ] Ejecutar `run_etl` y `ETL_Json` tras limpiar las tablas para confirmar que la unificación de scripts no rompe la carga incremental.
