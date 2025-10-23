USE DW_SALES;
GO

-- Esquema
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dw') EXEC('CREATE SCHEMA dw');
GO

/* =======================
   CICLO DE RECREACIÓN
   ======================= */

IF OBJECT_ID('dw.FACT_SALES','U') IS NOT NULL DROP TABLE dw.FACT_SALES;
IF OBJECT_ID('dw.DIM_WAREHOUSE','U') IS NOT NULL DROP TABLE dw.DIM_WAREHOUSE;
IF OBJECT_ID('dw.DIM_SALESPERSON','U') IS NOT NULL DROP TABLE dw.DIM_SALESPERSON;
IF OBJECT_ID('dw.DIM_PRODUCTS','U') IS NOT NULL DROP TABLE dw.DIM_PRODUCTS;
IF OBJECT_ID('dw.DIM_CUSTOMERS','U') IS NOT NULL DROP TABLE dw.DIM_CUSTOMERS;
IF OBJECT_ID('dw.DIM_CURRENCY','U') IS NOT NULL DROP TABLE dw.DIM_CURRENCY;
IF OBJECT_ID('dw.DIM_COUNTRY','U') IS NOT NULL DROP TABLE dw.DIM_COUNTRY;
IF OBJECT_ID('dw.DIM_TIME','U') IS NOT NULL DROP TABLE dw.DIM_TIME;
GO

/* =======================
   DIMENSIONES
   ======================= */

-- Dimensión de tiempo con TC (USD->CRC) por fecha (de XLSX 2024-2025)
CREATE TABLE dw.DIM_TIME (
  idDate        INT           NOT NULL PRIMARY KEY,      -- formato YYYYMMDD
  [date]        DATE          NOT NULL,
  [year]        INT           NOT NULL,
  [month]       TINYINT       NOT NULL,
  [day]         TINYINT       NOT NULL,
  [quarter]     TINYINT       NOT NULL,
  month_name    NVARCHAR(15)  NOT NULL,
  tc_usd_crc    DECIMAL(18,6) NULL                       -- desde TIPOS_DE_CAMBIO
);
CREATE UNIQUE INDEX UX_DIM_TIME_DATE ON dw.DIM_TIME([date]);

-- Dimensión de países (origen cliente si aplica)
CREATE TABLE dw.DIM_COUNTRY (
  idCountry  INT IDENTITY(1,1) PRIMARY KEY,
  iso2       NCHAR(2)       NOT NULL UNIQUE,
  [name]     NVARCHAR(100)  NOT NULL
);

-- Dimensión de clientes
CREATE TABLE dw.DIM_CUSTOMERS (
  idCustomer  INT IDENTITY(1,1) PRIMARY KEY,
  cardCode    NVARCHAR(50)  NOT NULL UNIQUE,             -- ej: código SAP o 'AGG_JSON'
  [name]      NVARCHAR(200) NOT NULL,
  zona        NVARCHAR(100) NULL,
  idCountry   INT               NULL REFERENCES dw.DIM_COUNTRY(idCountry)
);

-- Dimensión de productos
CREATE TABLE dw.DIM_PRODUCTS (
  idProduct   INT IDENTITY(1,1) PRIMARY KEY,
  itemCode    NVARCHAR(50)  NOT NULL UNIQUE,
  [name]      NVARCHAR(200) NOT NULL,
  brand       NVARCHAR(100) NULL
);

-- Dimensión de vendedores
CREATE TABLE dw.DIM_SALESPERSON (
  idSalesperson  INT IDENTITY(1,1) PRIMARY KEY,
  spCode         NVARCHAR(50) NOT NULL UNIQUE,
  [name]         NVARCHAR(200) NOT NULL
);

-- Dimensión de almacenes
CREATE TABLE dw.DIM_WAREHOUSE (
  idWarehouse  INT IDENTITY(1,1) PRIMARY KEY,
  whsCode      NVARCHAR(50)  NOT NULL UNIQUE,
  [name]       NVARCHAR(200) NOT NULL
);

-- Opcional: dimensión de moneda (por si se quiere consultar por moneda nativa)
CREATE TABLE dw.DIM_CURRENCY (
  idCurrency   INT IDENTITY(1,1) PRIMARY KEY,
  code         CHAR(3)       NOT NULL UNIQUE,  -- 'USD', 'CRC'
  [name]       NVARCHAR(50)  NOT NULL
);

CREATE UNIQUE INDEX UX_DIM_PRODUCTS_NAME ON dw.DIM_PRODUCTS([name]);


/* =======================
   TABLA DE HECHOS
   ======================= */

-- Recomendación del profesor: un único fact de ventas con total neto
-- (facturas - devoluciones) y manejar montos en USD y CRC
CREATE TABLE dw.FACT_SALES (
  id              BIGINT IDENTITY(1,1) PRIMARY KEY,
  idDate          INT           NOT NULL REFERENCES dw.DIM_TIME(idDate),
  idCustomer      INT           NOT NULL REFERENCES dw.DIM_CUSTOMERS(idCustomer),
  idProduct       INT           NOT NULL REFERENCES dw.DIM_PRODUCTS(idProduct),
  idSalesperson   INT               NULL REFERENCES dw.DIM_SALESPERSON(idSalesperson),
  idWarehouse     INT               NULL REFERENCES dw.DIM_WAREHOUSE(idWarehouse),
  idCurrency      INT               NULL REFERENCES dw.DIM_CURRENCY(idCurrency),

  quantity        DECIMAL(21,6) NOT NULL,  -- positiva en facturas, negativa en devoluciones
  total_usd       DECIMAL(21,6)     NULL,  -- valor final en USD si la fuente lo trae en USD
  total_crc       DECIMAL(21,6)     NULL,  -- valor final en CRC si la fuente lo trae en CRC

  -- Auditoría mínima
  source_system   NVARCHAR(40)  NOT NULL,  -- 'DB_SALES' | 'AGG_JSON'
  source_doc_id   NVARCHAR(80)      NULL,
  load_ts         DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);

-- Índices útiles
CREATE INDEX IX_FACT_SALES_DATE ON dw.FACT_SALES(idDate);
CREATE INDEX IX_FACT_SALES_CUSTOMER ON dw.FACT_SALES(idCustomer);
CREATE INDEX IX_FACT_SALES_PRODUCT ON dw.FACT_SALES(idProduct);
GO

-- Si existe la vista, eliminarla para permitir re-ejecución
IF OBJECT_ID('dw.FACT_SALES_CRC','V') IS NOT NULL
  DROP VIEW dw.FACT_SALES_CRC;
GO

-- Debe ser la PRIMERA sentencia del batch
CREATE VIEW dw.FACT_SALES_CRC AS
SELECT
  f.*,
  CAST(
    CASE
      WHEN f.total_crc IS NOT NULL THEN f.total_crc
      WHEN f.total_usd IS NOT NULL AND t.tc_usd_crc IS NOT NULL THEN f.total_usd * t.tc_usd_crc
      ELSE NULL
    END AS DECIMAL(21,6)
  ) AS total_crc_final
FROM dw.FACT_SALES f
JOIN dw.DIM_TIME t ON t.idDate = f.idDate;
GO
