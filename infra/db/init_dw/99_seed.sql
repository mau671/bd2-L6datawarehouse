USE DW_SALES;
GO

-- Monedas básicas
IF NOT EXISTS (SELECT 1 FROM dw.DIM_CURRENCY WHERE code='USD') INSERT INTO dw.DIM_CURRENCY(code,[name]) VALUES ('USD','US Dollar');
IF NOT EXISTS (SELECT 1 FROM dw.DIM_CURRENCY WHERE code='CRC') INSERT INTO dw.DIM_CURRENCY(code,[name]) VALUES ('CRC','Costa Rican Colón');

-- Cliente sintético para el JSON mensual sin cliente
-- El JSON trae ventas mensuales solo en USD y sin cliente, así que lo homologamos con 'AGG_JSON'
-- y asumimos día 1 del mes correspondiente al cargar al DW.
IF NOT EXISTS (SELECT 1 FROM dw.DIM_CUSTOMERS WHERE cardCode='AGG_JSON')
INSERT INTO dw.DIM_CUSTOMERS(cardCode,[name],zona,idCountry) VALUES ('AGG_JSON','Ventas agregadas JSON','AGG',NULL);
GO
