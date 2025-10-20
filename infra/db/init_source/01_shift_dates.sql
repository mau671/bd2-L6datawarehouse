IF DB_ID('DB_SALES') IS NULL
BEGIN
  RAISERROR('DB_SALES no existe. Ejecuta primero el RESTORE.', 16, 1);
  RETURN;
END
GO

USE DB_SALES;
GO

-- Ajuste de fechas +4 años
UPDATE OINV SET DocDate = DATEADD(year, 4, DocDate);
UPDATE ORIN SET DocDate = DATEADD(year, 4, DocDate);
GO

-- Verificación rápida
SELECT TOP 3 DocDate FROM OINV ORDER BY DocDate;
SELECT TOP 3 DocDate FROM ORIN ORDER BY DocDate;
GO
