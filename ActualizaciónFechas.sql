--Actualice la data para que las ventas
--queden en 2024 (campo DocDate de OINV y ORIN, est�n a 2020,
--sumar los 4 a�os)

UPDATE DBO.OINV
SET DocDate = DATEADD(yyyy,5,DocDate)

UPDATE DBO.ORIN
SET DocDate = DATEADD(yyyy,5,DocDate)

