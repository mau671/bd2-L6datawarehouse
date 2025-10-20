--Actualice la data para que las ventas
--queden en 2024 (campo DocDate de OINV y ORIN, están a 2020,
--sumar los 4 años)

UPDATE DBO.OINV
SET DocDate = DATEADD(yyyy,4,DocDate)

UPDATE DBO.ORIN
SET DocDate = DATEADD(yyyy,4,DocDate)


