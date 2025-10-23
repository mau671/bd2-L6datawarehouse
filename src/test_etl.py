#ARCHIVO PARA TESTEAR RESULTADOS SE EJECUTARÍA DESPUES DE ETLS
import pyodbc
from src.db_config import DW_CONN_STR


def main():
    try:
        # Crear conexión
        conn = pyodbc.connect(DW_CONN_STR)
        cursor = conn.cursor()

        # Pedir el número n al usuario
        n = int(input("Ingrese el número de registros a consultar (TOP n): "))

        # Pedir las tablas separadas por coma
        tablas = ["dw.FACT_SALES", "dw.DIM_COUNTRY", "dw.DIM_CURRENCY", "dw.DIM_CUSTOMERS", "dw.DIM_PRODUCTS", "dw.DIM_SALESPERSON", "dw.DIM_WAREHOUSE", "dw.DIM_TIME"]

        # Ejecutar un SELECT TOP n para cada tabla
        for tabla in tablas:
            print(f"\n=== Resultados de la tabla '{tabla}' ===")
            try:
                query = f"SELECT TOP {n} * FROM {tabla};"
                cursor.execute(query)
                rows = cursor.fetchall()

                # Mostrar columnas
                columns = [col[0] for col in cursor.description]
                print(" | ".join(columns))
                print("-" * 50)

                # Mostrar filas
                for row in rows:
                    print(" | ".join(str(value) for value in row))

            except Exception as e:
                print(f"❌ Error al consultar la tabla '{tabla}': {e}")

    except Exception as e:
        print("❌ Error al realizar la consulta:", e)

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
