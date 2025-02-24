import os
import zipfile
import pandas as pd
import psycopg2
from psycopg2 import sql
import numpy as np
from datetime import datetime, timedelta

import DBcredentials # Credenciales bases de datos

def descomprimir_archivos(carpeta_zip, carpeta_descomprimida):
    print("Iniciando proceso de descompresión")
    archivos_zip = [os.path.join(carpeta_zip, archivo) for archivo in os.listdir(carpeta_zip) if archivo.endswith('.zip')] # Obtener la lista de archivos zip en la carpeta
    # print("Pre-order:\n", archivos_zip)
    archivos_zip.sort(key=os.path.getmtime, reverse=True) # Ordenar la lista de archivos zip por fecha de modificación (más reciente primero)
    # print("Post-order:\n", archivos_zip)
    
    if archivos_zip: # Si se encontraron archivos zip
        last_file = archivos_zip[0] # Tomar solo el archivo zip más reciente
        carpeta_temporal = os.path.join(carpeta_descomprimida, "raw_data")
        os.makedirs(carpeta_temporal, exist_ok=True) # Crear una carpeta donde se almacenará el archivo descomprimido
        existente = os.listdir(carpeta_temporal)
        if existente: # Si ya existen archivos
            print(f"Existente: {existente}")
            archivo_borrar = os.path.join(carpeta_temporal, existente[0])
            os.remove(archivo_borrar)  # Eliminar el archivo si ya existe
            print(f"Se ha borrado el archivo previo: {archivo_borrar}")

        with zipfile.ZipFile(last_file, 'r') as zip_ref: # Descomprimir el archivo zip más reciente
            zip_ref.extractall(carpeta_temporal)
        print(f"Archivo descomprimido: {last_file}")


def borrar_encabezado(carpeta):
    print("Iniciando función de borrado de encabezados")
    carpeta_raw = os.path.join(carpeta, "raw_data") # Ruta carpeta donde se encuentra archivo descomprimido
    archivos_csv = [archivo for archivo in os.listdir(carpeta_raw) if archivo.endswith('.csv')] # Filtrar los archivos CSV en la carpeta de destino
    # print(archivos_csv)

    # Eliminar el encabezado de cada archivo CSV en la carpeta de destino
    for archivo_csv in archivos_csv:
        ruta_archivo = os.path.join(carpeta_raw, archivo_csv)
        with open(ruta_archivo, 'r') as f:
            lineas = f.readlines()
        with open(ruta_archivo, 'w') as f:
            f.writelines(lineas[6:-1])
        print(f"Lineas borradas del archivo: {archivo_csv}")


def editar_archivos_csv(carpeta):
    print("Iniciando función de arreglo de columnas")
    carpeta_raw = os.path.join(carpeta, "raw_data") # Ruta carpeta donde se encuentra archivo descomprimido
    archivos_csv = [archivo for archivo in os.listdir(carpeta_raw) if archivo.endswith('.csv')] # Filtrar los archivos CSV en la carpeta de destino

    # Modificar los archivos CSV en la carpeta de destino
    for archivo_csv in archivos_csv:
        ruta_archivo = os.path.join(carpeta_raw, archivo_csv)
        # Leer el archivo CSV y cargarlo en un DataFrame
        df = pd.read_csv(ruta_archivo,
                         usecols=["Date","Time","eNodeB Name","Cell Name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
                         )
        # Columnas númericas que presentan problemas si contienen valors no numericos
        numeric_cols = ["L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]  # Agrega aquí las columnas que deseas corregir
        # Reemplazo los valores "NIL" que puedan existir en estas columnas
        df[numeric_cols] = df[numeric_cols].replace("NIL", 0)
        # Concatenar las columnas 'fecha' y 'hora' en una nueva columna "Timestamp". Utilizo metodo insert para posicionarla al inicio, como en la base de datos
        df.insert(0, "Timestamp", df['Date'] + ' ' + df['Time'])
        # Convertir la columna 'Timestamp' a formato datetime
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])
        # Dejar todos los valores de 'Timestamp' con el mismo formato
        df['Timestamp'] = df['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        # Eliminar las columnas 'Date' y 'Time'
        df = df.drop(columns=["Date", "Time"])
        # Renombrar columnas para dejarlas tal cual en la BD
        df = df.rename(columns={"eNodeB Name":"Node_name", "Cell Name":"Cell_name"})
        # Guardar el DataFrame modificado en el archivo CSV
        df.to_csv(ruta_archivo, index=False)
        print(f"Archivo arreglado: {archivo_csv}")
        
def create_table(table_name, table_type):
    if table_type == "celda":
        column_definitions = [
            ("Timestamp", "TIMESTAMP"),
            ("Node_name", "VARCHAR"),
            ("Cell_name", "VARCHAR"),
            ("L.Traffic.ActiveUser.DL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.DL.Max", "SMALLINT"),
            ("L.Traffic.ActiveUser.UL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.UL.Max", "SMALLINT"),
            ("L.Traffic.User.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.User.Max", "SMALLINT"),
            ("L.ChMeas.PRB.DL.Avail", "SMALLINT"),
            ("L.ChMeas.PRB.DL.Used.Avg", "DOUBLE PRECISION"),
            ("L.ChMeas.PRB.UL.Avail", "SMALLINT"),
            ("L.ChMeas.PRB.UL.Used.Avg", "DOUBLE PRECISION"),
            ("L.Thrp.bits.DL(bit)", "BIGINT"),
            ("L.Thrp.bits.UL(bit)", "BIGINT"),
            ("L.Thrp.bits.DL.LastTTI(bit)", "BIGINT"),
            ("L.Thrp.Time.DL.RmvLastTTI(ms)", "BIGINT")
        ]
    elif table_type == "sector":
        column_definitions = [
            ("Timestamp", "TIMESTAMP"),
            ("sector_name", "VARCHAR"),
            ("L.Traffic.ActiveUser.DL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.DL.Max", "SMALLINT"),
            ("L.Traffic.ActiveUser.UL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.UL.Max", "SMALLINT"),
            ("L.Traffic.User.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.User.Max", "SMALLINT"),
            ("L.ChMeas.PRB.DL.Avail", "SMALLINT"),
            ("L.ChMeas.PRB.DL.Used.Avg", "DOUBLE PRECISION"),
            ("L.ChMeas.PRB.UL.Avail", "SMALLINT"),
            ("L.ChMeas.PRB.UL.Used.Avg", "DOUBLE PRECISION"),
            ("L.Thrp.bits.DL(bit)", "BIGINT"),
            ("L.Thrp.bits.UL(bit)", "BIGINT"),
            ("L.Thrp.bits.DL.LastTTI(bit)", "BIGINT"),
            ("L.Thrp.Time.DL.RmvLastTTI(ms)", "BIGINT")
        ]
    elif table_type == "nodo":
        column_definitions = [
            ("Timestamp", "TIMESTAMP"),
            ("node_name", "VARCHAR"),
            ("L.Traffic.ActiveUser.DL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.DL.Max", "SMALLINT"),
            ("L.Traffic.ActiveUser.UL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.UL.Max", "SMALLINT"),
            ("L.Traffic.User.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.User.Max", "SMALLINT"),
            ("L.ChMeas.PRB.DL.Avail", "SMALLINT"),
            ("L.ChMeas.PRB.DL.Used.Avg", "DOUBLE PRECISION"),
            ("L.ChMeas.PRB.UL.Avail", "SMALLINT"),
            ("L.ChMeas.PRB.UL.Used.Avg", "DOUBLE PRECISION"),
            ("L.Thrp.bits.DL(bit)", "BIGINT"),
            ("L.Thrp.bits.UL(bit)", "BIGINT"),
            ("L.Thrp.bits.DL.LastTTI(bit)", "BIGINT"),
            ("L.Thrp.Time.DL.RmvLastTTI(ms)", "BIGINT")
        ]
    elif table_type == "cluster":
        column_definitions = [
            ("Timestamp", "TIMESTAMP"),
            ("cluster_name", "VARCHAR"),
            ("L.Traffic.ActiveUser.DL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.DL.Max", "SMALLINT"),
            ("L.Traffic.ActiveUser.UL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.UL.Max", "SMALLINT"),
            ("L.Traffic.User.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.User.Max", "SMALLINT"),
            ("L.ChMeas.PRB.DL.Avail", "SMALLINT"),
            ("L.ChMeas.PRB.DL.Used.Avg", "DOUBLE PRECISION"),
            ("L.ChMeas.PRB.UL.Avail", "SMALLINT"),
            ("L.ChMeas.PRB.UL.Used.Avg", "DOUBLE PRECISION"),
            ("L.Thrp.bits.DL(bit)", "BIGINT"),
            ("L.Thrp.bits.UL(bit)", "BIGINT"),
            ("L.Thrp.bits.DL.LastTTI(bit)", "BIGINT"),
            ("L.Thrp.Time.DL.RmvLastTTI(ms)", "BIGINT")
        ]
    elif table_type == "localidad":
        column_definitions = [
            ("Timestamp", "TIMESTAMP"),
            ("localidad_dane_code", "INTEGER"),
            ("L.Traffic.ActiveUser.DL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.DL.Max", "INTEGER"),
            ("L.Traffic.ActiveUser.UL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.UL.Max", "INTEGER"),
            ("L.Traffic.User.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.User.Max", "INTEGER"),
            ("L.ChMeas.PRB.DL.Avail", "INTEGER"),
            ("L.ChMeas.PRB.DL.Used.Avg", "DOUBLE PRECISION"),
            ("L.ChMeas.PRB.UL.Avail", "INTEGER"),
            ("L.ChMeas.PRB.UL.Used.Avg", "DOUBLE PRECISION"),
            ("L.Thrp.bits.DL(bit)", "BIGINT"),
            ("L.Thrp.bits.UL(bit)", "BIGINT"),
            ("L.Thrp.bits.DL.LastTTI(bit)", "BIGINT"),
            ("L.Thrp.Time.DL.RmvLastTTI(ms)", "BIGINT")
        ]
    elif table_type == "municipio":
        column_definitions = [
            ("Timestamp", "TIMESTAMP"),
            ("municipio_dane_code", "INTEGER"),
            ("L.Traffic.ActiveUser.DL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.DL.Max", "INTEGER"),
            ("L.Traffic.ActiveUser.UL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.UL.Max", "INTEGER"),
            ("L.Traffic.User.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.User.Max", "INTEGER"),
            ("L.ChMeas.PRB.DL.Avail", "INTEGER"),
            ("L.ChMeas.PRB.DL.Used.Avg", "DOUBLE PRECISION"),
            ("L.ChMeas.PRB.UL.Avail", "INTEGER"),
            ("L.ChMeas.PRB.UL.Used.Avg", "DOUBLE PRECISION"),
            ("L.Thrp.bits.DL(bit)", "BIGINT"),
            ("L.Thrp.bits.UL(bit)", "BIGINT"),
            ("L.Thrp.bits.DL.LastTTI(bit)", "BIGINT"),
            ("L.Thrp.Time.DL.RmvLastTTI(ms)", "BIGINT")
        ]
    elif table_type == "area_metro":
        column_am = [
            ("Timestamp", "TIMESTAMP"),
            ("am_name", "VARCHAR"),
            ("L.Traffic.ActiveUser.DL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.DL.Max", "INTEGER"),
            ("L.Traffic.ActiveUser.UL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.UL.Max", "INTEGER"),
            ("L.Traffic.User.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.User.Max", "INTEGER"),
            ("L.ChMeas.PRB.DL.Avail", "INTEGER"),
            ("L.ChMeas.PRB.DL.Used.Avg", "DOUBLE PRECISION"),
            ("L.ChMeas.PRB.UL.Avail", "INTEGER"),
            ("L.ChMeas.PRB.UL.Used.Avg", "DOUBLE PRECISION"),
            ("L.Thrp.bits.DL(bit)", "BIGINT"),
            ("L.Thrp.bits.UL(bit)", "BIGINT"),
            ("L.Thrp.bits.DL.LastTTI(bit)", "BIGINT"),
            ("L.Thrp.Time.DL.RmvLastTTI(ms)", "BIGINT")
            ]
    elif table_type == "departamento":
        column_definitions = [
            ("Timestamp", "TIMESTAMP"),
            ("dpto_dane_code", "INTEGER"),
            ("L.Traffic.ActiveUser.DL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.DL.Max", "INTEGER"),
            ("L.Traffic.ActiveUser.UL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.UL.Max", "INTEGER"),
            ("L.Traffic.User.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.User.Max", "INTEGER"),
            ("L.ChMeas.PRB.DL.Avail", "INTEGER"),
            ("L.ChMeas.PRB.DL.Used.Avg", "DOUBLE PRECISION"),
            ("L.ChMeas.PRB.UL.Avail", "INTEGER"),
            ("L.ChMeas.PRB.UL.Used.Avg", "DOUBLE PRECISION"),
            ("L.Thrp.bits.DL(bit)", "BIGINT"),
            ("L.Thrp.bits.UL(bit)", "BIGINT"),
            ("L.Thrp.bits.DL.LastTTI(bit)", "BIGINT"),
            ("L.Thrp.Time.DL.RmvLastTTI(ms)", "BIGINT")
        ]
    elif table_type == "regional":
        column_definitions = [
            ("Timestamp", "TIMESTAMP"),
            ("regional_name", "VARCHAR"),
            ("L.Traffic.ActiveUser.DL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.DL.Max", "INTEGER"),
            ("L.Traffic.ActiveUser.UL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.UL.Max", "INTEGER"),
            ("L.Traffic.User.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.User.Max", "INTEGER"),
            ("L.ChMeas.PRB.DL.Avail", "INTEGER"),
            ("L.ChMeas.PRB.DL.Used.Avg", "DOUBLE PRECISION"),
            ("L.ChMeas.PRB.UL.Avail", "INTEGER"),
            ("L.ChMeas.PRB.UL.Used.Avg", "DOUBLE PRECISION"),
            ("L.Thrp.bits.DL(bit)", "BIGINT"),
            ("L.Thrp.bits.UL(bit)", "BIGINT"),
            ("L.Thrp.bits.DL.LastTTI(bit)", "BIGINT"),
            ("L.Thrp.Time.DL.RmvLastTTI(ms)", "BIGINT")
        ]
    elif table_type == "total":
        column_definitions = [
            ("Timestamp", "TIMESTAMP"),
            ("L.Traffic.ActiveUser.DL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.DL.Max", "INTEGER"),
            ("L.Traffic.ActiveUser.UL.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.ActiveUser.UL.Max", "INTEGER"),
            ("L.Traffic.User.Avg", "DOUBLE PRECISION"),
            ("L.Traffic.User.Max", "INTEGER"),
            ("L.ChMeas.PRB.DL.Avail", "INTEGER"),
            ("L.ChMeas.PRB.DL.Used.Avg", "DOUBLE PRECISION"),
            ("L.ChMeas.PRB.UL.Avail", "INTEGER"),
            ("L.ChMeas.PRB.UL.Used.Avg", "DOUBLE PRECISION"),
            ("L.Thrp.bits.DL(bit)", "BIGINT"),
            ("L.Thrp.bits.UL(bit)", "BIGINT"),
            ("L.Thrp.bits.DL.LastTTI(bit)", "BIGINT"),
            ("L.Thrp.Time.DL.RmvLastTTI(ms)", "BIGINT")
        ]
    elif table_type == "kpi":
        column_definitions = [
            ("Date", "DATE"),
            ("BH", "TIME"),
            ("cell_name", "VARCHAR"),
            ("avg_users_BH", "REAL"),
            ("daily_max_users", "INTEGER"),
            ("max_users_hour", "TIME"),
            ("PRBusage_BH_DL", "REAL"),
            ("PRBusage_BH_UL", "REAL"),
            ("traffic_bh(GB)", "REAL"),
            ("traffic_avg(GB)", "REAL"),
            ("traffic_total(GB)", "REAL"),
            ("uexp_BH(Mbps)", "REAL")
        ]
    else:
        print("El tipo de tabla ingresado no es valido")
        return False
    
    # Conectar a la base de datos PostgreSQL
    conn = psycopg2.connect(**DBcredentials.BD_DATA_PARAMS)

    # Crear un cursor
    cur = conn.cursor()

    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = %s
        )
        """,
        (table_name,)
    )
    table_exists = cur.fetchone()[0]

    # Si la tabla no existe, crearla
    if not table_exists:
        print("Creando la tabla: ", table_name)

        # Crear la consulta CREATE TABLE
        create_table_query = sql.SQL(
            "CREATE TABLE {} ({})").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(
                    sql.SQL("{} {}").format(
                        sql.Identifier(column_name),
                        sql.SQL(column_type)
                    )
                    for column_name, column_type in column_definitions
                )
            )
        # print("Consulta CREATE TABLE:", create_table_query.as_string(cur))

        # Ejecutar la consulta CREATE TABLE
        cur.execute(create_table_query)
        conn.commit() # Confirmar los cambios
        print(f"Tabla {table_name} creada con exito")

        index_name = f"idx_{table_name}_time_name" # Crear el nombre del índice compuesto
        
        if table_type == "celda":
            # Definir la sentencia SQL para crear el índice compuesto de la tabla de celdas
            create_index_query = sql.SQL("CREATE INDEX {} ON {} (\"Timestamp\", \"Cell_name\");").format(sql.Identifier(index_name), sql.Identifier(table_name))

        elif table_type == "total":
            # Para el total de la red
            create_index_query = sql.SQL("CREATE INDEX {} ON {} (\"Timestamp\");").format(sql.Identifier(index_name), sql.Identifier(table_name))

        elif table_type == "kpi":
            # Para la tabla de kpis diarios
            create_index_query = sql.SQL("CREATE INDEX {} ON {} (\"Date\", \"cell_name\");").format(sql.Identifier(index_name), sql.Identifier(table_name))
        else:
            # Definir la sentencia SQL para crear el índice compuesto en las columnas de Timestamp y nombre de la celda (Menos tabla de celdas y tabla total)
            columna2 = column_definitions[1][0]  # Tomo la segunda en la lista y extraigo el nombre
            create_index_query = sql.SQL("CREATE INDEX {} ON {} (\"Timestamp\", {});").format(sql.Identifier(index_name), sql.Identifier(table_name), sql.Identifier(columna2))

        # print("Consulta crear indice:", create_index_query.as_string(cur))

        cur.execute(create_index_query) # Ejecutar la sentencia SQL para crear el índice
        conn.commit() # Confirmar los cambios
        print("Indices de la tabla creados con exito")
        
    else:
        print(f"La tabla {table_name} existe")

    cur.close()    
    conn.close()
    
    return True

def cargar_datos_postgresql(carpeta, table_name):
    print("Iniciando función de subida a base de datos")
    carpeta_raw = os.path.join(carpeta, "raw_data") # Ruta carpeta donde se encuentra archivo descomprimido
    archivos_csv = [archivo for archivo in os.listdir(carpeta_raw) if archivo.endswith('.csv')] # Filtrar los archivos CSV en la carpeta de destino

    # Conectar a la base de datos PostgreSQL
    conn = psycopg2.connect(**DBcredentials.BD_DATA_PARAMS)

    # Crear un cursor
    cur = conn.cursor()

    # Verificar si la tabla existe
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = %s
        )
        """,
        (table_name,)
    )
    table_exists = cur.fetchone()[0]

    # Si la tabla existe
    if table_exists:
        print(f"La tabla {table_name} existe")
        # Preparar la consulta COPY
        columns = ["Timestamp","Node_name","Cell_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
        copy_query = sql.SQL("COPY {} ({}) FROM STDIN WITH CSV HEADER").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )

        # Ejecutar la consulta COPY
        print("Iniciando consulta COPY")
        for archivo_csv in archivos_csv:
            ruta_archivo = os.path.join(carpeta, archivo_csv)
            with open(ruta_archivo, 'r') as f:
                cur.copy_expert(sql=copy_query, file=f)
                conn.commit()
            print(f"{archivo_csv} terminado")
    else:
        print(f"La tabla {table_name} NO EXISTE")
        # Commit y cerrar la conexión a la base de datos PostgreSQL
        conn.commit()
        conn.close()

def cargar_archivo_postgresql(conn, archivo, table_name, table_type, columns):
    # Crear un cursor
    cur = conn.cursor()

    # Si la tabla no existe, crearla
    table_exists = create_table(table_name, table_type)

    if table_exists: # No inicia consulta COPY si hubo algún error
        print("Iniciando consulta COPY")
        # Preparar la consulta COPY
        copy_query = sql.SQL("COPY {} ({}) FROM STDIN WITH CSV HEADER").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )

        # Ejecutar la consulta COPY
        with open(archivo, 'r') as f:
            cur.copy_expert(sql=copy_query, file=f)
            conn.commit()
        print(f"Archivo {archivo} subido exitosamente")

        # Cerrar cursor y commit para guardar cambios en la base de datos
        cur.close()
        conn.commit()
    else:
        print(f"Hubo un error relacionado con la creación de la tabla")

def celdas(conn, carpeta): # Función que agrega los datos de celda del archivo CSV a la base de datos
    print("Iniciando función que sube info de celdas")
    carpeta_raw = os.path.join(carpeta, "raw_data") # Ruta carpeta donde se encuentra archivo descomprimido
    archivos_csv = [archivo for archivo in os.listdir(carpeta_raw) if archivo.endswith('.csv')] # Filtrar los archivos CSV en la carpeta de destino

    cur = conn.cursor() # Crear un cursor
    for archivo_csv in archivos_csv:
        ruta_archivo = os.path.join(carpeta_raw, archivo_csv)
        print(ruta_archivo)
        columnas = ["Timestamp","Node_name","Cell_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
        cargar_archivo_postgresql(conn, ruta_archivo, "ran_1h_cell", "celda", columnas) # Llamado a función que sube el archivo a la base de datos

    cur.close()
    print("Se terminó de agregar las celdas a la base de datos")

def bit_to_GB(bit):
    gbyte = bit / (8*10**9)
    return gbyte

def raw_to_kpi(conn, carpeta):
    print("Iniciando función de agregación de KPIs")
    carpeta_raw = os.path.join(carpeta, "raw_data") # Ruta carpeta donde se encuentra archivo descomprimido
    archivos_csv = [archivo for archivo in os.listdir(carpeta_raw) if archivo.endswith('.csv')] # Filtrar los archivos CSV en la carpeta de destino
    
    for archivo_csv in archivos_csv:
        ruta_archivo = os.path.join(carpeta_raw, archivo_csv)
        df_raw = pd.read_csv(ruta_archivo, 
                             usecols=["Timestamp","Cell_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
                             ) # Leer el archivo CSV y cargarlo en un DataFrame
        df_raw["Cell_name"] = df_raw["Cell_name"].str.upper() # Valores a mayusculas
        df_raw["Timestamp"] = pd.to_datetime(df_raw["Timestamp"]) # Convertir la columna 'Timestamp' a formato datetime

        # Encontrar hora pico (BH)
        bh_day = df_raw.groupby(["Cell_name"])["L.Traffic.ActiveUser.DL.Avg"].idxmax() # Agrupo los datos por nombre de celda y luego encuentro las horas donde los usuarios son máximos
        bh_df = df_raw.loc[bh_day].reset_index(drop=True) # Creo un nuevo df con las horas pico por día
        bh_df.insert(0, "Date", bh_df['Timestamp'].dt.date) # Crear columna con días
        bh_df.insert(1, "BH", bh_df['Timestamp'].dt.time) # Crear columna con días
        bh_df = bh_df.rename(columns={"L.Traffic.ActiveUser.DL.Avg":"avg_users_BH"})
        bh_df = bh_df.drop(columns=["Timestamp","L.Traffic.ActiveUser.DL.Max"])

        # Ocupación PRBs
        bh_df["PRBusage_BH_DL"] = (bh_df["L.ChMeas.PRB.DL.Used.Avg"] / bh_df["L.ChMeas.PRB.DL.Avail"]) * 100 # Cálculo de % ocupación en downlink y guardado en nueva columna
        bh_df["PRBusage_BH_UL"] = (bh_df["L.ChMeas.PRB.UL.Used.Avg"] / bh_df["L.ChMeas.PRB.UL.Avail"]) * 100 # Cálculo de % ocupación en uplink y guardado en nueva columna
        bh_df = bh_df.drop(columns=["L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.ChMeas.PRB.UL.Avail"])

        # Traffic bh
        bh_df["traffic_bh(GB)"] = bh_df["L.Thrp.bits.DL(bit)"].apply(bit_to_GB) # Conversión de bit a GB

        # User experience
        bh_df["uexp_BH(Mbps)"] = ((bh_df["L.Thrp.bits.DL(bit)"]-bh_df["L.Thrp.bits.DL.LastTTI(bit)"]) / (bh_df["L.Thrp.Time.DL.RmvLastTTI(ms)"])) / 1024 # Calculo user experience
        bh_df = bh_df.drop(columns=["L.Thrp.bits.DL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"])

        # BH users_max
        df_raw_max = df_raw[["Timestamp","Cell_name","L.Traffic.ActiveUser.DL.Max"]].copy()
        max_bh = df_raw_max.groupby(["Cell_name"])["L.Traffic.ActiveUser.DL.Max"].idxmax() # Agrupo los datos por nombre de celda y luego encuentro las horas donde los usuarios son máximos
        max_df = df_raw_max.loc[max_bh].reset_index(drop=True) # Creo un nuevo df con las horas pico por día
        max_df.insert(1, "max_users_hour", max_df['Timestamp'].dt.time) # Crear columna con días
        max_df = max_df.drop(columns=["Timestamp"]) # Eliminar columna que ya no necesito
        max_df = max_df.rename(columns={"L.Traffic.ActiveUser.DL.Max":"daily_max_users"}) # Renombrar columna
        # print("bh_df max\n", max_df)
        bh_df = bh_df.merge(max_df,how="left",on="Cell_name")

        # traffic avg
        trff_avg_df = df_raw.groupby(["Cell_name"])["L.Thrp.bits.DL(bit)"].mean().reset_index() # Promedio del tráfico de cada hora del día
        trff_avg_df["traffic_avg(GB)"] = trff_avg_df["L.Thrp.bits.DL(bit)"].apply(bit_to_GB) # Conversion de bit a GB
        trff_avg_df = trff_avg_df.drop(columns=["L.Thrp.bits.DL(bit)"]) # Eliminar columna que ya no necesito
        # print("trff_avg\n",trff_avg_df)
        bh_df = bh_df.merge(trff_avg_df,how="left",on="Cell_name")

        # traffic sum
        trff_sum_df = df_raw.groupby(["Cell_name"])["L.Thrp.bits.DL(bit)"].sum().reset_index() # Suma del tráfico de cada hora del día
        trff_sum_df["traffic_total(GB)"] = trff_sum_df["L.Thrp.bits.DL(bit)"].apply(bit_to_GB) # Conversion de bit a GB
        trff_sum_df = trff_sum_df.drop(columns=["L.Thrp.bits.DL(bit)"]) # Eliminar columna que ya no necesito
        # print("trff_total\n",trff_sum_df)
        bh_df = bh_df.merge(trff_sum_df,how="left",on="Cell_name")

        # Reordenar columnas tal como en la base de datos
        column_order = ["Date","BH","Cell_name","avg_users_BH","daily_max_users","max_users_hour","PRBusage_BH_DL","PRBusage_BH_UL","traffic_bh(GB)","traffic_avg(GB)","traffic_total(GB)","uexp_BH(Mbps)"]
        bh_df = bh_df[column_order]

        nueva_ruta = os.path.join(carpeta, "kpi_temp.csv")
        bh_df.to_csv(nueva_ruta, index=False)

        columnas = ["Date","BH","cell_name","avg_users_BH","daily_max_users","max_users_hour","PRBusage_BH_DL","PRBusage_BH_UL","traffic_bh(GB)","traffic_avg(GB)","traffic_total(GB)","uexp_BH(Mbps)"]
        cargar_archivo_postgresql(conn, nueva_ruta, "ran_kpi_cell", "kpi", columnas) # Llamado a función que sube el archivo a la base de datos
        
    print("Se terminó de agregar los KPIs diarios con exito")

def sectores(conn, carpeta, df_geo): # Función que agrega sectores desde el archivo de celdas
    print("Iniciando función agregación sectores")
    df_geo = df_geo[["dwh_cell_name_wom","sector_name"]].copy() # Hago copia del df solo con las columnas que necesito
    carpeta_raw = os.path.join(carpeta, "raw_data") # Ruta carpeta donde se encuentra archivo descomprimido
    archivos_csv = [archivo for archivo in os.listdir(carpeta_raw) if archivo.endswith('.csv')] # Filtrar los archivos CSV en la carpeta de destino
    
    for archivo_csv in archivos_csv:
        ruta_archivo = os.path.join(carpeta_raw, archivo_csv)
        df_day = pd.read_csv(ruta_archivo, 
                             usecols=["Timestamp","Cell_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
                             ) # Leer el archivo CSV y cargarlo en un DataFrame
        df_day["Cell_name"] = df_day["Cell_name"].str.upper() # Valores a mayusculas
        df_merged = df_day.merge(df_geo, left_on='Cell_name', right_on='dwh_cell_name_wom', how='left')

        # Agrupar los datos por 'Timestamp' y 'sector_name' y sumar los valores
        df_merged = df_merged.groupby(['Timestamp', 'sector_name']).sum(numeric_only=True).reset_index() # Cuando pongo el atributo numeric_omly borra las columnas no numericas

        nueva_ruta = os.path.join(carpeta, "sector_temp.csv")
        df_merged.to_csv(nueva_ruta, index=False)

        columnas = ["Timestamp","sector_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
        cargar_archivo_postgresql(conn, nueva_ruta, "ran_1h_sector", "sector", columnas) # Llamado a función que sube el archivo a la base de datos
    
    print("Se terminó de agregar los sectores con exito")

def nodos(conn, carpeta): # Función que agrega nodos desde archivo de celdas
    print("Iniciando función agregación nodos")
    carpeta_raw = os.path.join(carpeta, "raw_data") # Ruta carpeta donde se encuentra archivo descomprimido
    archivos_csv = [archivo for archivo in os.listdir(carpeta_raw) if archivo.endswith('.csv')] # Filtrar los archivos CSV en la carpeta de destino

    for archivo_csv in archivos_csv:
        ruta_archivo = os.path.join(carpeta_raw, archivo_csv)
        df_day = pd.read_csv(ruta_archivo, 
                             usecols=["Timestamp","Node_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
                             ) # Leer el archivo CSV y cargarlo en un DataFrame
        df_day["Node_name"] = df_day["Node_name"].str.upper() # Valores a mayusculas
        df_day = df_day.groupby(['Timestamp', 'Node_name']).sum(numeric_only=True).reset_index() # Cuando pongo el atributo numeric_only borra las columnas no numericas
        df_day.rename(columns={"Node_name": "node_name"}, inplace=True) # Renombrar columna de Node_name a node_name porque así se guarda en la base de datos

        nueva_ruta = os.path.join(carpeta, "node_temp.csv")
        df_day.to_csv(nueva_ruta, index=False)

        columnas = ["Timestamp","node_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
        cargar_archivo_postgresql(conn, nueva_ruta, "ran_1h_node", "nodo", columnas) # Llamado a función que sube el archivo a la base de datos

    print("Se terminó de agregar los nodos con exito")

def cluster(conn, carpeta, df_geo): # Función que agrega cluster desde archivo de nodos
    print("Iniciando función agregación clusters")
    df_geo = df_geo[["node_name", "cluster_key"]].copy() # Hago copia del df solo con las columnas que necesito
    df_geo = df_geo.drop_duplicates(subset="node_name")

    ruta_archivo = os.path.join(carpeta, "node_temp.csv")
    df_day = pd.read_csv(ruta_archivo, 
                            usecols=["Timestamp","node_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
                            ) # Leer el archivo CSV y cargarlo en un DataFrame
    df_merged = df_day.merge(df_geo, on="node_name", how='left')

    # Agrupar los datos por 'Timestamp' y 'cluster_key' y sumar los valores
    df_merged = df_merged.groupby(['Timestamp', 'cluster_key']).sum(numeric_only=True).reset_index() # Cuando pongo el atributo numeric_only borra las columnas no numericas

    nueva_ruta = os.path.join(carpeta, "cluster_temp.csv")
    df_merged.to_csv(nueva_ruta, index=False)

    columnas = ["Timestamp","cluster_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
    cargar_archivo_postgresql(conn, nueva_ruta, "ran_1h_cluster", "cluster", columnas) # Llamado a función que sube el archivo a la base de datos

    print("Se terminó de agregar los clusters con exito")

def comprobacion_localidad(row): # Función para verificar si el código de localidad es correcto
    # La finalidad de esta función es evitar agrupar las celdas que contienen un error en su codigo DANE de localidades en la base de datos

    localidad_code = str(row["dwh_dane_cod_localidad"])
    municipio_code = str(row["dane_code"])
    
    # Verificar si el código del municipio está contenido en el código de la localidad
    if localidad_code.startswith(municipio_code):
        return True
    else:
        return False

def localidad(conn, carpeta, df_geo): # Función que agrega localidades desde archivo de nodos
    print("Iniciando función agregación localidades")
    df_geo = df_geo[["node_name","dwh_dane_cod_localidad","dane_code"]].copy() # Hago copia del df solo con las columnas que necesito
    df_geo = df_geo[df_geo["dwh_dane_cod_localidad"] != -1]

    filas_correctas = df_geo.apply(comprobacion_localidad, axis=1) # Aplicar función para comprobar localidades a cada fila del df
    df_geo = df_geo[filas_correctas] # Filtrar filas con códigos de localidad correctas
    df_geo = df_geo.drop_duplicates(subset="node_name") # Eliminar nodos duplicados
    df_geo = df_geo.drop(columns=["dane_code"]) # Eliminar columna que ya no es necesaria

    ruta_archivo = os.path.join(carpeta, "node_temp.csv")
    df_day = pd.read_csv(ruta_archivo, 
                            usecols=["Timestamp","node_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
                            ) # Leer el archivo CSV y cargarlo en un DataFrame
    df_merged = df_day.merge(df_geo, on="node_name", how='left')

    # Agrupar los datos por 'Timestamp' y codigo dane de localidad y sumar los valores
    df_merged = df_merged.groupby(['Timestamp', 'dwh_dane_cod_localidad']).sum(numeric_only=True).reset_index() # Cuando pongo el atributo numeric_omly borra las columnas no numericas
    df_merged["dwh_dane_cod_localidad"] = df_merged["dwh_dane_cod_localidad"].astype(int)

    nueva_ruta = os.path.join(carpeta, "localidad_temp.csv")
    df_merged.to_csv(nueva_ruta, index=False)

    columnas = ["Timestamp","localidad_dane_code","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
    cargar_archivo_postgresql(conn, nueva_ruta, "ran_1h_localidad", "localidad", columnas) # Llamado a función que sube el archivo a la base de datos

    print("Se terminó de agregar las localidades con exito")

def municipio(conn, carpeta, df_geo):
    print("Iniciando función agregación municipios")
    df_geo = df_geo[["node_name", "dane_code"]].copy() # Hago copia del df solo con las columnas que necesito
    df_geo = df_geo[df_geo["dane_code"] != -1]
    df_geo = df_geo.drop_duplicates(subset="node_name")

    ruta_archivo = os.path.join(carpeta, "node_temp.csv")
    df_day = pd.read_csv(ruta_archivo, 
                            usecols=["Timestamp","node_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
                            ) # Leer el archivo CSV y cargarlo en un DataFrame
    df_merged = df_day.merge(df_geo, on="node_name", how='left')

    # Agrupar los datos por 'Timestamp' y codigo DANE de los municipios y sumar los valores
    df_merged = df_merged.groupby(['Timestamp', 'dane_code']).sum(numeric_only=True).reset_index() # Cuando pongo el atributo numeric_omly borra las columnas no numericas
    df_merged["dane_code"] = df_merged["dane_code"].astype(int)

    nueva_ruta = os.path.join(carpeta, "municipio_temp.csv")
    df_merged.to_csv(nueva_ruta, index=False)

    columnas = ["Timestamp","municipio_dane_code","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
    cargar_archivo_postgresql(conn, nueva_ruta, "ran_1h_municipio", "municipio", columnas) # Llamado a función que sube el archivo a la base de datos

    print("Se terminó de agregar los municipios con exito")

def am(conn, carpeta, df_geo):
    print("Iniciando función agregación areas metropolitanas")
    df_geo = df_geo[["node_name", "AM"]].copy() # Hago copia del df solo con las columnas que necesito
    df_geo = df_geo.drop_duplicates(subset="node_name")
    df_geo = df_geo[df_geo["AM"] != "Sin AM"] # Mantener filas donde el valor de la columna AM sea diferente a "Sin AM"


    ruta_archivo = os.path.join(carpeta, "node_temp.csv")
    df_day = pd.read_csv(ruta_archivo, 
                            usecols=["Timestamp","node_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
                            ) # Leer el archivo CSV y cargarlo en un DataFrame
    df_merged = df_day.merge(df_geo, on="node_name", how='left')

    # Agrupar los datos por 'Timestamp' y codigo DANE de los municipios y sumar los valores
    df_merged = df_merged.groupby(['Timestamp', 'AM']).sum(numeric_only=True).reset_index() # Cuando pongo el atributo numeric_omly borra las columnas no numericas

    nueva_ruta = os.path.join(carpeta, "am_temp.csv")
    df_merged.to_csv(nueva_ruta, index=False)

    columnas = ["Timestamp","am_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
    cargar_archivo_postgresql(conn, nueva_ruta, "ran_1h_am", "area_metro", columnas) # Llamado a función que sube el archivo a la base de datos

    print("Se terminó de agregar las areas_metros con exito")

def departamento(conn, carpeta, df_geo):
    print("Iniciando función agregación departamentos")
    df_geo = df_geo[["node_name", "dane_code_dpto"]].copy() # Hago copia del df solo con las columnas que necesito
    df_geo = df_geo[df_geo["dane_code_dpto"] != -1]
    df_geo = df_geo.drop_duplicates(subset="node_name")

    ruta_archivo = os.path.join(carpeta, "node_temp.csv")
    df_day = pd.read_csv(ruta_archivo, 
                            usecols=["Timestamp","node_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
                            ) # Leer el archivo CSV y cargarlo en un DataFrame
    df_merged = df_day.merge(df_geo, on="node_name", how='left')

    # Agrupar los datos por 'Timestamp' y codigo DANE de los municipios y sumar los valores
    df_merged = df_merged.groupby(['Timestamp', 'dane_code_dpto']).sum(numeric_only=True).reset_index() # Cuando pongo el atributo numeric_omly borra las columnas no numericas
    df_merged["dane_code_dpto"] = df_merged["dane_code_dpto"].astype(int)

    nueva_ruta = os.path.join(carpeta, "dpto_temp.csv")
    df_merged.to_csv(nueva_ruta, index=False)

    columnas = ["Timestamp","dpto_dane_code","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
    cargar_archivo_postgresql(conn, nueva_ruta, "ran_1h_departamento", "departamento", columnas) # Llamado a función que sube el archivo a la base de datos

    print("Se terminó de agregar los departamentos con exito")

def regional(conn, carpeta, df_geo):
    print("Iniciando función agregación regional")
    df_geo = df_geo[["node_name", "wom_regional"]].copy() # Hago copia del df solo con las columnas que necesito
    df_geo = df_geo.drop_duplicates(subset="node_name")

    ruta_archivo = os.path.join(carpeta, "node_temp.csv")
    df_day = pd.read_csv(ruta_archivo, 
                            usecols=["Timestamp","node_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
                            ) # Leer el archivo CSV y cargarlo en un DataFrame
    df_merged = df_day.merge(df_geo, on="node_name", how='left')

    # Agrupar los datos por 'Timestamp' y codigo DANE de los municipios y sumar los valores
    df_merged = df_merged.groupby(['Timestamp', 'wom_regional']).sum(numeric_only=True).reset_index() # Cuando pongo el atributo numeric_omly borra las columnas no numericas

    nueva_ruta = os.path.join(carpeta, "regional_temp.csv")
    df_merged.to_csv(nueva_ruta, index=False)

    columnas = ["Timestamp","regional_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
    cargar_archivo_postgresql(conn, nueva_ruta, "ran_1h_regional", "regional", columnas) # Llamado a función que sube el archivo a la base de datos

    print("Se terminó de agregar las regiones con exito")

def total(conn, carpeta):
    print("Iniciando función agregación total de la red")

    ruta_archivo = os.path.join(carpeta, "node_temp.csv")
    df_day = pd.read_csv(ruta_archivo, 
                            usecols=["Timestamp","node_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
                            ) # Leer el archivo CSV y cargarlo en un DataFrame

    # Agrupar los datos por 'Timestamp' y sumar los valores
    df_day = df_day.groupby(['Timestamp']).sum(numeric_only=True).reset_index() # Cuando pongo el atributo numeric_omly borra las columnas no numericas

    nueva_ruta = os.path.join(carpeta, "total_temp.csv")
    df_day.to_csv(nueva_ruta, index=False)

    columnas = ["Timestamp","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.Traffic.ActiveUser.UL.Avg","L.Traffic.ActiveUser.UL.Max","L.Traffic.User.Avg","L.Traffic.User.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)","L.Thrp.Time.DL.RmvLastTTI(ms)"]
    cargar_archivo_postgresql(conn, nueva_ruta, "ran_1h_total", "total", columnas) # Llamado a función que sube el archivo a la base de datos

    print("Se terminó de agregar la totalida de la red con exito")

def area_metro(cell_name):
    # Diccionario de áreas metropolitanas
    areas_metropolitanas = {
        'ARM': 'Armenia AM',
        'CARM': 'Armenia AM',
        'AMB': 'Barranquilla AM',
        'BQL': 'Barranquilla AM',
        'CBQL': 'Barranquilla AM',
        'BTA': 'Bogota AM',
        'CBT': 'Bogota AM',
        'CBTA': 'Bogota AM',
        'AMS': 'Bucaramanga AM',
        'BUC': 'Bucaramanga AM',
        'CBUC': 'Bucaramanga AM',
        'CCLI': 'Cali AM',
        'CLI': 'Cali AM',
        'CAR': 'Cartagena AM',
        'CCAR': 'Cartagena AM',
        'AMC': 'Cucuta AM',
        'CCUC': 'Cucuta AM',
        'CUC': 'Cucuta AM',
        'CMAN': 'Manizales AM',
        'MAN': 'Manizales AM',
        'AMA': 'Medellin AM',
        'CMED': 'Medellin AM',
        'MED': 'Medellin AM',
        'CPER': 'Pereira AM',
        'CRI': 'Pereira AM',
        'PER': 'Pereira AM',
        'AMV': 'Valledupar AM',
        'CVDP': 'Valledupar AM',
        'VDP': 'Valledupar AM'
        }
    
    identificador_ciudad = cell_name.split()[0]  # Asume que el identificador de ciudad es la primera palabra
    return areas_metropolitanas.get(identificador_ciudad, 'Sin AM')

def query_geodata(): # Función para hacer query desde la base de datos de Sergio
    # Conectar a la base de datos PostgreSQL
    conn = psycopg2.connect(**DBcredentials.BD_GEO_PARAMS)

    # Crear un cursor
    cur = conn.cursor()
    query = """SELECT dwh_cell_name_wom, dwh_banda, dwh_sector, dwh_latitud, dwh_longitud, cluster_key, cluster_nombre, dwh_localidad, dwh_dane_cod_localidad, dane_nombre_mpio, dane_code, dane_code_dpto, dane_nombre_dpt, wom_regional 
            FROM bodega_analitica.roaming_cell_dim 
            WHERE dwh_operador_rat = 'WOM 4G' LIMIT 100000"""

    cur.execute(query) # Ejecutar la consulta
    datos = cur.fetchall() # Almacenar todas las filas de la consulta en esta variable
    columnas = [desc[0] for desc in cur.description]  # Obtener los nombres de las columnas

    cur.close()
    conn.close()

    df_geo = pd.DataFrame(datos, columns=columnas) # Creo dataframe

    df_geo = df_geo.dropna(subset="dwh_cell_name_wom")
    # Corrijo la columna que contiene el nombre de las celdas para que cuadre con los nombres de los informes
    df_geo["dwh_cell_name_wom"] = df_geo["dwh_cell_name_wom"].str.upper() # Todo a mayusculas
    df_geo["node_name"] = df_geo["dwh_cell_name_wom"]
    # Concatenar las columnas, reemplazando "B4" con "AWS" cuando sea necesario
    df_geo["dwh_cell_name_wom"] = np.where(df_geo["dwh_banda"] == "B4", # Cuando se cumpla esta condición
                                            df_geo["dwh_cell_name_wom"] + "_AWS_" + df_geo["dwh_sector"].astype(str), # Se aplica este fragmento
                                            df_geo["dwh_cell_name_wom"] + "_" + df_geo["dwh_banda"].astype(str) + "_" + df_geo["dwh_sector"].astype(str)) # Else
    df_geo = df_geo.drop_duplicates(subset=["dwh_cell_name_wom"]) # Elimino los nombres exactamente iguales
    df_geo["sector"] = df_geo["dwh_sector"].apply(lambda x: 1 if x in [1,4,7] else (2 if x in [2,5,8] else (3 if x in [3,6,9] else 4))) # Creación de columna "sector" para logica de agregación por sectores. Se agrupa según el id de sector
    df_geo["sector_name"] = df_geo["node_name"] + ": " + df_geo["sector"].astype(str)

    df_geo['AM'] = df_geo['dwh_cell_name_wom'].apply(area_metro)

    return df_geo

def equilibrar(conn, days, table_name):
    print(f"Iniciando equilibrio de filas para la tabla {table_name}")
    cur = conn.cursor()
    # Definir dia de corte
    cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    print(f"Día de corte {cutoff_date}")
    if table_name == "ran_kpi_cell":
        # Borrar datos anteriores a este dia para la tabla de KPIs
        delete_query = sql.SQL("DELETE FROM {} WHERE \"Date\" < %s").format(sql.Identifier(table_name))
    else:
        # Borrar datos anteriores a este dia para el resto de tablas
        delete_query = sql.SQL("DELETE FROM {} WHERE \"Timestamp\" < %s").format(sql.Identifier(table_name))

    cur.execute(delete_query, (cutoff_date,))
    conn.commit()
    print(f"Filas equilibradas en {table_name}, se conservaron los ultimos {days} días")

def tablas_agregaciones(carpeta):
    # Conectar a la base de datos PostgreSQL
    conn = psycopg2.connect(**DBcredentials.BD_DATA_PARAMS)

    celdas(conn, carpeta) # Función que agrega data de celdas a la BD
    equilibrar(conn, 100, "ran_1h_cell") # Función que asegura que siempre haya 100 días de histórico, poco más de 20 GB

    raw_to_kpi(conn, carpeta) # Función que calcula KPIs y sube datos a la tabla de KPIs
    equilibrar(conn, 6840, "ran_kpi_cell") # 6840 dias son 19 años y menos de 20 GB

    df_geo = query_geodata() # Dataframe a partir del baseline de la BD

    sectores(conn, carpeta, df_geo)
    equilibrar(conn, 210, "ran_1h_sector") # 210 dias serian poco más de 20 GB

    nodos(conn,carpeta)
    equilibrar(conn, 617, "ran_1h_node") # 617 dias serian poco más de 20 GB

    cluster(conn, carpeta, df_geo)
    equilibrar(conn, 5163, "ran_1h_cluster") # 5163 dias serian poco más de 20 GB

    localidad(conn, carpeta, df_geo)
    equilibrar(conn, 6840, "ran_1h_localidad") # 6840 dias son 19 años y menos de 20 GB

    municipio(conn, carpeta, df_geo)
    equilibrar(conn, 6840, "ran_1h_municipio") # 6840 dias son 19 años y menos de 20 GB

    am(conn, carpeta, df_geo)
    equilibrar(conn, 6840, "ran_1h_am") # 6840 dias son 19 años y menos de 20 GB

    departamento(conn, carpeta, df_geo)
    equilibrar(conn, 6840, "ran_1h_departamento") # 6840 dias son 19 años y menos de 20 GB

    regional(conn, carpeta, df_geo)
    equilibrar(conn, 6840, "ran_1h_regional") # 6840 dias son 19 años y menos de 20 GB

    total(conn, carpeta)
    equilibrar(conn, 6840, "ran_1h_total") # 6840 dias son 19 años y menos de 20 GB

    conn.close()


def main():
    carpeta_zip = "C:/Users/roberto.cuervo.WOMCOL/OneDrive - WOM Colombia/Documentos/FTP"
    carpeta_descomprimida = "C:/Users/roberto.cuervo.WOMCOL/OneDrive - WOM Colombia/Documentos/Progra_Tests/Python/RAN_ETL/Temp"

    # Descomprimir ultimo archivo ZIP
    descomprimir_archivos(carpeta_zip, carpeta_descomprimida)

    # Borrar lineas no necesarias
    borrar_encabezado(carpeta_descomprimida)

    # Organizar columnas del archivo
    editar_archivos_csv(carpeta_descomprimida)

    # Cargar datos a PostgreSQL de manera secuencial
    tablas_agregaciones(carpeta_descomprimida)

if __name__ == "__main__":
    main()
