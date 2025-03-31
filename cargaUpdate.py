import os
import re
import petl as etl
import pandas as pd
from sqlalchemy import create_engine, text, inspect, Table, Column, Integer, String, MetaData
from sqlalchemy.exc import SQLAlchemyError, DBAPIError
from dotenv import load_dotenv

# Cargar variables de entorno desde un archivo .env
load_dotenv(override=True)

# Configuración de la base de datos
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

# Crear conexión a la base de datos
engine = create_engine(f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}')

# Mapeo de nombres de archivos a nombres de tablas (sin el número)
archivo_a_tabla = {
    "B002537_": "data_general",
    "CRUCE_AERONAVES": "aeronaves",
    "CRUCE_ASIGNACIONES FAMILIARES": "asignaciones_familiares",
    "CRUCE_DATOS_IDENTIFICACION_PF": "datos_identificacion",
    "CRUCE_DESEMPLEO": "desempleo",
    "CRUCE_EMBARCACIONES": "embarcaciones",
    "CRUCE_EMPLEO_DEPENDIENTE": "empleo_dependiente",
    "CRUCE_EMPLEO_INDEPENDIENTE": "empleo_independiente",
    "CRUCE_FALLECIDOS": "fallecidos",
    "CRUCE_INMUEBLES": "inmuebles",
    "CRUCE_JUBILACIONES_PENSIONES": "jubilaciones_pensiones",
    "CRUCE_OBRAS SOCIALES FULL": "obras_sociales",
    "CRUCE_PADRON_ DEUDORES_BCRA": "deudores_bcra", # Tiene un espacio después de PADRON_ (asi viene el archivo)
    "CRUCE_PADRON_AUTOMOTORES": "automotores",
    "CRUCE_PERSONAS_DOMICILIOS": "personas_domicilios",
    "CRUCE_PERSONAS_JURIDICAS": "personas_juridicas",
    "CRUCE_PNC SIN RUB": "pensiones_no_contributivas",
    "CRUCE_RUBPS": "programas_sociales",
    "CRUCE_TCA_100": "tipo_de_documento",
    "CRUCE_TCA_200": "sexo",
    "CRUCE_TCA_5375": "fallecidos_oficina_seccional",
    "CRUCE_TCA_5407": "programas_sociales_tipo_prestacion",
    "CRUCE_TCA_5411": "bases"
}

# Lista de codificaciones conocidas
CODIFICACIONES_CONOCIDAS = ['windows-1252', 'utf-8', 'ISO-8859-1', 'ascii']

def procesar_y_cargar_archivos(carpeta):
    archivos_cargados = []
    archivos_sin_id_persona = []

    for archivo in os.listdir(carpeta):
        # Utilizar expresiones regulares para identificar el patrón en el nombre del archivo
        match_cruce = re.match(r"CRUCE_\d+_(.+)\.TXT", archivo)
        match_b = re.match(r"B002537_\d+\.TXT", archivo)

        if match_cruce:
            # Construir la clave sin el número para archivos CRUCE_
            clave = f"CRUCE_{match_cruce.group(1)}"
            if clave in archivo_a_tabla:
                ruta_archivo = os.path.join(carpeta, archivo)
                print(f"\nPROCESANDO ARCHIVO: {archivo}")
                if procesar_archivo(archivo, ruta_archivo, archivo_a_tabla[clave], archivos_sin_id_persona):
                    archivos_cargados.append(archivo)
                else:
                    archivos_cargados.append(archivo)

        elif match_b:
            # Clave fija para archivos B002537_
            clave = "B002537_"
            if clave in archivo_a_tabla:
                ruta_archivo = os.path.join(carpeta, archivo)
                print(f"PROCESANDO ARCHIVO: {archivo}")
                if procesar_archivo(archivo, ruta_archivo, archivo_a_tabla[clave], archivos_sin_id_persona):
                    archivos_cargados.append(archivo)
                else:
                    archivos_cargados.append(archivo)

    # Resumen final
    print("\nResumen del proceso:")
    print(f"Archivos totales cargados ({len(archivos_cargados)}):")
    for archivo in archivos_cargados:
        print(f" - {archivo}")

    print(f"\nArchivos cargados sin columna 'ID_PERSONA' ({len(archivos_sin_id_persona)}):")
    for archivo in archivos_sin_id_persona:
        print(f" - {archivo}")

def procesar_archivo(archivo, ruta_archivo, nombre_tabla, archivos_sin_id_persona):
    # Intentar leer el archivo con diferentes codificaciones conocidas
    for encoding in CODIFICACIONES_CONOCIDAS:
        try:
            with open(ruta_archivo, 'r', encoding=encoding) as f:
                lines = f.read().splitlines()
            print(f"Codificación detectada: {encoding}")
            break
        except (UnicodeDecodeError, ValueError):
            continue
    else:
        print(f"\nError: El archivo {archivo} está codificado con una codificación no conocida.")
        return False

    if not lines:
        print(f"\nEl archivo {archivo} está vacío.")
        return False

    # Leer el encabezado y determinar la cantidad de columnas
    header_line = lines[0].strip()
    header = header_line.split("\t")

    # Función para ajustar cada fila: si tiene más columnas se recorta, si tiene menos se rellena
    def ajustar_fila(row_list):
        if len(row_list) > len(header):
            return row_list[:len(header)]
        elif len(row_list) < len(header):
            return row_list + [''] * (len(header) - len(row_list))
        else:
            return row_list

    # Procesar las filas (sin incluir la primera que es el encabezado)
    rows = []
    for line in lines:
        if line.strip() == "":
            rows.append([''] * len(header))
        else:
            row = line.split("\t")
            row = ajustar_fila(row)
            rows.append(row)

    # Crear la tabla PETL a partir de las filas procesadas
    tabla = etl.wrap(rows).setheader(header)

    # Guardar el archivo limpio en la misma carpeta, con sufijo "_limpio"
    dir_archivo = os.path.dirname(ruta_archivo)
    base_archivo = os.path.basename(ruta_archivo)
    nombre_limpio = re.sub(r'\.TXT$', '_limpio.TXT', base_archivo)
    ruta_limpio = os.path.join(dir_archivo, nombre_limpio)
    etl.tocsv(tabla, ruta_limpio, delimiter="\t", encoding='utf-8')
    print(f"Archivo limpio guardado en: {ruta_limpio}")

    # Convertir la tabla limpia a un DataFrame de pandas
    df = etl.todataframe(tabla)

    # Normalizar los nombres de las columnas: quitar espacios y pasar a mayusculas
    df.columns = df.columns.str.strip().str.lower()
    print("Columnas en el DataFrame:", df.columns)
    print("Número total de filas leídas:", len(df))

    # Verificar si la columna 'ID_PERSONA' existe
    if 'ID_PERSONA' not in df.columns:
        print(f"El archivo no contiene la columna 'ID_PERSONA'.")
        archivos_sin_id_persona.append(archivo)

    # Procesar el DataFrame: aplicar strip solo a columnas de tipo object
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()

    # Cargar los datos en la base de datos
    cargar_datos_en_bd(df, nombre_tabla)
    return True

def convertir_a_formato_tabla(df, nombre_tabla):
    # Obtener el esquema de la tabla desde la base de datos
    inspector = inspect(engine)
    columnas_esquema = inspector.get_columns(nombre_tabla)

    # Convertir los datos en el DataFrame al tipo correcto según el esquema
    for columna in columnas_esquema:
        col_name = columna['name'].lower()
        col_type = columna['type']
        if col_name in df.columns:
            if 'INT' in str(col_type) and df[col_name].dtype == 'object':
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0).astype(int)
            elif 'VARCHAR' in str(col_type) or 'TEXT' in str(col_type):
                df[col_name] = df[col_name].astype(str)
            # Agrega más conversiones según sea necesario
    return df

def cargar_datos_en_bd(df, nombre_tabla, lot_size=1000):
    connection = None
    try:
        connection = engine.connect()
        trans = connection.begin()

        # Convertir los nombres de las columnas a mayúsculas
        df.columns = df.columns.str.lower()

        # Verificar si la tabla existe
        inspector = inspect(engine)
        if not inspector.has_table(nombre_tabla):
            metadata = MetaData()
            columns = [
                Column(name, Integer, primary_key=False, unique=False) if name == 'ID_PERSONA' else Column(name, String(255))
                for name in df.columns
            ]
            table = Table(nombre_tabla, metadata, *columns)
            metadata.create_all(engine)
            print(f"Tabla {nombre_tabla} creada.")

        # Convertir DataFrame al formato correcto
        df = convertir_a_formato_tabla(df, nombre_tabla)

        # Insertar o actualizar en lotes
        for i in range(0, len(df), lot_size):
            batch = df.iloc[i:i + lot_size]
            valores = batch.to_dict(orient='records')

            # Construir la consulta SQL
            columnas = ", ".join(batch.columns)
            valores_sql = ", ".join([f":{col}" for col in batch.columns])
            actualizaciones = ", ".join([f"{col} = VALUES({col})" for col in batch.columns if col != "ID_PERSONA"])

            sql = text(f"""
                INSERT INTO {nombre_tabla} ({columnas})
                VALUES ({valores_sql})
                ON DUPLICATE KEY UPDATE {actualizaciones};
            """)

            # Ejecutar la inserción con actualización
            connection.execute(sql, valores)

        trans.commit()
        print(f"Datos cargados en la tabla '{nombre_tabla.upper()}'.")
    
    except SQLAlchemyError as e:
        if trans:
            trans.rollback()
        print(f"Error al insertar datos en la tabla '{nombre_tabla.upper()}': {e}")
    
    finally:
        if connection:
            connection.close()

# Ejecutar el proceso
procesar_y_cargar_archivos('C:/Users/luis.luque/Desktop/20250224135111 deportes/resultado')
