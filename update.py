import os
import re
import petl as etl
import pandas as pd
from sqlalchemy import create_engine, text, inspect, Table, Column, Integer, String, MetaData, bindparam
from sqlalchemy.exc import SQLAlchemyError, DBAPIError
from dotenv import load_dotenv

# Cargar variables de entorno desde un archivo .env
load_dotenv(override=True)

# Configuración de la base de datos y ruta de archivos
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
PATH = os.getenv("PATH")

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
    "CRUCE_PADRON_ DEUDORES_BCRA": "deudores_bcra",  # Tiene un espacio después de PADRON_ (así viene el archivo)
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

# Diccionario global para acumular estadísticas por tabla.
# Se registrará el total de filas “nuevas” (que se insertaron) y el total de filas que ya existían.
estadisticas_tablas = {}

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

    # Resumen final de archivos procesados
    print("\nResumen del proceso:")
    print(f"Archivos totales cargados ({len(archivos_cargados)}):")
    for archivo in archivos_cargados:
        print(f" - {archivo}")

    print(f"\nArchivos cargados sin columna 'ID_PERSONA' ({len(archivos_sin_id_persona)}):")
    for archivo in archivos_sin_id_persona:
        print(f" - {archivo}")

    # Imprimir resumen final de filas nuevas y ya existentes por tabla
    if estadisticas_tablas:
        print("\nRegistros nuevos / ya existentes:")
        for tabla, stats in estadisticas_tablas.items():
            print(f"Tabla '{tabla.upper()}': {stats['new']} filas nuevas / {stats['existing']} filas ya existentes")

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

    # Función interna para ajustar cada fila
    def ajustar_fila(row_list):
        if len(row_list) > len(header):
            return row_list[:len(header)]
        elif len(row_list) < len(header):
            return row_list + [''] * (len(header) - len(row_list))
        else:
            return row_list

    # Procesar filas (saltando el encabezado)
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

    # Guardar el archivo limpio en la misma carpeta con sufijo "_limpio"
    dir_archivo = os.path.dirname(ruta_archivo)
    base_archivo = os.path.basename(ruta_archivo)
    nombre_limpio = re.sub(r'\.TXT$', '_limpio.TXT', base_archivo)
    ruta_limpio = os.path.join(dir_archivo, nombre_limpio)
    etl.tocsv(tabla, ruta_limpio, delimiter="\t", encoding='utf-8')
    print(f"Archivo limpio guardado en: {ruta_limpio}")

    # Convertir la tabla limpia a un DataFrame de pandas
    df = etl.todataframe(tabla)

    # Normalizar los nombres de las columnas: quitar espacios y pasar a minúsculas
    df.columns = df.columns.str.strip().str.lower()
    print("Columnas en el DataFrame:", df.columns)
    print("Número total de filas leídas:", len(df))

    # Si no existe la columna 'id_persona', se registra el archivo en archivos_sin_id_persona
    if 'id_persona' not in df.columns:
        print(f"El archivo no contiene la columna 'ID_PERSONA'.")
        archivos_sin_id_persona.append(archivo)

    # Aplicar strip a columnas de tipo object
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()

    # Cargar datos en la base de datos
    cargar_datos_en_bd(df, nombre_tabla)
    return True

def convertir_a_formato_tabla(df, nombre_tabla):
    # Obtener el esquema de la tabla en la BD
    inspector = inspect(engine)
    columnas_esquema = inspector.get_columns(nombre_tabla)
    for columna in columnas_esquema:
        col_name = columna['name'].lower()
        col_type = columna['type']
        if col_name in df.columns:
            if 'INT' in str(col_type) and df[col_name].dtype == 'object':
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0).astype(int)
            elif 'VARCHAR' in str(col_type) or 'TEXT' in str(col_type):
                df[col_name] = df[col_name].astype(str)
    return df

def cargar_datos_en_bd(df, nombre_tabla, lot_size=1000):
    global estadisticas_tablas
    connection = None
    try:
        connection = engine.connect()
        trans = connection.begin()

        # Convertir nombres de columnas a minúsculas
        df.columns = df.columns.str.lower()

        inspector = inspect(engine)
        # Verificar si la tabla ya existe (para luego realizar el pre-check)
        tabla_existente = inspector.has_table(nombre_tabla)
        if not tabla_existente:
            metadata = MetaData()
            # Determinar la clave primaria según la regla:
            # - personas_domicilios: usar id_domicilio si existe, sino codigo.
            # - para las demás: si existe id_persona se usa; sino codigo.
            if nombre_tabla == "personas_domicilios":
                pk = "id_domicilio" if ("id_domicilio" in df.columns or "id_domicilios" in df.columns) else "codigo"
            elif "id_persona" in df.columns:
                pk = "id_persona"
            else:
                pk = "codigo"

            columns = []
            for name in df.columns:
                if name == pk:
                    # Marcar como PRIMARY KEY y UNIQUE según lo requerido
                    columns.append(Column(name, Integer, primary_key=True, unique=True))
                else:
                    columns.append(Column(name, String(255)))
            table = Table(nombre_tabla, metadata, *columns)
            metadata.create_all(engine)
            print(f"Tabla {nombre_tabla} creada.")

        # Convertir DataFrame al formato correcto
        df = convertir_a_formato_tabla(df, nombre_tabla)

        # Inicializar estadísticas para la tabla si no están registradas aún
        if nombre_tabla not in estadisticas_tablas:
            estadisticas_tablas[nombre_tabla] = {"new": 0, "existing": 0}

        # Procesar en lotes
        for i in range(0, len(df), lot_size):
            batch = df.iloc[i:i + lot_size]
            valores = batch.to_dict(orient='records')
            if not valores:
                continue

            # Determinar la clave a usar para el ON DUPLICATE KEY UPDATE (la misma que la PK)
            if nombre_tabla == "personas_domicilios" and "id_domicilio" in batch.columns:
                clave_update = "id_domicilio"
            elif "id_persona" in batch.columns:
                clave_update = "id_persona"
            else:
                clave_update = "codigo"

            # --- Pre-check: Si la tabla ya existía, contar cuántas filas de este batch ya están en BD ---
            if tabla_existente:
                # Extraer los valores de la clave para el batch
                pk_values = [row[clave_update] for row in valores if row.get(clave_update) not in (None, '')]
                if pk_values:
                    # Usar bindparam con expanding para IN clause
                    sql_select = text(f"SELECT COUNT(*) FROM {nombre_tabla} WHERE {clave_update} IN :pk_list").bindparams(bindparam("pk_list", expanding=True))
                    result_select = connection.execute(sql_select, {"pk_list": tuple(pk_values)})
                    count_existing = result_select.scalar()  # cantidad que ya existían
                else:
                    count_existing = 0
            else:
                count_existing = 0

            new_count = len(batch) - count_existing
            # Acumular los totales
            estadisticas_tablas[nombre_tabla]["new"] += new_count
            estadisticas_tablas[nombre_tabla]["existing"] += count_existing

            # Construir la consulta INSERT ... ON DUPLICATE KEY UPDATE
            columnas = list(batch.columns)
            columnas_sql = ", ".join(columnas)
            valores_sql = ", ".join([f":{col}" for col in columnas])
            # Se excluye la columna clave (no se actualiza)
            actualizaciones = ", ".join([f"{col} = VALUES({col})" for col in columnas if col != clave_update])
            sql = text(f"""
                INSERT INTO {nombre_tabla} ({columnas_sql})
                VALUES ({valores_sql})
                ON DUPLICATE KEY UPDATE {actualizaciones};
            """)
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
procesar_y_cargar_archivos(PATH)
