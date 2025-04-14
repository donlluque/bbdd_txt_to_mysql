El objetivo del script es poder normalizar la información de una base de datos que viene en formato txt y luego cargarla en una base de datos mysql. EL script principal "carga.py" hace un drop / create de lo que se carga en la bbdd mysql. Tambien hay una alternativa "cargaUpdate.py" que hace un update de lo que se carga en la bbdd mysql.

INSTRUCCIONES:

1- Crear un .env con las credenciales del usuario y los datos de conexión a la bbdd y la ruta desde donde se toman los archivos .txt

2- Crear un entorno virtual con el comando "python -m venv NombreDelVenv"

3- Instalar las librerias con el comando "pip install -r requirements.txt"

4- Ejecutar el comando "python carga.py"

5- Opcionalmente se puede utilizar el comando "python update.py" si lo que se quiere es realizar una actualización de información que ya existe en la bbdd para que solo actualice las filas en donde se repite el id_persona o id_domicilio
