El objetivo del script es poder normalizar la información de una base de datos que viene en formato txt y luego cargarla en una base de datos mysql. EL script principal "carga.py" hace un drop / create de lo que se carga en la bbdd mysql. Tambien hay una alternativa "cargaUpdate.py" que hace un update de lo que se carga en la bbdd mysql.

INSTRUCCIONES:

1- Crear un .env con las credenciales del usuario y los datos de conexión a la bbdd

2- Al final del código en el archivo carga.py, poner la ruta a la carpeta en donde están los archivos .txt

3- Crear un entorno virtual con el comando "python -m venv NombreDelVenv"

4- Instalar las librerias con el comando "pip install -r requirements.txt"

5- Ejecutar el comando "python carga.py"
