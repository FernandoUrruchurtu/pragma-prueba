[![Python](https://img.shields.io/badge/Python-3.12+-yellow?style=for-the-badge&logo=python&logoColor=white&labelColor=101010)](https://python.org)  [![Docker](https://img.shields.io/badge/docker-257bd6?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)


# Data Rangers - Prueba De Ingenieria de Datos  

En este repo se trabaja con docker-compose para replicar facilmente el ejercicio, en este caso se crea un contenedor con una imagen de postgres. Como pre-requisito es necesario que el puerto 5432 se encuentre libre. Para verificar, se debe probar con la libreria ```netstat```.

## Setup  
1. Clonar el repositorio  
    ```bash
    git clone git@github.com:FernandoUrruchurtu/Prueba_Pragma.git
    ```  
2. Crear entorno virtual. El framework utilizado es Python 3.12+  
    ```bash
    python3 -m venv .venv
    ```  
3. Activar entorno virtual  

    MacOS/Linux (bash):  
    ```bash
    source .venv/bin/activate
    ``` 
    Windows (cmd):   
    ```windows
    .venv\Scripts\activate
    ```  
4. Instalar paquetes necesarios
    ```bash
    pip install -r requirements.txt
    ```  
5. Configuracion base de datos (postgres)  

    5.1. Utilizando Docker  

    Si se tiene instalado docker y docker compose en la maquina ejecutar desde la carpeta donde se realizo el pull  
    ```bash
    docker compose up -d
    ```  
    5.2. Utilizando otras bases de datos modificar en archivo db.env  
        - ```DATABASE_NAME``` Base de datos a utilizar   
        - ```DB_HOST``` Host donde esta alojada la base de datos  
        - ```DB_USER``` Usuario con permisos de crear, insertar y seleccionar  
        - ```DB_PASSWORD``` Password del ```DB_USER```  
  
6. Ejecutar archivo en un jupyter notebook ```dataRangers.ipynb```. 

[![Web](https://img.shields.io/badge/GitHub-FernandoUrruchurtu-14a1f0?style=for-the-badge&logo=github&logoColor=white&labelColor=101010)](https://github.com/FernandoUrruchurtu)