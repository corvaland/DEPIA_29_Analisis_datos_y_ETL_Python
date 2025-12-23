# pipeline.py

import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine
import logging
import sys
import random
from datetime import datetime, timedelta
import random
from config import DATABASE_CONFIG, DATABASE_TRANSACT_CONFIG, DATABASE_DW_CONFIG, CSV_FILES, LOG_FILE

# Configuración de Logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_db_engine(config):
    """
    Crea una conexión de motor a la base de datos MySQL.
    """
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}",
            echo=False
        )
        logging.info("Conexión a la base de datos establecida correctamente.")
        return engine
    except Exception as e:
        logging.error(f"Error al conectar a la base de datos: {e}")
        sys.exit(1)


def read_csv(file_path, separ):
    """
    Lee un archivo CSV y devuelve un DataFrame de pandas.
    """
    try:
        df = pd.read_csv(file_path, sep= separ)
        logging.info(f"Archivo {file_path} leído exitosamente.")
        return df
    except Exception as e:
        logging.error(f"Error al leer el archivo {file_path}: {e}")
        sys.exit(1)

def gen_rating():
    # Generar un número aleatorio entre 0 y 5 con 1 solo decimal
    numero_aleatorio = round(random.uniform(0, 5), 1)
    # Mostrar el número aleatorio
    return numero_aleatorio

def gen_timestamp():
    # Generar un timestamp aleatorio dentro de un rango específico
    start_date = datetime(2024, 1, 15)
    end_date = datetime(2024, 4, 6)

    # Calcular un valor aleatorio entre start_date y end_date
    random_date = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))

    # Mostrar el timestamp aleatorio
    return random_date

def transform_dimMovie():

    """
    Realiza transformaciones específicas en el DataFrame de dimMovie.
    """

    engine = create_db_engine(DATABASE_TRANSACT_CONFIG)

    conn = engine.connect()
    query = """
            SELECT 
                movie.movieID as movieID, movie.movieTitle as title, movie.releaseDate as releaseDate, 
                gender.name as gender , person.name as participantName, participant.participantRole as roleparticipant 
            FROM movie 
            INNER JOIN participant 
            ON movie.movieID=participant.movieID
            INNER JOIN person
            ON person.personID = participant.personID
            INNER JOIN movie_gender 
            ON movie.movieID = movie_gender.movieID
            INNER JOIN gender 
            ON movie_gender.genderID = gender.genderID
            """
    movies_data = pd.read_sql(query, con=conn)

    movies_data["movieID"] = movies_data["movieID"].astype('int')

    movies_awards = read_csv(CSV_FILES['awards_movie'], ',')

    movies_awards["movieID"] = movies_awards["movieID"].astype('int')

    movies_awards.rename(columns={"Aware": "Award"}, inplace = True)

    movie_data_final = pd.merge(movies_data, movies_awards,
                             left_on = "movieID", right_on="movieID")

    # engine = create_db_engine(DATABASE_CONFIG)

    # conn_dw =engine.connect()

    movie_data_final = movie_data_final.rename(columns={"releaseDate": "releaseMovie", "Award": "awardMovie"})


    movie_data_final = movie_data_final.drop(columns=['IdAward'])

    # movie_data_final.to_sql('dimMovie', conn_dw, if_exists='append', index= False)

    return movie_data_final

def transform_dimUser():
    """
    Realiza transformaciones específicas en el DataFrame de dimUser.
    """
    users = read_csv(CSV_FILES['users'], '|')

    users = users.rename(columns= {"idUser": "userID"})

    # engine = create_db_engine(DATABASE_CONFIG)

    # conn_dw =engine.connect()

    # users.to_sql("dimUser", conn_dw, if_exists='append', index= False)

    return users

def transform_FactWatchs(users_df, movies_df):

    """
    Realiza transformaciones específicas en el DataFrame de FactWatchs.
    Genera datos de visualizaciones mediante un producto cruzado de usuarios y películas.
    """
    users_id = users_df["userID"]
    movies_id = movies_df["movieID"]

    watchs_data=pd.merge(users_id,movies_id, how="cross")

    watchs_data["rating"] = watchs_data["movieID"].apply(lambda x: gen_rating())

    watchs_data["timestamp"] = watchs_data["userID"].apply(lambda x: gen_timestamp())

    # engine = create_db_engine(DATABASE_CONFIG)

    # conn_dw =engine.connect()

    # watchs_data.to_sql("FactWatchs", conn_dw, if_exists='append', index=False)

    return watchs_data


def load_data(engine, table_name, df, if_exists='append'):
    """
    Carga un DataFrame de pandas a una tabla de MySQL.
    """
    try:
        df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False)
        logging.info(f"Datos cargados exitosamente en la tabla {table_name}.")
    except Exception as e:
        logging.error(f"Error al cargar datos en la tabla {table_name}: {e}")
        sys.exit(1)

def main():
    # Crear conexión a la base de datos
    engine = create_db_engine(DATABASE_CONFIG)
    
    # Orden de carga para respetar las dependencias de claves foráneas
    load_order = ['dimMovie', 'dimUser', 'FactWatchs']
    
    # Diccionario para almacenar DataFrames transformados
    dataframes = {}
    
    # Leer y transformar cada tabla
    # 1. dimMovie
    dimMovie_df = transform_dimMovie()
    dataframes['dimMovie'] = dimMovie_df
    
    # 2. dimUser
    dimUser_df = transform_dimUser()
    dataframes['dimUser'] = dimUser_df
    
    # 3. FactWatchs
    FactWatchs_df = transform_FactWatchs(dimUser_df, dimMovie_df)
    dataframes['FactWatchs'] = FactWatchs_df
    
    
    # Cargar datos en MySQL en el orden correcto
    for table in load_order:
        load_data(engine, table, dataframes[table], if_exists='append')
    
    logging.info("Pipeline de datos ejecutado exitosamente.")

if __name__ == "__main__":
    main()
