# config.py

# Configuración para la base de datos transaccional (origen)
DATABASE_TRANSACT_CONFIG = {
    'host': 'localhost',
    'port': 3310,
    'user': 'root',
    'password': 'root',
    'database': 'db_movies_netflix_transact'
}

# Configuración para el Data Warehouse (destino)
DATABASE_DW_CONFIG = {
    'host': 'localhost',
    'port': 3310,
    'user': 'root',
    'password': 'root',
    'database': 'dw_netflix'
}

# Para mantener compatibilidad con código existente
DATABASE_CONFIG = DATABASE_DW_CONFIG

CSV_FILES = {
    'awards_movie': 'data/Awards_movie.csv',
    'awards_participant': 'data/Awards_participant.csv',
    'users': 'data/users.csv'
}

LOG_FILE = 'logs/pipeline.log'