#CONEXEÇÕES COM OS BANCOS DE DADOS

#BIBLIOTECAS
from sqlalchemy import create_engine
from pymongo import MongoClient
import os
import pandas as pd


# %% 
def get_postgres_engine():
    """Cria e retorna uma conexão com o banco de dados PostgreSQL."""
    return create_engine(os.getenv("POSTGRES_URL"))

# %% 
def get_mariadb_engine():
    """Cria e retorna uma conexão com o banco de dados MariaDB."""
    return create_engine(os.getenv("MARIADB_URL"))

# %% 
def get_mongo_client():
    """Cria e retorna um cliente para o MongoDB."""
    return MongoClient(os.getenv("MONGODB_URL"))

# %% 
def test_database_SQL_connection(engine, query):
    """TESTA A CONEXÃO EXECUTANDO UMA QUERY."""
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        return None