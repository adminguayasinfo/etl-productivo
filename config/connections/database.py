import os
from typing import Optional, List
from contextlib import contextmanager
from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv

load_dotenv()


class DatabaseConnection:
    def __init__(self):
        self.host = os.getenv('OLTP_DB_HOST')
        self.port = os.getenv('OLTP_DB_PORT')
        self.database = os.getenv('OLTP_DB_NAME')
        self.user = os.getenv('OLTP_DB_USER')
        self.password = os.getenv('OLTP_DB_PASSWORD')
        
        
        self.connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        
        self.engine = None
        self.SessionLocal = None
        
    def init_engine(self, echo: bool = False):
        if not self.engine:
            self.engine = create_engine(
                self.connection_string,
                echo=echo,
                poolclass=NullPool
            )
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )
        return self.engine
    
    @contextmanager
    def get_session(self) -> Session:
        if not self.SessionLocal:
            self.init_engine()
            
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def execute_query(self, query: str, params: Optional[dict] = None):
        if not self.engine:
            self.init_engine()
            
        with self.engine.connect() as connection:
            result = connection.execute(text(query), params or {})
            # Convertir el resultado a una lista antes de cerrar la conexión
            rows = result.fetchall() if result.returns_rows else []
            connection.commit()
            return rows
    
    def test_connection(self) -> bool:
        try:
            if not self.engine:
                self.init_engine()
            
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✓ Conexión exitosa a la base de datos")
            return True
        except Exception as e:
            print(f"✗ Error de conexión: {str(e)}")
            return False
    
    def create_schemas(self, schemas: List[str] = None):
        """Create database schemas if they don't exist."""
        if schemas is None:
            schemas = ['etl-productivo']
        
        if not self.engine:
            self.init_engine()
        
        with self.engine.connect() as conn:
            for schema in schemas:
                try:
                    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
                    conn.commit()
                    print(f"✓ Schema '{schema}' creado o ya existe")
                except Exception as e:
                    print(f"✗ Error creando schema '{schema}': {str(e)}")
                    conn.rollback()
    
    def create_all_tables(self, base):
        """Create all tables from SQLAlchemy models."""
        if not self.engine:
            self.init_engine()
        
        # Primero crear los schemas
        self.create_schemas()
        
        # Luego crear las tablas
        base.metadata.create_all(bind=self.engine)
        print("✓ Todas las tablas han sido creadas")
    
    def get_table_info(self, schema: str = None):
        """Get information about tables in a specific schema or all schemas."""
        if not self.engine:
            self.init_engine()
        
        query = """
        SELECT 
            table_schema,
            table_name,
            table_type
        FROM information_schema.tables
        WHERE table_schema = 'etl-productivo'
        """
        
        if schema:
            query += f" AND table_schema = '{schema}'"
        
        query += " ORDER BY table_schema, table_name"
        
        result = self.execute_query(query)
        return result


db_connection = DatabaseConnection()