from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
import json
import pandas as pd
from sqlalchemy import create_engine

def transfer_mysql_schema_to_postgres(
    mysql_conn_id: str,
    postgres_conn_id: str,
    source_schema_name: str,
    target_schema_name: str,
    chunksize: int = 10000,
    **context
):
    """
    Transfers all tables from a MySQL schema to a Postgres schema.

    Args:
        mysql_conn_id: Airflow connection ID for MySQL.
        postgres_conn_id: Airflow connection ID for Postgres.
        schema_name: The name of the MySQL schema to transfer.
        chunksize: Number of rows to fetch and insert at a time.
    """

    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    mysql_engine = create_engine(mysql_hook.get_uri())
    postgres_engine = create_engine(postgres_hook.get_uri())

    try:
        with postgres_engine.connect() as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema_name}")
            #conn.commit()
        print(f"Schema {target_schema_name} ensured to exist.")
    except Exception as e:
        print("Skipping schema creation",e)


    # Get all tables in the schema
    tables = mysql_hook.get_records(f"SHOW TABLES IN {source_schema_name}")

    if not tables:
        print(f"No tables found in schema: {source_schema_name}")
        return

    for table_name_tuple in tables:
        table_name = table_name_tuple[0]
        print(f"Transferring table: {table_name}")
        try:
            #Read data in chunks
            for df_chunk in pd.read_sql_table(
                table_name, mysql_engine, chunksize=chunksize, schema=source_schema_name
            ):

                # Use pandas to_sql for efficient bulk insert (with method='multi' for postgres)
                df_chunk.to_sql(
                    table_name,
                    postgres_engine,
                    schema=target_schema_name, # or specify a target schema
                    if_exists="replace", # or 'replace' if needed
                    index=False,
                    method='multi'
                )
            print(f"Table {table_name} transferred successfully")
        except Exception as e:
            print(f"Error transferring table {table_name}: {e}")

def transfer_postgres_schema_to_another_schema(
    source_postgres_conn_id: str,
    target_postgres_conn_id: str,
    source_schema_name: str,
    target_schema_name: str,
    chunksize: int = 10000,
    **context
):
    """
    Transfers all tables from one Postgres schema to another Postgres schema.

    Args:
        source_postgres_conn_id: Airflow connection ID for the source Postgres database.
        target_postgres_conn_id: Airflow connection ID for the target Postgres database.
        source_schema_name: The name of the source Postgres schema.
        target_schema_name: The name of the target Postgres schema.
        chunksize: Number of rows to fetch and insert at a time.
    """

    source_hook = PostgresHook(postgres_conn_id=source_postgres_conn_id)
    target_hook = PostgresHook(postgres_conn_id=target_postgres_conn_id)

    source_engine = create_engine(source_hook.get_uri())
    target_engine = create_engine(target_hook.get_uri())

    try:
        with target_engine.connect() as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema_name}")
            #conn.commit()
    except Exception as e:
        print("Skipping schema creation",e)
    print(f"Schema {target_schema_name} ensured to exist.")

    # Get all tables in the source schema
    tables = source_hook.get_records(
        f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{source_schema_name}'
        """
    )

    if not tables:
        print(f"No tables found in schema: {source_schema_name}")
        return

    for table_name_tuple in tables:
        table_name = table_name_tuple[0]
        print(f"Transferring table: {table_name}")
        try:
            for df_chunk in pd.read_sql_table(
                table_name, source_engine, chunksize=chunksize, schema=source_schema_name
            ):
                df_chunk.to_sql(
                    table_name,
                    target_engine,
                    schema=target_schema_name,
                    if_exists="replace",  # Use 'replace' to overwrite
                    index=False,
                    method="multi",  # Important for Postgres performance
                )
            print(f"Table {table_name} transferred successfully")
        except Exception as e:
            print(f"Error transferring table {table_name}: {e}")

def transfer_mongodb_collections_to_postgres(
    mongo_conn_id: str,
    postgres_conn_id: str,
    mongo_database: str,
    target_schema_name:str,
    chunksize: int = 10000,
):
    """
    Transfers all collections from a MongoDB database to Postgres tables (one table per collection).

    Args:
        mongo_conn_id: Airflow connection ID for MongoDB.
        postgres_conn_id: Airflow connection ID for Postgres.
        mongo_database: The name of the MongoDB database.
        chunksize: Number of documents to fetch and insert at a time.
    """

    mongo_hook = MongoHook(conn_id=mongo_conn_id)
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    postgres_engine = create_engine(postgres_hook.get_uri())

    try:
        with postgres_engine.connect() as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema_name}")
            #conn.commit()
    except Exception as e:
        print("Skipping schema creation",e)
    print(f"Schema {target_schema_name} ensured to exist.")

    mongo_client = mongo_hook.get_conn()
    db = mongo_client[mongo_database]

    collection_names = db.list_collection_names()

    for mongo_collection in collection_names:
        postgres_table = mongo_collection  # Use collection name as table name
        print(f"Transferring collection: {mongo_collection} to table: {postgres_table}")
        try:
            collection = db[mongo_collection]
            cursor = collection.find()
            total_docs = collection.count_documents({})
            processed_docs = 0

            while processed_docs < total_docs:
                documents = list(cursor.limit(chunksize))
                if not documents:
                    break

                df = pd.DataFrame(documents)

                if "_id" in df.columns:
                    df["_id"] = df["_id"].astype(str)

                for col in df.columns:
                    if df[col].dtype == 'object':
                        try:
                            df[col] = df[col].apply(json.dumps)
                        except TypeError:
                            df[col] = df[col].astype(str)

                df.to_sql(
                    postgres_table,
                    postgres_engine,
                    schema=target_schema_name,  # Specify your schema
                    if_exists="replace",  # Or 'replace'
                    index=False,
                    method="multi",
                )
                processed_docs += len(documents)
                print(f"Processed {processed_docs}/{total_docs} documents for {postgres_table}")

        except Exception as e:
            print(f"Error transferring collection {mongo_collection}: {e}")
        finally:
            mongo_client.close()