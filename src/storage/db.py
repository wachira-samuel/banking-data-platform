import psycopg2


def connect_db():

    try:
        conn = psycopg2.connect(
            host="localhost",
            database="banking_db",
            user="postgres",
            password="12345",
            port="5432"
        )

        return conn

    except Exception as e:
        print(f"Database connection error: {e}")
        return None
