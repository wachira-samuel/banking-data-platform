from storage.db import connect_db


def create_table():

    conn = connect_db()

    if conn is None:
        print("Database unavailable")
        return

    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transaction_analytics (

            id SERIAL PRIMARY KEY,

            transaction_id VARCHAR(50),

            account_id BIGINT,

            amount DECIMAL(12,2),

            risk_score INTEGER,

            is_fraud BOOLEAN,

            timestamp TIMESTAMP
        )
    """)

    conn.commit()

    cursor.close()
    conn.close()

    print("transaction_analytics table created")


if __name__ == "__main__":
    create_table()
