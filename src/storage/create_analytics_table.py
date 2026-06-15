from storage import connect_db


def create_table():

    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transaction_analytics (
            id SERIAL PRIMARY KEY,
            transaction_id VARCHAR(50),
            amount DECIMAL(12,2),
            risk_score INTEGER,
            is_fraud BOOLEAN
        )
    """)

    conn.commit()
