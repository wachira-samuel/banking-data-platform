from storage import connect_db

def save_processed_transaction(transaction):

    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO transaction_analytics
        (transaction_id, amount, risk_score, is_fraud)
        VALUES (%s,%s,%s,%s)
    """, (
        transaction["transaction_id"],
        transaction["amount"],
        transaction["risk_score"],
        transaction["is_fraud"]
    ))

    conn.commit()
