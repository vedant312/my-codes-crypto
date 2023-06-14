import pandas as pd
import redshift_connector


def execute_query(query: str) -> None:
    conn = redshift_connector.connect(
        host='redshift-cluster-1.c4lh7ejvk9mj.eu-west-1.redshift.amazonaws.com',
        port=5439,
        database='dev',
        user='intern',
        password='Mypassword1',
    )

    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    conn.close()
