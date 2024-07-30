import psycopg2
import logging
from psycopg2 import sql
print("Starting.....")
# Configure logging
logging.basicConfig(filename='prova_analytics_coupon.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Database connection details
db1_config = {
    'dbname': 'fiscozen',
    'user': 'administrator',
    'password': '2Ky6X4!BxFqtue6==',
    'host': 'data-platformf0ae860.cvq86cwow7on.eu-west-1.rds.amazonaws.com',
    'port': '5432'
}

db2_config = {
    'dbname': 'postgres',
    'user': 'administrator',
    'password': 'mxr4C9=hF!4KtE9Qi=',
    'host': 'data-platform-aurora-instance-1.cvq86cwow7on.eu-west-1.rds.amazonaws.com',
    'port': '5432'
}

def transfer_data(fetch_query, target_table):
    try:
        # Connect to the first database
        conn1 = psycopg2.connect(**db1_config)
        cursor1 = conn1.cursor()

        # Connect to the second database
        conn2 = psycopg2.connect(**db2_config)
        cursor2 = conn2.cursor()

        # Log the start of data fetching
        logging.info('Fetching data from the source database.')

        # Execute the fetch query
        cursor1.execute(fetch_query)
        rows = cursor1.fetchall()

        # If no rows are fetched, log and exit
        if not rows:
            logging.info('No data fetched from the source database.')
            return

        # Get the column names from the fetch query result
        column_names = [desc[0] for desc in cursor1.description]

        # Construct the insert query dynamically
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(target_table),
            sql.SQL(', ').join(map(sql.Identifier, column_names)),
            sql.SQL(', ').join(sql.Placeholder() * len(column_names))
        )

        # Log the start of data insertion
        logging.info('Inserting data into the target database.')

        # Insert the fetched data into the second database
        cursor2.executemany(insert_query, rows)

        # Commit the transaction in the second database
        conn2.commit()

        # Log the success of the operation
        logging.info('Data transfer completed successfully.')

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        if conn2:
            conn2.rollback()
    finally:
        # Close the database connections
        if cursor1:
            cursor1.close()
        if conn1:
            conn1.close()
        if cursor2:
            cursor2.close()
        if conn2:
            conn2.close()

# Example usage
fetch_query = """
SELECT
  c.id AS id,
  c.code AS code,
  c.value_eur_owner AS value_eur_owner,
  c.value_eur_referral AS value_eur_referral,
  c.value_eur_agreement as value_eur_agreement,
  c.owner_partner_id AS owner_partner_id,
  CASE
    WHEN c.code ILIKE '%ambassador%' THEN 'affiliate/ambassador'
    WHEN c.owner_partner_id IS NOT NULL and p.name NOT ILIKE '%fiscozen%' THEN 'affiliate/partner'
  END AS channel
FROM
  fiscozen_coupon c
  LEFT JOIN fiscozen_partner p on c.owner_partner_id = p.id;"""

target_table = 'analytics_coupon'

transfer_data(fetch_query, target_table)
print("Finished!!")

