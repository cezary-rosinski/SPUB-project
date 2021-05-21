import sqlite3



def create_db(db_name, list_of_tables, list_of_names):
    connection = sqlite3.connect(db_name)
    for name, table in zip(list_of_names, list_of_tables):
        table.to_sql(name, connection, if_exists='replace', index=False)
    connection.close()
    