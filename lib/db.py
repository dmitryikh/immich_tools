import sqlite3

def query_all_database(db_path, fields, include_corrupted=False):
    """Execute a query on the SQLite database and return the results."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    query = f"SELECT {', '.join(fields)} FROM media_files"
    if not include_corrupted:
        query += " WHERE is_corrupted = 0"
    query += " ORDER BY file_path"
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results