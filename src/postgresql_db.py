import psycopg2


class PostgreSQLDatabase:
    def __init__(
        self,
        dbname: str,
        host: str,
        port: int,
        user: str,
        password: str,
    ):
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
        )
        self.cur = self.conn.cursor()

    def disconnect(self):
        self.cur.close()
        self.conn.close()

    def query(self, query: str, returning: bool = True, values=None):
        self.cur.execute(query, values)
        self.conn.commit()
        if returning:
            results = self.cur.fetchall()
            columns = [column[0] for column in self.cur.description]
            return [dict(zip(columns, row)) for row in results]
