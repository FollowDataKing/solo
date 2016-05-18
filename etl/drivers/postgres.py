from connection import Connection

class PostgresConnection(Connection):
    driver = "org.postgresql.Driver"

    def load_data_frame(self, table, alias=None):
        conn = self.context.read.format("jdbc").options(
            url="jdbc:postgresql://{host}:{port}/{db}".format(host=self.host, port=self.port, db=self.db),
            driver=PostgresConnection.driver,
            user=self.user,
            password=self.password)
        df = conn.load(dbtable="(" + table + ") as df_load")

        if alias is not None:
            df.registerTempTable(alias)

        return df

    def write_data_frame(self, table):
        pass
