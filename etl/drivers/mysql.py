from connection import Connection

class MysqlConnection(Connection):
    driver = "com.mysql.jdbc.Driver"

    def load_data_frame(self, table, alias=None):
        conn = self.context.read.format("jdbc").options(
            url="jdbc:mysql://{host}:{port}/{db}".format(host=self.host, port=self.port, db=self.db),
            driver=MysqlConnection.driver,
            user=self.user,
            password=self.password)
        df = conn.load(dbtable="(" + table + ") as df_load")

        if alias is not None:
            df.registerTempTable(alias)

        return df

    def write_data_frame(self, df, table, mode="append"):
        properties = {
            "user": self.user,
            "password": self.password,
            "characterEncoding": "utf8"
        }
        df.write.jdbc(url="jdbc:mysql://{host}:{port}/{db}".format(host=self.host, port=self.port, db=self.db),
            table=table,
            mode=mode,
            properties=properties)
