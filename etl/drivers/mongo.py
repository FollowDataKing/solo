from connection import Connection

class MongoConnection(Connection):
    driver = "com.stratio.datasource.mongodb"

    sql = "CREATE TEMPORARY TABLE {alias} USING {driver} OPTIONS (host '{host}:{port}', database '{db}', collection '{table}', credentials '{user},{db},{password}')"

    def load_data_frame(self, table, alias=None):
        true_alias = alias or table

        self.context.sql(MongoConnection.sql.format(
            alias=true_alias,
            driver=MongoConnection.driver,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            db=self.db,
            table=table
        ))

        return self.context.sql("select * from " + true_alias)

    def write_data_frame(self, table):
        pass
