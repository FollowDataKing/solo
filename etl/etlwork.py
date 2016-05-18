# -*- coding: utf-8 -*-

__author__ = 'baihe'
__author_email__ = 'baihe@xiaomei.com'
__mtime__ = '16/5/17'

def load_sub_modules(module):
    import sys
    import os
    import pkgutil
    dirname = os.path.dirname(module.__file__)
    for importer, package_name, _ in pkgutil.iter_modules([dirname]):
        full_package_name = '%s.%s' % (module.__name__, package_name)
        if full_package_name not in sys.modules:
            importer.find_module(package_name).load_module(full_package_name)

def load_drivers():
    import drivers
    from connection import Connection

    load_sub_modules(drivers)
    supports = dict((cls.__name__.replace("Connection", "").lower(), cls) for cls in Connection.__subclasses__())

    print "Supported drivers: ", supports.keys()
    return supports

def load_ext_jars(path, classpath="SPARK_CLASSPATH"):
    import os
    from os import listdir
    from os.path import isfile, join
    jars = [join(path, f) for f in listdir(path) if isfile(join(path, f)) if f.endswith(".jar")]
    os.environ[classpath] = ":".join(jars)


class ETLWork(object):
    import os
    extlib = os.environ.get('EXT_LIB_DIR', None)
    if extlib is None:
        raise RuntimeError("Environment [EXT_LIB_DIR] not set!")

    spark_home = os.environ.get('SPARK_HOME', None)
    if spark_home is None:
        raise RuntimeError("Environment [SPARK_HOME] not set!")

    load_ext_jars(extlib)
    supports = load_drivers()

    def __init__(self, app_name="etlwork", spark_master="local[*]", py4j_version="0.9"):
        import os
        import sys

        # Add pyspark to sys.path
        sys.path.insert(0, ETLWork.spark_home + "/python")

        # Add the py4j to the path.
        # You may need to change the version number to match your install
        sys.path.insert(0, os.path.join(ETLWork.spark_home, 'python/lib/py4j-' + py4j_version + '-src.zip'))

        from pyspark import SparkContext
        from pyspark import SparkConf
        from pyspark.sql import SQLContext

        # Initialize spark conf/context/sqlContext
        self.conf = SparkConf().setMaster(spark_master).setAppName(app_name)
        self.sc = SparkContext(conf=self.conf)
        self.context = SQLContext(self.sc)

    def connect(self, driver, host, port, user, password, db):
        conn_cls = ETLWork.supports[driver]
        return conn_cls(self.context, host, port, user, password, db)

    def execute_task(self, task):
        import importlib
        task_module = importlib.import_module("tasks.%s" % task)
        task_module.execute(self)

    @staticmethod
    def execute(task):
        etl = ETLWork()
        etl.execute_task(task)

if __name__ == "__main__":
    import sys
    ETLWork.execute(sys.argv[1])
