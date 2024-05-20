import os
import pathlib
import glob

## ==============================================
## createDriverClass
## ==============================================
def createDriverClass(name):
    if name == "demon":
        from drivers.demondriver import DemonDriver
        return DemonDriver
    if name == "cassandra":
        from drivers.cassandradriver import CassandraDriver
        return CassandraDriver
    if name == "couchdb":
        from drivers.couchdbdriver import CouchdbDriver
        return CouchdbDriver
    if name == "csv":
        from drivers.csvdriver import CsvDriver
        return CsvDriver
    if name == "hbase":
        from drivers.hbasedriver import HbaseDriver
        return HbaseDriver
    if name == "membase":
        from drivers.membasedriver import MembaseDriver
        return MembaseDriver
    if name == "mongodb":
        from drivers.mongodbdriver import MongodbDriver
        return MongodbDriver
    if name == "redis":
        from drivers.redisdriver import RedisDriver
        return RedisDriver
    if name == "scalaris":
        from drivers.scalarisdriver import ScalarisDriver
        return ScalarisDriver
    if name == "sqlite":
        from drivers.sqlitedriver import SqliteDriver
        return SqliteDriver
    if name == "tokyocabinet":
        from drivers.tokyocabinetdriver import TokyocabinetDriver
        return TokyocabinetDriver
    else:
        return None
## DEF

## ==============================================
## getDrivers
## ==============================================
def getDrivers():
    drivers = []
    for f in [os.path.basename(x).replace("driver.py", "") for x in glob.glob(f"{pathlib.Path(__file__).parent.resolve()}/*driver.py")]:
        if f != "abstract": drivers.append(f)
    return (drivers)
## DEF
