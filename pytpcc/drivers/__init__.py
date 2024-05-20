import os
import pathlib
import glob

## ==============================================
## createDriverClass
## ==============================================
def createDriverClass(name):
    if name == "demon":
        from .demondriver import DemonDriver
        return DemonDriver
    if name == "cassandra":
        from .cassandradriver import CassandraDriver
        return CassandraDriver
    if name == "couchdb":
        from .couchdbdriver import CouchdbDriver
        return CouchdbDriver
    if name == "csv":
        from .csvdriver import CsvDriver
        return CsvDriver
    if name == "hbase":
        from .hbasedriver import HbaseDriver
        return HbaseDriver
    if name == "membase":
        from .membasedriver import MembaseDriver
        return MembaseDriver
    if name == "mongodb":
        from .mongodbdriver import MongodbDriver
        return MongodbDriver
    if name == "redis":
        from .redisdriver import RedisDriver
        return RedisDriver
    if name == "scalaris":
        from .scalarisdriver import ScalarisDriver
        return ScalarisDriver
    if name == "sqlite":
        from .sqlitedriver import SqliteDriver
        return SqliteDriver
    if name == "tokyocabinet":
        from .tokyocabinetdriver import TokyocabinetDriver
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
