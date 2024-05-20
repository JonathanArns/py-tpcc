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
    else:
        return None
    # full_name = "%sDriver" % name.title()
    # mod = import_module(f".{full_name.lower()}", package="pytpcc.drivers")
    # klass = getattr(mod, full_name)
    # return klass
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
