import os
import sys
import logging
import http.client
from pprint import pprint,pformat
from datetime import datetime

if os.environ.get("EXEC_MODE") == "remote_worker":
    import constants
    from drivers.abstractdriver import *
else:
    from .. import constants
    from .abstractdriver import *


## ==============================================
## DemonDriver
## ==============================================
class DemonDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "host":         ("The hostname", "localhost"),
        "port":         ("The port number", 8080),
    }
    
    def __init__(self, ddl):
        super(DemonDriver, self).__init__("demon", ddl)
        self.database = None
        self.conn = None
        self.denormalize = False
        self.w_customers = { }
        self.w_orders = { }
        
        ## Create member mapping to collections
        for name in constants.ALL_TABLES:
            self.__dict__[name.lower()] = None
    
    ## ----------------------------------------------
    ## makeDefaultConfig
    ## ----------------------------------------------
    def makeDefaultConfig(self):
        return DemonDriver.DEFAULT_CONFIG
    
    ## ----------------------------------------------
    ## loadConfig
    ## ----------------------------------------------
    def loadConfig(self, config):
        for key in list(DemonDriver.DEFAULT_CONFIG.keys()):
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)
        
        self.addr = config["host"] + ":" + str(config["port"])
        self.conn = http.client.HTTPConnection(self.addr)

    def exec_query(self, query):
        for _ in range(3):
            try: 
                self.conn.request('POST', '/query', query, {})
                response = self.conn.getresponse()
                return response.read()
            except Exception as e:
                print(e)
                print(("on query: " + query))
                self.conn = http.client.HTTPConnection(self.addr)
    
    ## ----------------------------------------------
    ## loadTuples
    ## ----------------------------------------------
    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0: return
        logging.debug("Loading %d tuples for tableName %s" % (len(tuples), tableName))
        query = "load_tuples " + tableName + " " + ";".join([",".join([val.isoformat() if isinstance(val, datetime) else str(val) for val in item]) for item in tuples])
        self.exec_query(query)
        
    ## ----------------------------------------------
    ## loadFinishDistrict
    ## ----------------------------------------------
    def loadFinishDistrict(self, w_id, d_id):
        pass

    ## ----------------------------------------------
    ## loadFinish
    ## ----------------------------------------------
    def loadFinish(self):
        pass

    ## ----------------------------------------------
    ## doDelivery
    ## ----------------------------------------------
    def doDelivery(self, params):
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]
        
        query = "delivery " + str(w_id) + " " + str(o_carrier_id) + " " + ol_delivery_d.isoformat()
        self.exec_query(query)

    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = params["o_entry_d"]
        i_ids = ",".join([str(item) for item in params["i_ids"]])
        i_w_ids = ",".join([str(item) for item in params["i_w_ids"]])
        i_qtys = ",".join([str(item) for item in params["i_qtys"]])
        query = " ".join(["new_order", str(w_id), str(d_id), str(c_id), o_entry_d.isoformat(), i_ids, i_w_ids, i_qtys])
        self.exec_query(query)


    ## ----------------------------------------------
    ## doOrderStatus
    ## ----------------------------------------------
    def doOrderStatus(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        query = " ".join(["order_status", str(w_id), str(d_id), str(c_id), str(c_last)])
        self.exec_query(query)
        
    ## ----------------------------------------------
    ## doPayment
    ## ----------------------------------------------    
    def doPayment(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = params["h_date"]
        query = " ".join(["payment", str(w_id), str(d_id), str(h_amount), str(c_w_id), str(c_d_id), str(c_id), str(c_last), h_date.isoformat()])
        self.exec_query(query)

        
    ## ----------------------------------------------
    ## doStockLevel
    ## ----------------------------------------------    
    def doStockLevel(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]
        query = " ".join(["stock_level", str(w_id), str(d_id), str(threshold)])
        self.exec_query(query)
