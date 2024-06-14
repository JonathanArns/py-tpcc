import os
import sys
import logging
import time
import httpx
from pprint import pprint,pformat
from datetime import datetime

import constants
from drivers.abstractdriver import *


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
        
        self.url = f"http://{config['host']}:{str(config['port'])}/query"
        transport = httpx.HTTPTransport(retries=1)
        self.client = httpx.Client(http2=True, transport=transport)

    def exec_query(self, query):
        response = self.client.post(self.url, content=query.encode("utf-8"))
        if response.status_code >= 400:
            raise Exception("bad response")
        return response.read()
    
    ## ----------------------------------------------
    ## loadTuples
    ## ----------------------------------------------
    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0: return
        logging.debug("Loading %d tuples for tableName %s" % (len(tuples), tableName))
        query = "load_tuples " + tableName + " " + ";".join([",".join([str(int(val.timestamp())) if isinstance(val, datetime) else str(val) for val in item]) for item in tuples])
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
        # wait for state to be settled
        time.sleep(5)

    ## ----------------------------------------------
    ## doDelivery
    ## ----------------------------------------------
    def doDelivery(self, params):
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]
        
        query = "delivery " + str(w_id) + " " + str(o_carrier_id) + " " + str(int(ol_delivery_d.timestamp())) 
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
        query = " ".join(["new_order", str(w_id), str(d_id), str(c_id), str(int(o_entry_d.timestamp())), i_ids, i_w_ids, i_qtys])
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
        query = " ".join(["payment", str(w_id), str(d_id), str(h_amount), str(c_w_id), str(c_d_id), str(c_id), str(c_last), str(int(h_date.timestamp()))])
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
