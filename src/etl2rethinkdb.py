import json
import datetime
import sys
from Queue import Queue
from elasticsearch import Elasticsearch
import time
import threading
import rethinkdb as r
from time import gmtime, strftime
import logging

sourceDB = ["10.181.1.6", "10.181.1.7"]
destDB = "rethinkdb.service.consul"

class RunningAvg(object):
    def __init__(self):
        self.total = 0
        self.noPts = 0
    
    def add(self, value):
        self.total += value
        self.noPts += 1
        
    def getAvg(self):
        if self.noPts == 0:
            raise Exception("Number of Points is 0, add first before averaging")
        return self.total/self.noPts
    
    def getTotal(self):
        return self.total
    
    def getNoPoints(self):
        return self.noPts
    
class Consumer(threading.Thread):
    def __init__(self, que, name, logger):
        #print "initializeing", i
        threading.Thread.__init__(self, name=name)
        self.name = "Consumer_"+str(name)
        self.q = que
        self.logger = logger
        #print "init Done for", i
        
    def run(self):
        retry = True
        cntr = 0

        while retry:
            try:
                conn = r.connect( destDB, 28015).repl()
                retry = False
            except:
                cntr += 1
                if cntr > 5:
                    self.logger.error("ERROR: getting connection to db aborting")
                    retry = False
                    raise Exception("ERROR: getting connection to db aborting")
                else:
                    retry = True
        es = Elasticsearch(sourceDB)
                        

        while True:
            site = self.q.get(True, 600)
            ra = RunningAvg()
            self.logger.debug("Consumer thread "+str(self.name)+" processing site: " + site)
            cycles = es.search(index="verify_source", doc_type=site, body={"size": 100000, "fields": ["cycle", "forecastType"]})
            
            if len(cycles["hits"]["hits"]) == 0:
                self.logger.info("Consumer Thread "+ str(self.name)+ " processing site: "+site+\
                       " for cycle " +cycles)
            else:
                for cycle in cycles["hits"]["hits"]:
                    #print site, cycle["_id"]
                    forecast = es.get(index="verify_source", doc_type=site, id=cycle["_id"])

                    dict_keys = []
                    fcast = json.loads(forecast["_source"]["forecast"])
                    for row in fcast:
                        for k in row.keys():
                            if k not in dict_keys:
                                dict_keys.append(k)

                    collection = {}
                    for k in dict_keys:
                        collection[k]= []


                    for row in fcast:
                        for dk in dict_keys:
                            if dk in row.keys():
                                collection[dk].append(row[dk])
                            else: 
                                collection[dk].append("")
                    tjson = {}
                    tjson["id"]=site+'_'+cycle["_id"]
                    tjson["site"]=site
                    tjson["cycle"]=cycle["fields"]["cycle"][0]
                    tjson["forecastType"]=cycle["fields"]["forecastType"][0]
                    tjson["forecast"]=collection

                    startTime = datetime.datetime.now()
                    result= r.db("forecast").table("verify").insert(tjson).run(durability="soft", noreply=True)
                    endTime = datetime.datetime.now()
                    ra.add((endTime-startTime).total_seconds())
                    
                    self.logger.info(str(self.name)+ " saving site: "+site+\
                      " for cycle " +cycle["fields"]["cycle"][0]+ " results " + json.dumps(result)[0:100])
            if ra.getNoPoints() > 0:
                self.logger.info("Avg Time to save "+str(ra.getNoPoints())+" Points was "+str(ra.getAvg())+" for "+ site)
                self.logger.info("AvgCSV, "+str(ra.getNoPoints())+", "+str(ra.getAvg())+", "+ site)
            else:
                self.logger.info("No data Saved for Site "+site)
                self.logger.info("AvgCSV, , , "+ site)
            self.q.task_done()

class Producer(threading.Thread):
    def __init__(self, ques, logger):
        threading.Thread.__init__(self, name="producer")
        self.qs = ques
        self.logger = logger

    def run(self):
        es = Elasticsearch(sourceDB)

        sites = es.search(index='verify_source', body={"size": 0, "aggs": {"mycount": {"terms": {"field": "site", "size": 0, "order": {"_term": "asc"}}}}})

        q_id = 0;
        self.logger.info("queue length: "+str(len(self.qs)))
        
        for site in sites["aggregations"]["mycount"]["buckets"]:
            placed = False
            while (not placed):
                for q_id in range(len(self.qs)):
                    if self.qs[q_id].qsize() == 0:
                        self.logger.info("Placing site "+site["key"]+" on queue no. "+str(q_id))
                        self.qs[q_id].put( site["key"] )
                        q_id = (q_id + 1)%len(self.qs)
                        placed = True
                        break
                if (not placed):
		    time.sleep(10)


if __name__ == "__main__":
    logging.basicConfig( \
    format='%(asctime)s %(levelname)s %(threadName)s %(filename)s(%(lineno)s)-%(funcName)s %(message)s '\
    , level=logging.INFO, filename="etl2rethink.log")
    logger = logging.getLogger()
    
    logger.info("begin")
    threads = 10 
    threadList = []
    startTime = datetime.datetime.now()
    qs = []  
    
    logger.info("start")

    for i in range(threads):
        logger.info("Creating and loading queue " + str(i))
        q = Queue()
        qs.append(q)
        t = Consumer(name=i, que=q, logger=logger)
        threadList.append(t)
        t.start()

    logger.info("load")
    #Thread(target=producer).start()
    p=Producer(qs, logger=logger)
    p.run()
    
    logger.info("join")
    time.sleep(200)
    for q in qs:
        q.join()
    for t in threadList:
        t.join()
        
    endTime = datetime.datetime.now()
    logger.info("Total Run time: " + str(endTime-startTime))
    logger.info("finished")


