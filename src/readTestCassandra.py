import json
import datetime
import sys
import Queue
import time
import threading
from time import gmtime, strftime
import logging
import csv
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy, TokenAwarePolicy, RetryPolicy
from cassandra.query import tuple_factory, SimpleStatement
from cassandra import ConsistencyLevel

sourceDB = ["10.181.2.54","10.181.2.55", "10.181.3.16", "10.181.3.17"]

class RunningStats(object):
    def __init__(self):
        self.total = 0
        self.noPts = 0
        self.max = 0
        self.min = sys.float_info.max
    
    def add(self, value):
        self.total += value
        self.noPts += 1
        if self.max < value:
            self.max = value
        if self.min > value:
            self.min=value
        
    def getAvg(self):
        if self.noPts == 0:
            raise Exception("Number of Points is 0, add first before averaging")
        return self.total/self.noPts
    
    def getMax(self):
        if self.noPts == 0:
            raise Exception("Number of Points is 0, add first before averaging")
        return self.max

    def getMin(self):
        if self.noPts == 0:
            raise Exception("Number of Points is 0, add first before averaging")
        return self.min

    def getTotal(self):
        return self.total
    
    def getNoPoints(self):
        return self.noPts
    
    
class Consumer(threading.Thread):
    def __init__(self, que, name, logger):
        threading.Thread.__init__(self, name=name)
        self.name = "Consumer_"+str(name)
        self.q = que
        self.logger = logger
        
    def run(self):
        cluster = Cluster(contact_points=sourceDB,
                load_balancing_policy= RoundRobinPolicy(),
                default_retry_policy = RetryPolicy())

        session = cluster.connect()
        session.row_factory = tuple_factory
        bound_statement = SimpleStatement("""
                SELECT JSON * FROM forecast.verify
                WHERE cycle = %s
                AND site = %s 
                AND forecastType = %s
                ALLOW FILTERING;""",
                consistency_level=ConsistencyLevel.QUORUM)


        while True:
            try:
                queryData = self.q.get(True, 15)
            except Queue.Empty:
                print "nothing left on queue returning from "+self.name
                return 0
            
            ra = RunningStats()
            #self.logger.info(str(queryData))

            startTime = datetime.datetime.now()
            #self.logger.info(bound_statement % (queryData[1], queryData[0], queryData[2]))
            
	    cursor = ''
            if len(queryData) >= 3:
                try:
                    cursor = session.execute(bound_statement, parameters=[queryData[1], queryData[0], queryData[2]])
                except:
                    self.logger.error("IndexArray error: "+str(queryData))
#                cursor = session.execute(bound_statement, parameters=[queryData[1], queryData[0], queryData[2]])
            else:
                print "Incomplete input: ", queryData

            if len(cursor) == 0:
                print self.name, "data not found"
                self.logger.info( "Query time no data:"+ "0, 0, "+
                          queryData[1]+", "+queryData[0]+", "+queryData[2])

            else:
                print self.name, type(cursor[0][0]), len(cursor[0])
                row = cursor[0]
                endTime = datetime.datetime.now()
                if len(row) > 0:
                        print self.name, "Row Length: ", len(row[0])
                else:
                        print self.name, "Row Fetched has no length"

                self.logger.info( "Query time:"+ str((endTime-startTime).total_seconds())+", "+str(len(row[0]))+", "+
                                queryData[1]+", "+queryData[0]+", "+queryData[2])

            self.q.task_done()

class Producer(threading.Thread):
    def __init__(self, ques, logger):
        threading.Thread.__init__(self, name="producer")
        self.qs = ques
        self.logger = logger

    def run(self):
        with open('/ipython/random.data', 'rb') as f:
            reader = csv.reader(f)
            queryData = list(reader)
            for row in queryData:
                placed = False
                while (not placed):
                    for q_id in range(len(self.qs)):
                        if self.qs[q_id].qsize() == 0 and not placed:
                            self.logger.info("Placing "+str(row)+" on queue no. "+str(q_id))
                            self.qs[q_id].put( row )
                            placed = True
                            break
                            
                    if (not placed):
                        time.sleep(0.05)
                    
if __name__ == "__main__":
    logging.basicConfig( \
    format='%(asctime)s %(levelname)s %(threadName)s %(filename)s(%(lineno)s)-%(funcName)s %(message)s '\
    , level=logging.INFO, filename="readTestCassandra.log")
    logger = logging.getLogger()
    
    logger.info("begin")
    threads = 10 
    threadList = []
    startTime = datetime.datetime.now()
    qs = []  
    
    logger.info("start")

    for i in range(threads):
        logger.info("Creating and loading queue " + str(i))
        q = Queue.Queue()
        qs.append(q)
        t = Consumer(name=i, que=q, logger=logger)
        threadList.append(t)
        t.start()

    logger.info("load")
    #Thread(target=producer).start()
    p=Producer(qs, logger=logger)
    p.run()
    
    logger.info("join")
    time.sleep(2)
    for q in qs:
        q.join()
    for t in threadList:
        t.join()
        
    endTime = datetime.datetime.now()
    logger.info("Total Run time: " + str(endTime-startTime))
    logger.info("finished")

