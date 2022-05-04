from elasticsearch import Elasticsearch
import csv

from threading import Thread
from queue import Queue

threads = 24 # 4 queries at a time / host (6x4), need to experiment



def ESQueryProxy(es_api, index, que, thread_nr):
    #fetch the req from the queue unless queue is empty
    while not que.empty():
        data = que.get() # we have a nr and a req in the que values
        request = data[1]
        item_nr = data[0]
        response = es_api.search(index=index, query=request)
        # do stuff with the result here in its own thread alrdy, when the while loop goes to next iteration, the result will be overwritten
        # write to a file named output_thread_nr.csv and aggregate results at the end
        # or something else :) need to watch out for file writing concurrency
        # the logging library has no issues with the concurrent writes, if it can be configured to write in a format of csv then it's ideal
        with open('output_' + thread_nr +'.csv', 'w') as csvfile:
            fieldnames = [
                '@timestamp',
                'client.ip',
                'destination.ip',
                'destination.port',
            ]
            spamwriter = csv.DictWriter(csvfile,fieldnames=fieldnames)
            spamwriter.writeheader()
            # I have no idea whats in a response but need to parse and write ;)
            for i in response:
                spamwriter.writerow(i)
        que.task_done()


def aggregate_results():
    for i in threads:
        # merge , sort the output_i.csv files into one
        # i would definetly try writing into one single output file directly from the threads, so that this aggregation at the end is not needed
        # try hacking the logging library into a csv write if the csv writer doesnt like the write concurrency


# array of 6 API interfaces
# set pages 1, max_size=100-500-1000 , experiment with the size of the response
# (bigger size, slower response, more time for nw transfer etc; if it's too small+too many requests in parallel then maybe other issues, new flooded ddos style or so)
es_api = []
for i,n in enumerate(['node_1', ..., 'node_6']):
    es_api[i] = Elasticsearch([n],
    scheme="https",
    http_auth=('user', 'secret'),
    max_retries=10,
    retry_on_timeout=True,
    request_timeout=60,
    ignore_throttled=False,
    maxsize=50,
    sniff_on_start=True,
    sniff_on_connection_fail=True,
    sniffer_timeout=60,)

# build a que of queries or figure out how the threads run queries
# need to know that these will be run in an independent context, one query does not know about the other
# unlimited size of the queue
req_q = Queue(maxsize=0)
for i, request in enumerate(settings_request):
    req_q.put((i,request) # 'i' is just a nr of the request for tracking / logging purposes

# we want numbered threads for tracking / logging (see my example mtcopy for configuring logging and error checking
# failed / timed out requests might need to be retried
# we can save the thread objects but it's not really needed - jsut in case they need management
workers = []
# this loop would launch 'thread' amount of processes running the target function with the passed arguments
for i in range(int(threads)):
    logging.info('Starting thread %d ' % i)
    workers[i] = Thread(target=ESQueryProxy, args=(es-obj[i%6], settings_index, req_q, i))
    workers[i].setDaemon(True)
    workers[i].start()
logging.info('Threads started, waiting to complete.')
req_q.join()
aggregate_results()