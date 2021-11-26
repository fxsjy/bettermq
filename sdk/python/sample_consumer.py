import grpc
import time
from bettermq_pb2 import *
from bettermq_pb2_grpc import *

host = '127.0.0.1:8404'

with grpc.insecure_channel(host) as channel:
    client = PriorityQueueStub(channel)
    i = 1
    while True:
        req = DequeueRequest(
            topic = "root",
            count = 1,
        )
        i+=1
        rsps = client.Dequeue(req)
        if len(rsps.items) == 0:
            print("nothing...")
            time.sleep(5)
        print(rsps)

