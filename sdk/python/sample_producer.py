import grpc
from bettermq_pb2 import *
from bettermq_pb2_grpc import *

host = '127.0.0.1:8404'

data = open('/bin/ls', 'rb').read()

with grpc.insecure_channel(host) as channel:
    client = PriorityQueueStub(channel)
    for i in range(1,100001):
        req = EnqueueRequest(
            topic = "root",
            meta = "k" + str(i),
            payload = data,
            priority = i % 5
        )
        rsps = client.Enqueue(req)
        print(rsps)

