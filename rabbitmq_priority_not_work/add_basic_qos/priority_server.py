#!/usr/bin/env python

import pika
import time
from concurrent.futures import ThreadPoolExecutor

conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',
                                                         virtual_host="vhost",
                                                         port=5672,
                                                         credentials=pika.PlainCredentials("user","pwd"),
                                                         heartbeat=0))
ch = conn.channel()

x = "amq.fanout"
q = "test"

ch.queue_declare(queue=q, durable=True, arguments={'x-max-priority': 10})
ch.queue_purge(queue=q)

ch.queue_bind(exchange=x, queue=q, routing_key="")


ch.basic_publish(exchange=x, routing_key="", body="1",
                 properties=pika.BasicProperties(priority=1, delivery_mode=1))
ch.basic_publish(exchange=x, routing_key="", body="2",
                 properties=pika.BasicProperties(priority=2, delivery_mode=1))
ch.basic_publish(exchange=x, routing_key="", body="3",
                 properties=pika.BasicProperties(priority=5, delivery_mode=1))
ch.basic_publish(exchange=x, routing_key="", body="4",
                 properties=pika.BasicProperties(priority=3, delivery_mode=1))

print("Done publishing.")

time.sleep(2)


def callback(ch, method, properties, body):
    time.sleep(1)
    print(" [x] Received {}, priorirty = {}".format(body, properties.priority))

def start_con():
  # ch.basic_consume(callback, queue=q, no_ack=True)
  ch.basic_consume(q, callback, auto_ack=True)
  # ch.basic_qos(1)
  ch.basic_qos(prefetch_count=1)
  print("Consuming... Hit Ctrl+C to stop...")
  ch.start_consuming()

def publish():
  for i in range(10):
      priority = 1
      ch.basic_publish(exchange=x, routing_key="", body=str(priority),
                       properties=pika.BasicProperties(priority=priority, delivery_mode=1))
  for i in range(10):
      priority = 2
      ch.basic_publish(exchange=x, routing_key="", body=str(priority),
                       properties=pika.BasicProperties(priority=priority, delivery_mode=1))

start_con()

# with ThreadPoolExecutor() as pool:
#   pool.submit(start_con)
#   pool.submit(publish)

conn.close()


# while True:
#     time.sleep(1)
