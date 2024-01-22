from confluent_kafka import Producer
import socket
import http.client
import json

conf = {'bootstrap.servers': 'localhost:9092',
            'client.id': socket.gethostname()}

def currency_rate_poller(currency):
    conn = http.client.HTTPSConnection("api.coincap.io")
    payload = ''
    headers = {}
    conn.request("GET", "/v2/rates/" + currency, payload, headers)
    res = conn.getresponse()
    data = res.read()
    return json.loads(data)["data"]["rateUsd"]


def currency_rank_poller(currency):
    conn = http.client.HTTPSConnection("api.coincap.io")
    payload = ''
    headers = {}
    conn.request("GET", "/v2/assets/" + currency, payload, headers)
    res = conn.getresponse()
    data = res.read()
    return json.loads(data)["data"]["rank"]

def send(topic, key, message):
    producer = Producer(conf)

    def acked(err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            print("Message produced: %s" % (str(msg)))

    producer.produce(topic, key=key, value=message, callback=acked)

    # Wait up to 1 second for events. Callbacks will be invoked during
    # this method call if the message is acknowledged.
    producer.poll(1)