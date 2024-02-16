from confluent_kafka import Producer
import json
import time


class ViewsProducer:
    def __init__(self):
        self.topic = "views"
        self.conf = {'bootstrap.servers': '{YOUR_CONFLUENT_BOOTSTRAP}:9092',
                     'security.protocol': 'SASL_SSL',
                     'sasl.mechanism': 'PLAIN',
                     'sasl.username': '{YOUR_CONFLUENT_API_KEY}',
                     'sasl.password': '{YOUR_CONFLUENT_API_SECRET}',
                     'client.id': "{YOUR_CLIENT_ID}"}
        self.data_dir = '/FileStore/resources/product_views/product_views.json'
    def delivery_callback(self, err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            key = msg.key().decode('utf-8')
            user_id = json.loads(msg.value().decode('utf-8'))["userid"]
            print(f"Produced event to : key = {key} value = {user_id}")

    def produce_views(self, producer):
        with open(self.data_dir) as lines:
            for line in lines:
                views = json.loads(line)
                message_id = views["messageid"]
                producer.produce(self.topic, key=message_id, value=json.dumps(views), callback=self.delivery_callback)
                time.sleep(0.5)
                producer.poll(1)


    def start(self):
        kafka_producer = Producer(self.conf)
        self.produce_views(kafka_producer)
        kafka_producer.flush()



if __name__ == "__main__":
    views_producer = ViewsProducer()
    views_producer.start()