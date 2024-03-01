from consumer_interface import mqConsumerInterface
import pika
import os
import json

class mqConsumer(mqConsumerInterface):
    
    def __init__(self, binding_key, exchange_name, queue_name) -> None:
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.setupRMQConnection()
    
    def startConsuming(self) -> None:
        print("[*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()

    def __del__(self) -> None:
        print("Closing RMQ connection on destruction")
        self.channel.close()
        self.connection.close()
    
    def on_message_callback(self, channel, method_frame, header_frame, body) -> None:
        print("beginning of message receive callback")
        message = body.decode("utf-8")
        channel.basic_ack(method_frame.delivery_tag, False)
        print(f"Received message: {message}")


    def setupRMQConnection(self) -> None:
        print("setting up rmq connection")
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)

        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)
        
        print("queue declared")
        exchange = self.channel.exchange_declare(exchange=self.exchange_name, exchange_type="topic")

        self.channel.queue_bind(
        queue=self.queue_name,
        routing_key= self.binding_key,
        exchange=self.exchange_name,
        )

        print ("queue bound")

        self.channel.basic_consume(
        self.queue_name, self.on_message_callback, auto_ack=False
        )

        print ("basic consume called")

        self.startConsuming()

        print("start of consumed called")






    
    

