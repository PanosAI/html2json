import pika  
import json  
  
def send_message_to_queue(message_body):  
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))  
    channel = connection.channel()  
  
    channel.queue_declare(queue='task_queue', durable=True)  
  
    message = json.dumps(message_body)  # convert the message body to JSON  
    channel.basic_publish(  
        exchange='',  
        routing_key='task_queue',  
        body=message,  
        properties=pika.BasicProperties(  
            delivery_mode=2,  # make message persistent  
        ))  
    print(" [x] Sent %r" % message)  
  
    connection.close()  
  
if __name__ == "__main__":  
    # Open the file/database with the chunk metadata  
    with open('chunk_metadata.json', 'r') as f:  
        chunk_metadata = json.load(f)  
  
    # Send each chunk as a message to the queue  
    for chunk in chunk_metadata:  
        send_message_to_queue(chunk)