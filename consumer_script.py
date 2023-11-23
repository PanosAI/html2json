import os
import json

from html2jsonUp import collect
from pyquery import PyQuery
import re
import openstack
import json
import pika

def process_html(html_obj, template  , obs_input , obs_output):
    html_file_content = conn.obs.download_object(html_obj , obs_input)
    html_file_content = html_file_content.decode('utf-8')
    html_file_metadata = conn.obs.get_object_metadata(html_obj , obs_input)
    
    html_file_name = html_obj["Key"]
    json_file_name = str(html_file_name).replace('.html', '.json')
    
    
    try:
        data = collect('<html>' + html_file_content + f'<span class="creation-time">{html_file_metadata["LastModified"]}</span></html>', template)
        #with open(json_file_name, 'w', encoding='utf-8') as f:
        #    json.dump(data, f, ensure_ascii=False, indent=4)
        conn.obs.upload_object(name = json_file_name ,container = obs_output , data = json.dumps(data))

    except Exception as e:
        # write (error, file name) to log file
        with open('log.txt', 'a') as f:
            f.write(f'{html_file_name},{e}\n')

def create_folder_generator(container , folder):
    for html_obj in conn.obs.objects(container ,headers={"prefix" :folder}):
        if folder in html_obj['Key']:
            yield html_obj
        else:
            break    

def process_folders(container_in, container_out,input_file, output_file):
    try:
        # download the content of input file and the output file from the container
        folders = conn.obs.download_object(input_file , container_in )
        folders = folders.decode('utf-8').split('\r\n')
        print(folders[0])
            # Process each folder
        for folder in folders:
          if folder == '':
            continue
          processed = conn.obs.download_object(output_file , container_in)
          processed = processed.decode('utf-8').split('\r\n')
          
          print(processed[0])
          
          if folder in processed:
            continue
          else:
            gen = create_folder_generator(container_in , folder)
            
            for html in gen:
              process_html(html, template, container_in, container_out)
            
            print('processed')
            processed.append(folder)
            processed_data = '\n'.join(processed)
      
            conn.obs.upload_object(name = output_file ,container = container_in , data = processed_data)      

    except FileNotFoundError:
        print("File not found!")
    except Exception as e:
        print(f"An error occurred: {e}") 

def callback(ch, method, properties, body):  
    print(" [x] Received %r" % body)  
    message = json.loads(body)  # convert the message body from JSON  
  
    # Here, message should contain the information about the HTML files  
    # to process. You would replace this with your own logic.  
    html_obj = message['html_obj']  
    #template = message['template']  
    obs_input = message['obs_input']  
    obs_output = message['obs_output']  
  
    process_folders(obs_input, obs_output, 'processed.txt', 'processed.txt') 
  
    ch.basic_ack(delivery_tag=method.delivery_tag) 
         


if __name__ == "__main__":  
    conn = openstack.connect()  
  
    os.chdir("/app")  
  
    with open('/app/html2json/template.json', 'r', encoding='utf-8') as tp:  
        template = json.load(tp)  
  
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))  
    channel = connection.channel()  
  
    channel.queue_declare(queue='task_queue', durable=True) 
    channel.basic_qos(prefetch_count=1) 
    channel.basic_consume(queue='task_queue', on_message_callback=callback)  
  
    print(' [*] Waiting for messages. To exit press CTRL+C')    
    channel.start_consuming()