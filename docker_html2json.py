import os
import json
from tqdm import tqdm
from pathlib import Path
from html2jsonUp import collect
from pyquery import PyQuery
import re
import openstack




def process_html(html_obj, template  , obs_input , obs_output):
    """extract data from html files from object store""" 
    html_file_content = conn.obs.download_object(html_obj , obs_input)
    html_file_metadata = conn.obs.get_object_metadata(html_obj , obs_output)
    
    html_file_name = html_obj["Key"]
    json_file_name = str(html_file_name).replace('.html', '.json')
    
    try:
        data = collect('<html>' + str(html_file_content) + f'<span class="creation-time">{html_file_metadata["LastModified"]}</span></html>', template)
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

def process_folders(container,input_file, output_file):
    try:
        with open(input_file, 'r') as file:
            folders = file.readlines()

            # Process each folder
            for folder in folders:
                folder = folder.strip()  # Remove newline characters
                
            
                

                # Append the processed folder to the output file
                with open(output_file, 'a') as processed_file:
                    # check if the folder has already been processed skip it
                    if folder in processed_file.read():
                        continue
                    else:
                        processed_file.write(folder + '\n')
                        gen = create_folder_generator(container , folder)

                        for html in gen:
                            process_html(html , template , container , 'companies-json-format')

    except FileNotFoundError:
        print("File not found!")
    except Exception as e:
        print(f"An error occurred: {e}")            

if __name__ == '__main__':
    conn = openstack.connect()
    
    #for cont in conn.obs.containers():
    #    if cont.name == 'companies-test':
    #        obs_input = cont 
    #    if cont.name == 'companies-test':
    #        obs_output = cont       
    #creation_time = os.path.getmtime(html_file)

    
    os.chdir("/app")
    
    with open('/app/html2json/template.json', 'r', encoding='utf-8') as tp:
        template = json.load(tp)
        
        process_folders('jobs-html-original-new1' , 'queue.txt' , 'processed.txt')

        for html_obj in obj_generator:
            process_html(html_obj, template, 'companies-test','companies-json-format')
        # once the files are processed, upload the log file to OBS
        conn.obs.upload_object(name = 'log.txt' ,container = 'companies-json-format' , data = open('log.txt', 'rb'))    
