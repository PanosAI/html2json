import os
import json
import multiprocessing
from multiprocessing import Pool, freeze_support
from tqdm import tqdm
from pathlib import Path
from html2json.html2jsonUp import collect
from pyquery import PyQuery
import re
import openstack
from itertools import repeat



def process_html(html_obj, template , obs_input , obs_output):
    html_file_content = conn.obs.download_object(html_obj , obs_input)
    html_file_metadata = conn.obs.get_object_metadata(html_obj , obs_input)
    html_file_name = html_obj.key
    
    json_file_name = str(html_file_name).replace('.html', '.json')
    
    #target_folder = os.path.dirname(json_file_name)
    
    #if not os.path.exists(target_folder):
    #    os.makedirs(target_folder)
    
    try:
        data = collect('<html>' + html_file_content + f'<span class="creation-time">{html_file_metadata.LastModified}</span></html>', template)
            
        conn.obs.upload_object(name = json_file_name ,container =  obs_output , data = json.dumps(data))

    except Exception as e:
        # write (error, file name) to log file
        with open('log.txt', 'a') as f:
            f.write(f'{html_file_name},{e}\n')


def generate_obs_obj(obs_container):
    for html_obj in obs_container.objects():
        if html_obj.key.endswith('.html'):
            yield html_obj

if __name__ == '__main__':
    conn = openstack.connect()
    
    for cont in conn.obs.containers():
        if cont.name == 'companies-test':
            obs_input = cont 
        if cont.name == 'companies-test-output.panosai':
            obs_output = cont       
    #creation_time = os.path.getmtime(html_file)

    obj_generator = generate_obs_obj(obs_input)
    
    max_processes = multiprocessing.cpu_count()
    


    with open('./html2json/template.json', 'r', encoding='utf-8') as tp:
        template = json.load(tp)
        
        pool = Pool(processes=max_processes)
        for _ in tqdm(pool.starmap(process_html, zip(obj_generator, repeat(template),  repeat(obs_input), repeat(obs_output)))):
            pass