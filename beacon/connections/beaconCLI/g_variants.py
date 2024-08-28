from beacon.request.parameters import RequestParams
from beacon.response.schemas import DefaultSchemas
from typing import Optional
from beacon.connections.beaconCLI.__init__ import client
import subprocess
from beacon.logs.logs import LOG

def get_variants(entry_id: Optional[str], qparams: RequestParams, dataset: str):
    stdin, stdout, stderr = client.exec_command('cd /CLItest && python3 main.py -rg 37 -c 1 -p 1 --range 1000000000 --public')
    try:
        bash = stdout.read()
    except subprocess.CalledProcessError as e:
        output = e.output
        LOG.debug(output)
    bash_list = bash.split(b'\n')

    for item in bash_list:
        item = item.decode("utf-8") 
        item = item.replace('[', '')
        item = item.replace(']', '')
        item = item.replace('(', '')
        item = item.replace(')', '')
        item = item.replace(' ', '')
        item = item.replace("'", '')
        item = item.replace('"', '')
        item_list = item.split(',')
        #boolean = item_list[0]
        count = int(item_list[1])
        dataset_count=count
        #end = len(item_list)
        #datasets_list = item_list[2:end]

        break
    docs={}
    schema = DefaultSchemas.GENOMICVARIATIONS

    return schema, count, dataset_count, docs, dataset