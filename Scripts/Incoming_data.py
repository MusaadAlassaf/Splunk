from asyncore import write
from urllib import response
import requests as req
import json
import sys,os


# get the data from the api
def api_call(request):
    response = req.get(request)
    if response.status_code != 200:
        print(response.json())
        exit()
    data = response.json()
    return json.dumps(data)    

def stream_to_splunk(checkpoint_file,data):
    for dt in data:
        if checkpoint(checkpoint_file,str(dt["report_number"])):
            continue
        else:
            write_to_checkoint_file(checkpoint_file,str(dt["report_number"]))
            print(json.dumps(dt))

#write the id's in the checkpoint file
def write_to_checkoint_file(checkpoint_file, crash_id):
    with open(checkpoint_file, 'a') as file:
        file.writelines(crash_id + '\n')


# checks if the crash id already there or not
def checkpoint(checkpoint_file, crash_id):
    with open(checkpoint_file,'r') as file:
        id_list = file.read().splitlines()
        return(crash_id in id_list)



def main():
    request_url = "https://data.montgomerycountymd.gov/resource/bhju-22kf.json"
    checkpoint_file = os.path.join(os.environ["SPLUNK_HOME"],'etc','apps','Dynamic_Dashboard','bin','checkpoint','checkpoint.txt')
    crash_reports = api_call(request_url)
    data = json.loads(crash_reports)
    stream_to_splunk(checkpoint_file,data)
    #print(crash_reports)



if __name__ == "__main__":
    main()
