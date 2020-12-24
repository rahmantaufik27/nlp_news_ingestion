import requests
import random 
from collections import OrderedDict
from headers_agent_list import list_header

def list_dict():
    # Get headers list
    headers_list = list_header()
    # Create ordered dict from Headers above
    ordered_headers_list = []
    for headers in headers_list:
        h = OrderedDict()
        for header,value in headers.items():
            h[header]=value
        ordered_headers_list.append(h)
    return ordered_headers_list

def list_test():
    headers_list = list_dict()
    max = len(headers_list)
    url = 'https://httpbin.org/headers'
    for i in range(0,max):
        #Pick a random browser headers
        headers = random.choice(headers_list)
        #Create a request session
        r = requests.Session()
        r.headers = headers
        
        response = r.get(url)
        print("Request #%d\nUser-Agent Sent:%s\n\nHeaders Recevied by HTTPBin:"%(i,headers['User-Agent']))
        print(response.json())
        print("-------------------")

def random_header():
    headers_list = list_dict()
    headers = random.choice(headers_list)
    return headers