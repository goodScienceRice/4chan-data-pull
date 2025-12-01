from typing import Union
import json
import openai
import os

import requests

from fastapi import FastAPI, File, UploadFile, Request
from pydantic import BaseModel

from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from bertopic.representation import OpenAI
from utils import *

from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from dataclasses import dataclass
from concurrent.futures import ProcessPoolExecutor
import time
import asyncio
import uuid

class ProcessFile(BaseModel):
    name: str
    channel: str | None = None

class ConfigItem(BaseModel):
    name: str
    value: str | None = None

@dataclass
class Item:
    id: str
    #name: str
    task: str
    file: str
    channel: str


api_url = '13.220.128.167:8000'

llm_model="aifeifei798/DarkIdol-Llama-3.1-8B-Instruct-1.2-Uncensored"

client = openai.OpenAI(
    base_url=f'http://{api_url}/v1',
    api_key='nycc-epigenesis'
)

#representation_model = OpenAI(
#    client, model=llm_model, chat=True, prompt=summarization_prompt, nr_docs=5, delay_in_seconds=3
#)
#vectorizer_model = CountVectorizer(stop_words="english")

#topic_model = BERTopic(
#    representation_model=representation_model,
#    vectorizer_model=vectorizer_model
#)

context = {
    'client': client,
    'llm_model': llm_model,
    'temperature': 0.3,
    'processed_file':'',
    'result': None,
    #'topic_model': topic_model,
    #'last_tm_file': '',
    'separator':'-----',
    'vantiq_post_url': "https://dev.vantiq.com/api/v1/resources/services/com.epigen.signal.ModelReturn/Return?token=O0QwbuIAViMVXAX_FGBbpAh5Rv-h6m77-tyuxOdvrW4="

}


# Computationally Intensive Task
def cpu_bound_task(item: Item):
    print(f"Processing: {item.file}")

    #doing prediction on the file
    res = []
    print(f"Processing task {item.task}/{item.id}")
    if item.task == 'predict':

        kwargs  = {k: context[k] for k in ['client','llm_model','temperature','separator']}
        res = process_file(item.file,**kwargs)
        
    if item.task == 'topics':

        kwargs  = {k: context[k] for k in ['client','llm_model','temperature']}
        res = generate_topics(item.file,**kwargs)
        try:
            print(res['summaries'])
        except Exception as e:
            print(e)

    if item.channel == 'vantiq':
        print(f"Uploading results of {item.task}/{item.id} to Vantiq")
        url = context['vantiq_post_url']
        
        r = requests.post(
            url,
            json={'result':res,'id':item.id}
        )
        #print(str(r))

    res['id'] = item.id
    return res
    
async def process_requests(q: asyncio.Queue, pool: ProcessPoolExecutor):
    while True:
        item = await q.get()  
        db[item.id] = {'status':'in_progress'}
        
        loop = asyncio.get_running_loop()
        r = await loop.run_in_executor(pool, cpu_bound_task, item)

        q.task_done()  # tell the queue that the processing on the task is completed
        db[item.id] = {
            'status':'complete',
            'result': r
        }

@asynccontextmanager
async def lifespan(app: FastAPI):
    q = asyncio.Queue()  # note that asyncio.Queue() is not thread safe
    pool = ProcessPoolExecutor()
    asyncio.create_task(process_requests(q, pool))  # Start the requests processing task
    yield {'q': q, 'pool': pool}
    pool.shutdown()  

db = {}

app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    return {"Threat Detection"}

@app.post("/predictone")
def process_one(text):
    """ Process one string"""

    return detect_aggresive_language(text)

@app.post("/configure")
def update_context(item: ConfigItem):
    """ Updates API context item """
    
    try:
        context[item.name] = item.value        
        return 'success'
    except Exception as e:
        return str(e) 

#@app.post("/updateclient/")
#def update_client(url: str):
#    """ Updates URL of the client """
    
#    client = openai.OpenAI(
#        base_url=f'http://{api_url}/v1',
#        api_key='nycc-epigenesis'
#    )
#    representation_model = OpenAI(
#        client, model=llm_model, chat=True, prompt=summarization_prompt, nr_docs=5, delay_in_seconds=3
#    )
    
#    topic_model = BERTopic(
#        representation_model=representation_model,
#        vectorizer_model=vectorizer_model
#    )
    
#    try:
#        context['client'] = client
#        context['topic_model'] = topic_model
        #
#        return 'success'
#    except Exception as e:
#        return str(e)    

@app.post("/predict")
async def predict(request: Request, file: ProcessFile):
    """ Make predictions on the dataset idendified by name. 
        Save the prediction in s3 bucket. The predictions on the file name,
        that already have been processed, are not repeated, but instead, loaded
        from the processed and saved file in S3.
    """

    item_id = str(uuid.uuid4())
    item = Item(item_id,'predict',file.name,file.channel)
    #print(item)
    request.state.q.put_nowait(item)  # Add request to the queue
    
    return item_id
    


#@app.get("/result")
#def get_result():
#    """ Updates URL of the client """

#    try:
#        return context['result']
#    except:
#        return {'success'}

@app.get("/getdb")
def get_db():
    """ Updates URL of the client """

    return db

@app.get("/getitem")
async def get_item(item_id: str):
    if item_id in db:
        return db[item_id]
    else:
        return JSONResponse("Item ID Not Found", status_code=404)

@app.post("/uploadfile")
async def create_upload_file(
    file: UploadFile = File(description="A file read as UploadFile"),
):
    file_path = os.path.join('/nycc_data/raw/',file.filename)
    if os.path.exists(file_path): return {'file exists'}
    contents = file.file.read()
    with open(file_path,'wb') as outf:
        outf.write(contents)
        
    return {"success"}


@app.post("/generatetopics")
def gen_topics(request: Request, file: ProcessFile):
    """ Generate topics on the file """

    item_id = str(uuid.uuid4())
    item = Item(item_id,'topics',file.name,file.channel)
    #print(item)
    request.state.q.put_nowait(item)  # Add request to the queue
    db[item_id] = 'pending'
    return item_id


#@app.get("/topics")
#def topics():
#    """ Topics IDs for all documents """
#    return json.dumps(context['topic_model'].topics_)

#@app.get("/representations")
#def topics_reps():
#    """ Topic representations / summareis """
#    d = {i:t[0][0] for i,t in context['topic_model'].topic_representations_.items()}
#    return d

#@app.get("/counts")
#def word_counts():
#    """ Counts/frequencies for 10 most common words per topic """
#    representations = context['topic_model'].topic_representations_
#    topics = np.array(context['topic_model'].topics_)
#    vectorizer = CountVectorizer(stop_words="english")
#    cv = vectorizer.fit_transform(context['docs'])
#    words = vectorizer.get_feature_names_out()
#    tot_w = cv.sum()
    
#    resp = {}
    
#    for i in representations:
        
#        ind = topics == i
#        total_counts = cv[ind,:].sum(axis=0)
##        idx = np.argsort(total_counts)
#        counts = np.array(total_counts[0,idx][0,-10:]).flatten()
#        widx = np.array(idx[0,-10:]).flatten()
#        wrds = words[widx]

#        resp[i] = {w:(int(c),int(c)/tot_w) for w,c in zip(wrds,counts)}

#    return resp


@app.post("/addcopy/")
def addcopy(text):
    """ Create add copy based on input text"""

    kwargs  = {k: context[k] for k in ['client','llm_model','temperature']}
    return generate_add_copy(text,**kwargs)

@app.get("/listprocessed/")
def listproc():
    """ List processed datafiles"""

    return os.listdir('/nycc_data/processed/')


@app.get("/getprocessed/")
def getproc(filename: str):
    """ List processed datafiles"""

    res_path = os.path.join('/nycc_data/processed/',filename)
    if os.path.exists(res_path):
        with open(res_path,'r') as inf:
            resp = json.load(inf)
        return resp

    return JSONResponse("Requested Item Not Found", status_code=404)


    
#@app.get("/tfidf/")
#def tfidf():
#    """ TFIDF for 10 most common words per topic """
#    representations = context['topic_model'].topic_representations_
#    topics = np.array(context['topic_model'].topics_)
#    vectorizer = TfidfVectorizer(stop_words="english")
#    cv = vectorizer.fit_transform(context['docs'])
#    words = vectorizer.get_feature_names_out()
#    
#    resp = {}
#    
#    for i in representations:
#        
#        ind = topics == i
#        total_counts = cv[ind,:].mean(axis=0)
#        idx = np.argsort(total_counts)
#        counts = np.array(total_counts[0,idx][0,-10:]).flatten()
#        widx = np.array(idx[0,-10:]).flatten()
#        wrds = words[widx]

#        resp[i] = {w:float(c) for w,c in zip(wrds,counts)}

#    return resp

#@app.get("/addcopy/")
#def add_copy():

#@app.get("/vistopics/")
#def vistopics():
#    """ Topics vizualization"""
#    return context['topic_model'].visualize_topics().to_html()