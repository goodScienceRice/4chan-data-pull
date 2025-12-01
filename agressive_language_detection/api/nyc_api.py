from typing import Union
import json
import openai
import os
import numpy as np
from prompts import *
import requests

from fastapi import FastAPI, File, UploadFile
from pydantic import BaseModel

from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from bertopic.representation import OpenAI

class ProcessFile(BaseModel):
    name: str
    channel: str | None = None


app = FastAPI()

api_url = '3.239.235.136:8081'

llm_model="aifeifei798/DarkIdol-Llama-3.1-8B-Instruct-1.2-Uncensored"

client = openai.OpenAI(
    base_url=f'http://{api_url}/v1',
    api_key='nycc-epigenesis'
)
representation_model = OpenAI(
    client, model=llm_model, chat=True, prompt=summarization_prompt, nr_docs=5, delay_in_seconds=3
)
vectorizer_model = CountVectorizer(stop_words="english")

topic_model = BERTopic(
    representation_model=representation_model,
    vectorizer_model=vectorizer_model
)

context = {
    'client': client,
    'processed_file':'',
    'result': None,
    'topic_model': topic_model,
    'last_tm_file': ''
}


def detect_aggresive_language(text):

    try:
        client = context['client']
        completion = client.chat.completions.create(
        #model="meta-llama/Meta-Llama-3.1-8B-Instruct",
        model=llm_model,
        messages=[
            {"role": "system", "content": sm_arrgessive_language_label },
            {"role": "user", "content": text}
            ]
        )

        return "success", json.dumps(completion.choices[0].message.content)
    except Exception as e:
        return "error",str(e)


def generate_add_copy(text):

    try:
        client = context['client']
        completion = client.chat.completions.create(
            model=llm_model,
            messages=[
                {"role": "system", "content": add_copy_prompt},
                {"role": "user", "content": text}
            ]
        )

        return "success", json.dumps(completion.choices[0].message.content)
    except Exception as e:
        return "error",str(e)

@app.get("/")
def read_root():
    return {"Threat Detection"}

@app.post("/predictone/")
def process_one(text):
    """ Process one string"""

    return detect_aggresive_language(text)

@app.post("/updateclient/")
def update_client(url: str):
    """ Updates URL of the client """
    
    client = openai.OpenAI(
        base_url=f'http://{api_url}/v1',
        api_key='nycc-epigenesis'
    )
    representation_model = OpenAI(
        client, model=llm_model, chat=True, prompt=summarization_prompt, nr_docs=5, delay_in_seconds=3
    )
    
    topic_model = BERTopic(
        representation_model=representation_model,
        vectorizer_model=vectorizer_model
    )
    
    try:
        context['client'] = client
        context['topic_model'] = topic_model
        
        return 'success'
    except Exception as e:
        return str(e)    

@app.post("/predict/")
def process_file(file: ProcessFile):
    """ Make predictions on the dataset idendified by name. 
        Save the prediction in s3 bucket. The predictions on the file name,
        that already have been processed, are not repeated, but instead, loaded
        from the processed and saved file in S3.
    """
    #pth = '/nycc_data/raw'
    pth = '/nycc_data/'
    fname = file.name
    
    outfname = fname.replace('/','_')
    res_path = os.path.join('/nycc_data/processed/',f'{outfname}_processed.json')
    
    if os.path.exists(res_path):
        with open(res_path,'r') as inf:
            resp = json.load(inf)
    else:
        resp = []
        with open(os.path.join(pth,file.name),'r') as inf:
            for l in inf.readlines():
                try:
                    _, pred = detect_aggresive_language(l)
                    try:    
                        threat_level, justification = pred.split('threat_level_justification')
                        threat_level = "".join(threat_level.split(' ')[1:])

                        d = {
                            'threat_level':threat_level,
                            'justification': justification,
                            'text': l
                        }
                        resp.append(d)
                    except:
                        pass
                    #d['text'] = l
                    #resp.append(d)
                    #resp.append({
                    #    "text": l,
                    #    "threat_level":detect_aggresive_language(l)
                    #})
                except Exception as e:
                    print(e)
                    pass
            
        with open(os.path.join('/nycc_data/processed/',f'{outfname}_processed.json'),'w') as outf:
            json.dump(resp,outf)
            
    context['file_processed'] = file.name
    context['result'] = resp

    if file.channel == 'vantiq':
        url = "https://dev.vantiq.com/api/v1/resources/services/com.epigen.signal.ModelReturn/Return?token=O0QwbuIAViMVXAX_FGBbpAh5Rv-h6m77-tyuxOdvrW4="
        
        r = requests.post(
            url,
            json=resp
        )

    #return 'success', resp
    return resp


@app.get("/result/")
def get_result():
    """ Updates URL of the client """

    try:
        return context['result']
    except:
        return {'success'}


@app.post("/uploadfile/")
async def create_upload_file(
    file: UploadFile = File(description="A file read as UploadFile"),
):
    file_path = os.path.join('/nycc_data/raw/',file.filename)
    if os.path.exists(file_path): return {'file exists'}
    contents = file.file.read()
    with open(file_path,'wb') as outf:
        outf.write(contents)
        
    return {"success"}


@app.post("/generatetopics/")
def generate_topics(file: ProcessFile):
    """ Generate topics on the file """
    #pth = '/nycc_data/raw/'
    #pth = '/nycc_data/raw'
    pth = '/nycc_data/'
    fname = file.name
    #outfname = fname.replace('/','_')    
    #fname = file.name
    
    resp = []
    docs = []
    
    with open(os.path.join(pth,file.name),'r') as inf:
        for l in inf.readlines(): docs.append(l)

    topic_model = context['topic_model']
    if context['last_tm_file'] != file.name:
        
        topics, probs = topic_model.fit_transform(docs)
        context['topic_model'] = topic_model
        
    context['docs'] = docs
    context['last_tm_file'] = file.name

    #return json.dumps(zip(docs,context['topic_model'].topics_) )
    return 'success'


@app.get("/topics/")
def topics():
    """ Topics IDs for all documents """
    return json.dumps(context['topic_model'].topics_)

@app.get("/representations/")
def topics_reps():
    """ Topic representations / summareis """
    d = {i:t[0][0] for i,t in context['topic_model'].topic_representations_.items()}
    return d

@app.get("/counts/")
def word_counts():
    """ Counts/frequencies for 10 most common words per topic """
    representations = context['topic_model'].topic_representations_
    topics = np.array(context['topic_model'].topics_)
    vectorizer = CountVectorizer(stop_words="english")
    cv = vectorizer.fit_transform(context['docs'])
    words = vectorizer.get_feature_names_out()
    tot_w = cv.sum()
    
    resp = {}
    
    for i in representations:
        
        ind = topics == i
        total_counts = cv[ind,:].sum(axis=0)
        idx = np.argsort(total_counts)
        counts = np.array(total_counts[0,idx][0,-10:]).flatten()
        widx = np.array(idx[0,-10:]).flatten()
        wrds = words[widx]

        resp[i] = {w:(int(c),int(c)/tot_w) for w,c in zip(wrds,counts)}

    return resp


@app.post("/addcopy/")
def addcopy(text):
    """ Process one string"""

    return generate_add_copy(text)

@app.post("/listprocessed/")
def listproc():
    """ List processed datafiles"""

    return os.listdir('/nycc_data/processed/')

    
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