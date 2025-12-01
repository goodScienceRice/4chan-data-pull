# utils.py ---- Epigenesis 
# author Nikolai Shaposhnikov
# ---- 29/9/2024 --------------

import requests
import json
import numpy as np
import os
from prompts import sm_arrgessive_language_label, add_copy_prompt, summarization_prompt

from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from bertopic.representation import OpenAI


def detect_aggresive_language(
    text,
    **kwargs
):

    print(kwargs)
    try:
        completion = kwargs['client'].chat.completions.create(
            model=kwargs['llm_model'],
            temperature=kwargs['temperature'],
            #max_tokens=6,
            messages=[
                {"role": "system", "content": sm_arrgessive_language_label},
                {"role": "user", "content": text}
            ]
        )

        return "success", json.dumps(completion.choices[0].message.content)
    except Exception as e:
        return "error",str(e)

def process_file(file,**kwargs):
    """ Make predictions on the dataset idendified by name. 
        Save the prediction in s3 bucket. The predictions on the file name,
        that already have been processed, are not repeated, but instead, loaded
        from the processed and saved file in S3.
    """
    
    pth = '/nycc_data/'
    fname = file
    sep = kwargs['separator']
    max_words = 50
    outfname = fname.replace('/','_')
    res_path = os.path.join('/nycc_data/processed/',f'{outfname}_processed.json')

    if os.path.exists(res_path):
        with open(res_path,'r') as inf:
            resp = json.load(inf)
    else:
        resp = []
        with open(os.path.join(pth,file),'r') as inf:

            pred_txt = ""

            while True:
                
                l = inf.readline()
            
                if l.startswith('---'): continue
                if l.startswith('&gt;&gt;'): continue
                
                pred_txt = pred_txt + " " + l
                
                if len(pred_txt.split()) < max_words and l != '': continue
                
                try:
                    rsp = detect_aggresive_language(pred_txt,**kwargs)
                    _, pred = rsp
                    
                    threat_level, justification = pred.split('threat_level_justification')
                    threat_level = "".join(threat_level.split(' ')[1:])

                    d = {
                        'threat_level':threat_level,
                        'justification': justification,
                        'response': pred,
                        'status': 'ok',
                        'text': pred_txt
                    }
                    
                except Exception as e:
                    d = {
                        'threat_level': '',
                        'justification': '',
                        'response': pred,
                        'status': 'error',
                        'text': pred_txt,
                        'error':str(e)
                    }
                    print(e)
                    pass
                print(pred_txt)
                if d['status'] == 'error':
                    continue
                else:
                    resp.append(d)
                    pred_txt = ""

                # end of the file reached
                if l == '': break

        with open(res_path,'w') as outf:
            json.dump(resp,outf)

    return resp


def generate_topics(file,**kwargs):
    """ Generate topics on the file """

    pth = '/nycc_data/processed/'
    fname = file
    
    outfname = fname.replace('/','_')
    res_path = os.path.join('/nycc_data/topics/',f'{fname}_topics.json')

    if os.path.exists(res_path):
        with open(res_path,'r') as inf:
            resp = json.load(inf)

    else:
        representation_model = OpenAI(
            kwargs['client'], 
            model=kwargs['llm_model'], 
            chat=True, prompt=summarization_prompt, nr_docs=5, delay_in_seconds=3
        )
        vectorizer_model = CountVectorizer(stop_words="english")
        
        topic_model = BERTopic(
            representation_model=representation_model,
            vectorizer_model=vectorizer_model
        )   
    
        
        with open(os.path.join(pth,file),'r') as inf:
            js = json.load(inf)
        
        docs = [it['text'] for it in js]
    
        topics, probs = topic_model.fit_transform(docs)
        
        representations = topic_model.topic_representations_
        topics = np.array(topic_model.topics_)
        vectorizer = CountVectorizer(stop_words="english")
        cv = vectorizer.fit_transform(docs)
        words = vectorizer.get_feature_names_out()
        tot_w = cv.sum()
        
        resp = {'docs':docs}
    
        word_dict = {}
        summaries = {}
        for i in representations:
            summaries[int(i)] = representations[i][0][0]
            ind = topics == i
            total_counts = cv[ind,:].sum(axis=0)
            idx = np.argsort(total_counts)
            counts = np.array(total_counts[0,idx][0,-10:]).flatten()
            widx = np.array(idx[0,-10:]).flatten()
            wrds = words[widx]
    
            word_dict[i] = {w:str(int(c)) for w,c in zip(wrds,counts)}
    
    
        resp = {
            'docs':docs,
            'topics_words': word_dict,
            'topics':list([int(t) for t in topics]),
            'summaries': summaries,
            'filename': file
        }

        with open(res_path,'w') as outf:
            json.dump(resp,outf)

    return resp

def generate_add_copy(text,client,**kwargs):

    try:
        
        completion = client.chat.completions.create(
            model=kwargs['llm_model'],
            temperature=kwargs['temperature'],
            messages=[
                {"role": "system", "content": add_copy_prompt},
                {"role": "user", "content": text}
            ]
        )

        return "success", json.dumps(completion.choices[0].message.content)
    except Exception as e:
        return "error",str(e)