from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import xml.etree.ElementTree as ET
import re
import os
from tqdm import tqdm

username = "elastic"
ELASTIC_PASSWORD = "xssoL66w+QP0ttP62MdU"
is_stem = False
file_dir = "./AP_DATA/ap89_collection"
stop_word_dir = "./AP_DATA/stoplist.txt"
stem_word_dir = "./AP_DATA/stem-classes.lst"
time_out = 60

client = Elasticsearch(
    "http://localhost:9200",  # Elasticsearch endpoint
    http_auth=(username, ELASTIC_PASSWORD),  # API key ID and secret
    timeout=time_out
)
fields = ["DOCNO", "FILEID", "FIRST", "SECOND", "HEAD", "BYLINE", "DATELINE", "TEXT"]
index = "test"


def build(index_name, max_size=None):
    stem = load_stem()
    stop = load_stop()
    if client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)
    client.indices.create(index=index_name, body=index_mapping())
    docs = get_docs()
    documents = []
    for doc in tqdm(docs):
        if max_size:
            max_size -= 1
            if max_size < 0:
                break
        documents.extend(read_doc(doc, stem, stop, index_name))
    print("uploading to elastic search")
    success, failed = bulk(client, documents)
    print(success)
    print(failed)
    print("upload complete")

    client.indices.refresh(index=index_name)
    return stem, stop


def get_docs():
    folder_path = file_dir
    docs = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            docs.append(file)
    return docs


def read_doc(filename, stem, stop, index_name):
    file_path = os.path.join(file_dir, filename)

    docs = []
    with open(file_path, 'r') as file:
        doc_content = ''
        for line in file:
            line = line.replace("&amp", " ")
            line = line.replace("&", " ")
            if '<DOC>' in line:
                doc_content = line
            elif '</DOC>' in line:
                doc_content += line
                docs.append(doc_content)
                doc_content = ''
            elif doc_content:
                doc_content += line

    processed = []
    for doc_content in docs:
        try:
            temp_root = ET.fromstring(doc_content)
            doc = {'_index': index_name}
            for child in temp_root:
                if child.tag not in fields:
                    fields.append(child.tag)
                if child.tag == "DOCNO":
                    doc[child.tag] = parse(child.text, stem, stop)
                    doc['_id'] = child.text.strip()
                else:
                    if child.tag not in doc:
                        doc[child.tag] = parse(child.text, stem, stop)
                    else:
                        doc[child.tag] = doc[child.tag] + " " + parse(child.text, stem, stop)
                # print(f"children element: {child.tag}, children content: {child.text}")
            processed.append(doc)
        except Exception as e:
            print("--" * 10)
            print(e)
            print(doc_content)
    return processed


def parse(text, stem_global, stop):
    if not is_stem:
        return text
    if not text:
        return ""
    tokens = re.findall(r'\b[^\W\d_]+\b|\d+', text.lower())
    processed = []
    for x in tokens:
        if x in stem_global:
            x = stem_global[x]

        if x not in stop:
            processed.append(x)
        # else:
        #     print(x)
    return " ".join(processed)


def load_stem():
    file_path = stem_word_dir
    stem = {}
    with open(file_path, 'r') as file:
        for line in file:
            root, list = line.split('|')
            root = root.strip()
            list = list.split(" ")
            for x in list:
                x = x.strip()
                if x:
                    stem[x] = root
    return stem


def load_stop():
    file_path = stop_word_dir
    stop = set()
    with open(file_path, 'r') as file:
        for line in file:
            stop.add(line.strip())
    return stop


def match(text, stem, stop):
    text = parse(text, stem, stop)
    body = {
        "query": {
            "multi_match": {
                "query": text,
                "fields": fields
            }
        }
    }
    return body


def match_general(text, stem, stop, size=10):
    text = parse(text, stem, stop)
    fuzzy = []
    for field in fields:
        fuzzy.append({
            "fuzzy": {
                field: {
                    "value": text,
                    "fuzziness": "AUTO"
                }
            }
        })

    body = {
        "size": size,
        "query": {
            "bool": {
                "should": [
                    {
                        "multi_match": {
                            "query": text,
                            "fields": ["DOCNO", "FILEID", "FIRST", "SECOND", "HEAD", "BYLINE", "DATELINE", "TEXT"]
                        }
                    },
                    *fuzzy
                ]
            }
        },
        "aggs": {
            "deduplicate": {
                "terms": {
                    "field": "id"
                },
                "aggs": {
                    "deduplicated_hits": {
                        "top_hits": {
                            "size": 1
                        }
                    }
                }
            }
        }
    }
    return body


def index_mapping():
    mapping = {
        "mappings": {
            "properties": {
                "id": {
                    "type": "keyword"
                },
            }
        }
    }
    return mapping


def search(query, stem, stop, size=10):
    body = match_general(query, stem, stop, size)
    fuzzy_search_result = client.search(index="test", body=body)
    result = []
    for back in fuzzy_search_result['hits']['hits']:
        result.append(output(back))
    return result


def output(doc_result):
    file = doc_result["_source"]
    s = {}
    for key in file:
        s[key] = file[key]
    return s
