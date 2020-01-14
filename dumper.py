# -*- coding: utf-8 -*-

import logging
import json
import os
from zipfile import ZipFile
from zipfile import ZIP_DEFLATED

from elasticsearch import Elasticsearch
from elasticsearch.connection import RequestsHttpConnection
from elasticsearch.helpers import bulk

class ElasticDumper(object):
    
    def __init__(self, hosts, index, doc_type):
        """ Dump your Elastic documents on disk to restore them later
        
        :param hosts: a list of ip:port pointing to the Elastic cluster
        :param index: an elastic index name
        :param doc_type: the doc_type in the index
        """
        self._hosts = hosts
        self._index = index
        self._doc_type = doc_type
        self._es = Elasticsearch(hosts=self._hosts, http_compress=True,
                           connection_class=RequestsHttpConnection,
                           timeout=30)
    
    def create_index(self, index_name, doc_type, create_if_not_exists=True, mapping=None):
        """ Create an index
        
        :param index: an Elastic index name
        :param doc_type: the doc_type in the index
        :param create_if_not_exists: if the index does not exists create it
        :param mapping: a json describing the document mapping        
        """
        if self._es.indices.exists(index_name):
            logging.debug(f'index already existing: {index_name}')
        else:
            if create_if_not_exists:
                self._es.indices.create(index_name)
                if mapping is not None:
                    self._es.indices.put_mapping(index=index_name,
                                           doc_type=doc_type,
                                           body=mapping, include_type_name=True)
        self._index = index_name
        self._doc_type = doc_type
    
    def iterate_data(self, query, page_size=100, raw=False):
        """ A generator to collect data from Elastic matching `query`

        :param query: Elastic query
        :param page_size: used for pagination (do not touch if you do not know)
        :param raw: whether return Elastic metadata too or not
        :return: a generator to the documents
        """
        try:
            for page in self._scroll(query, page_size=page_size):
                docs = page['hits']['hits']
                if len(docs) == 0:
                    break
                for doc in docs:
                    if raw is False:
                        yield doc['_source']
                    else:
                        yield doc
        except Exception as e:
            logging.warning(f'iterate_data() exception: {e}')
            raise e
            
    def _scroll(self, query, page_size=100, scroll='5m'):
        """ Internal helper to Elastc scroll
        
        :param query: Elastic query
        :param page_size: number of hits to return
        :param scroll: how long a consistent view of the index should be maintained
        """
        page = self._es.search(index=self._index, scroll=scroll, size=page_size, body=query)
        sid = page['_scroll_id']
        scroll_size = page['hits']['total']['value']
        page_counter = 0
        while scroll_size > 0:
            # get the number of results that we returned in the last scroll
            scroll_size = len(page['hits']['hits'])
            if scroll_size > 0:
                yield page
            # get next page
            page = self._es.scroll(scroll_id=sid, scroll=scroll)
            page_counter += 1
            # update the scroll ID
            sid = page['_scroll_id']
            
    def get_mapping(self, index_name, doc_type):
        """ Get mapping
        
        :param index: an Elastic index name
        :param doc_type: the doc_type in the index
        """
        return self._es.indices.get_mapping(index=index_name, doc_type=doc_type, include_type_name=True)
    
    def dump(self, query, dump_path, docs_per_file=10000, raw=False):
        """ Dump documents on disk
        
        :param query: Elastic query
        :param dump_path: a folder where to store data (if not exists it will create it)
        :param docs_per_file: how many documents will be stored in each zip file
        :param raw: whether return Elastic metadata too or not
        """
        
        def _store(buffer, count, data_path):
            # TODO: maybe padding (06d) should be improved to be dynamic
            fn = os.path.join(data_path, f'data_{count:06d}.json')
            json.dump(buffer, open(fn, 'w'))
            zip_fn = os.path.join(data_path, f'data_{count:06d}.zip')
            zip_file = ZipFile(zip_fn, mode='w', compression=ZIP_DEFLATED)
            zip_file.write(fn, arcname=f'data_{count:06d}.json')
            zip_file.close()
            os.remove(fn)
            logging.info(f'stored: {zip_fn}')
        
        if not os.path.exists(dump_path):
            os.makedirs(dump_path)
        data_path = os.path.join(dump_path, 'data')
        if not os.path.exists(data_path):
            os.makedirs(data_path)
        json.dump(self.get_mapping(index_name=self._index, doc_type=self._doc_type), open(os.path.join(dump_path, 'mapping.json'), 'w'))
        docs_count = 0
        count = 0
        buffer = []
        for d in self.iterate_data(query=query, raw=raw):
            buffer.append(d)
            if len(buffer) >= docs_per_file:
                count += 1
                _store(buffer, count, data_path)
                docs_count += len(buffer)
                buffer = []
        if len(buffer) > 0:
            count += 1
            _store(buffer, count, data_path)
            docs_count += len(buffer)
            buffer = []
        logging.info(f'dump completed: {docs_count} doc in {count} files')
        
    def restore(self, dump_path, index_name, doc_type):
        """ Restore documents from disk
        
        :param dump_path: a folder where to store data (if not exists it will create it)
        :param index: an elastic index name
        :param doc_type: the doc_type in the index
        """
        
        def _gen_bulk(data, index_name, doc_type):
            for d in data:
                if '_source' in d:  # in case dump uses raw=True
                    yield d
                else:
                    yield {
                        '_index': index_name,
                        '_type': doc_type,
                        '_source': d,
                    }
        
        if not os.path.exists(dump_path):
            raise Exception(f'not existing path: {dump_path}')
        data_path = os.path.join(dump_path, 'data')
        if not os.path.exists(data_path):
            raise Exception(f'not existing path: {data_path}')
        mapping = json.load(open(os.path.join(dump_path, 'mapping.json'), 'r'))
        self.create_index(index_name=index_name, doc_type=doc_type, mapping=mapping)
        try:
            files = sorted([f for f in os.listdir(data_path) if os.path.isfile(os.path.join(data_path, f)) and f.endswith('.zip')])
        except Exception as e:
            logging.warning(e)
            files = []
        if len(files) == 0:
            raise Exception(f'no zip files in {data_path}')
        files = [os.path.join(data_path, filename) for filename in files]
        logging.info(f'found {len(files)} zip files')
        docs_count = 0
        for f in files:
            zip_file = ZipFile(f)
            zip_file.extractall(data_path)
            zip_file.close()
            json_files = sorted([f for f in os.listdir(data_path) if os.path.isfile(os.path.join(data_path, f)) and f.endswith('.json')])
            json_files = [os.path.join(data_path, filename) for filename in json_files]
            for jf in json_files:
                buffer = json.load(open(jf, 'r'))
                bulk(self._es, _gen_bulk(data=buffer, index_name=index_name, doc_type=doc_type))
                docs_count += len(buffer)
                os.remove(jf)
            logging.info(f'restored data from: {f}')
        logging.info(f'restored {docs_count} docs in: {index_name}')
