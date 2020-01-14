# Dump and Restore Elastic Indices on Disk

Sometimes could be helpful to dump the documents in an Elastic index on disk so that can be "cold" stored and eventually restored later. [ElasticDumper](https://github.com/pierluigi-failla/elastic_dumper/blob/master/dumper.py) allows to dump on disk a set of zipped files containing an index (or a part of it) and if necessary allows to restore it later.

Easy like:

    dumper = ElasticDumper(hosts=['localhost:9200', ], index='my_index', doc_type='my_doc_type')
  
    query = {
        'query': {
          'bool': {
              'must': [
                      {'match_all': {}},
                  ],
          },
      },
    }
    dumper.dump(query=query, dump_path='my_index_dump', raw=True)
  
this will create a folder named `my_index_dump` which contains:
  - a `mapping.json` file which contains the specific mapping for the index 
  - subfolder `data` containing one or more `data_#.zip` files
  
In order to restore you can just:

    dumper.restore(dump_path='my_index_dump', index_name='restored_my_index', doc_type='my_doc_type')
    
this will take care of creating the index with proper mapping and bulk store the data from the `data_#.zip` files.

Few customization are available, for those take a look to the code [ElasticDumper](https://github.com/pierluigi-failla/elastic_dumper/blob/master/dumper.py).
