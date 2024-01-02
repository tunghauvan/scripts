import argparse
import datetime
from elasticsearch import Elasticsearch


def process_delete_index(es, index_prefix, days_before):
    date_str = (datetime.datetime.now() - datetime.timedelta(days=days_before)).strftime("%Y.%m.%d")
        
    # Get indices
    indices = es.indices.get(index='{}-{}'.format(index_prefix, date_str))
    
    # Delete indices
    for index in indices:
        es.indices.delete(index=index)
        print('deleted index: {}'.format(index))
        
    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--index-prefix', type=str, required=True)
    parser.add_argument('--days-before', type=int, required=True)
    parser.add_argument('--host', type=str, default='localhost:9200')
    
    args = parser.parse_args()
    
    print('index_prefix: {}'.format(args.index_prefix))
    print('days_before: {}'.format(args.days_before))
        
    es = Elasticsearch(args.host)
    
    process_delete_index(es, args.index_prefix, args.days_before)
    
    
if __name__ == '__main__':
    main()