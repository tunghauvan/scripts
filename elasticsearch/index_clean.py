import argparse
import datetime
from elasticsearch import Elasticsearch


def process_delete_indices_before_days(es, index_prefix, days_before):
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days_before)
    cutoff_date_str = cutoff_date.strftime("%Y.%m.%d")
    
    # Get all indices
    all_indices = es.indices.get(index='{}-*'.format(index_prefix), ignore_unavailable=True).keys()
    
    # Filter indices to delete
    indices_to_delete = [index for index in all_indices if index.startswith(index_prefix) and index[len(index_prefix)+1:] < cutoff_date_str]
    
    # Delete indices
    for index in indices_to_delete:
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
    
    process_delete_indices_before_days(es, args.index_prefix, args.days_before)
    
    
if __name__ == '__main__':
    main()
