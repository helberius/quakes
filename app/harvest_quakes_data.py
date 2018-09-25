import requests
from elasticsearch import Elasticsearch

import json
from math import ceil
import sys

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


def harvest_earthquakes(period):
    """ method to get the earthquakes from usgs """
    url = None
    dict_response = {}
    if period == 'last_hour':
        url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'
    elif period == 'last_day':
        url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson'
    elif period == 'last_week':
        url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson'
    elif period == 'last_month':
        url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'

    if url is not None:
        r = requests.get(url)
        response_as_dict = json.loads(r.content)
        features = response_as_dict['features']
        dict_response['features'] = features
        dict_response['result'] = 'success'
        return dict_response
    else:
        dict_response['response'] = 'fail'
        dict_response['error'] = 'The keyword provided does not correspond to any valid period.'
        return dict_response


def load_data_in_es(ls_docs, index):
    print('----- data loading into elasticsearch ----')
    ls_indexes = list_indexes()
    if index in ls_indexes:
        print('the index already exist')

    else:
        print('the index does not exist, i will create one')
        es.indices.create(index=index, ignore=400)

    query_count = {"query": {"match_all": {}}}
    count = es.count(index=index, body=query_count)
    print('number of features in ' + index + ' :' + str(count['count']))

    i = 1
    for d in ls_docs:
        d=add_days_to_data(d)
        es.index(index=index, doc_type='quake_summary', id=d['id'], body=d)

    count = es.count(index=index, body=query_count)
    print('after loading data, number of features in ' + index + ' :' + str(count['count']))
    print('----- ------------------------')

def search_quakes(keyword):
    """ search one earthquake by id """
    dict_query = {"from":0, "size":1000 ,"query": {"match": {'properties.place': keyword}}}
    y = es.search(index='usgs', body=dict_query)
    print(json.dumps(y, indent=4, sort_keys=True))

def list_indexes():
    """
        An index is a collection of documents that have somewhat similar characteristics.
    For example, you can have an index for customer data, another index for a product catalog,
    and yet another index for order data. An index is identified by a name (that must be all lowercase)
    and this name is used to refer to the index when performing indexing, search, update, and delete
    operations against the documents in it.
    """
    ls_indices = es.indices.get_alias("*")
    return ls_indices

def delete_by_index(index):
    """
    to delete an index,
    """
    try:
        es.indices.delete(index=index, ignore=[400, 4004])
        print('the index ' + index + ' has been eliminated.')
    except Exception as err:
        print(err)

def add_days_to_data(dict_quake):
    number_miliseconds_per_day=86400000
    days= ceil(dict_quake['properties']['time']/number_miliseconds_per_day)
    dict_quake['properties']['days']=days
    return dict_quake

def update_values_quakes(ls_quakes, index):
    for q in ls_quakes:
        updated_q=add_days_to_data(q['_source'])
        es.update(index=index, doc_type='quake_summary', id=updated_q['id'], body={"doc":updated_q})

def search_quakes_by_keyword_place(index, keyword):
    print ('testing query by keyword')
    dict_query={"from":0, "size":1000,"query":{"match":{'properties.place':keyword}}}
    result_query = es.search(index=index,body=dict_query)
    return result_query

def get_all_docs(index,number_of_docs):
    dict_query={"from":0, "size":number_of_docs ,"query":{"match_all":{}}}
    result_query = es.search(index=index,body=dict_query)
    return result_query

def get_docs_with_no_days(index, number_of_docs):
    dict_query = {"from": 0, "size": number_of_docs, "query": {"bool":{"must_not":{"exists":{"field":"properties.days"}}}}}
    result_query = es.search(index=index,body=dict_query)
    return result_query

def get_counts_per_day(index, keyword):
    dict_query={"size":0,"aggs":{"group_by_day":{"terms":{"field":"properties.days"}}}, "query":{"match":{'properties.place':keyword}}}
    result_query = es.search(index=index,body=dict_query)
    return result_query

def harvest_quakes_by_time_period():

    ls_requests=[]
    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime=2018-01-01 00:00:00&endtime=2018-01-15 00:00:00&minmagnitude=0&orderby=time')
    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime=2018-01-15 00:00:00&endtime=2018-02-01 00:00:00&minmagnitude=0&orderby=time')
    #ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime=2018-02-01 00:00:00&endtime=2018-03-01 00:00:00&minmagnitude=0&orderby=time')
    #ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime=2018-03-01 00:00:00&endtime=2018-04-01 00:00:00&minmagnitude=0&orderby=time')
    #ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime=2018-04-01 00:00:00&endtime=2018-05-01 00:00:00&minmagnitude=0&orderby=time')
    #ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime=2018-05-01 00:00:00&endtime=2018-06-01 00:00:00&minmagnitude=0&orderby=time')
    #ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime=2018-06-01 00:00:00&endtime=2018-06-15 00:00:00&minmagnitude=0&orderby=time')
    #ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime=2018-06-15 00:00:00&endtime=2018-07-01 00:00:00&minmagnitude=0&orderby=time')
    #ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime=2018-07-01 00:00:00&endtime=2018-07-15 00:00:00&minmagnitude=0&orderby=time')
    #ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime=2018-07-15 00:00:00&endtime=2018-08-01 00:00:00&minmagnitude=0&orderby=time')
    #ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime=2018-08-01 00:00:00&endtime=2018-08-15 00:00:00&minmagnitude=0&orderby=time')
    #ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime=2018-08-15 00:00:00&endtime=2018-09-01 00:00:00&minmagnitude=0&orderby=time')
    #ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime=2018-08-01 00:00:00&endtime=2018-09-01 00:00:00&minmagnitude=0&orderby=time')

    dict_response={}
    for url in ls_requests:
        print(url)
        r = requests.get(url)
        response_as_dict = json.loads(r.content)
        load_data_in_es(response_as_dict['features'], 'usgs')

if __name__ == '__main__':
    print (sys.argv)
    period=sys.argv[1]
    print('selected period: ',period)

    #--------------------------------------
    """ normal operation """
    harvested_quakes = harvest_earthquakes(period)
    load_data_in_es(harvested_quakes['features'], 'usgs')
    #--------------------------------------

    #----------------------------------------------
    """ 
        special operation harvest quakes from specific time periods
        the function harvest_quakes_by_time_period() contains a list of urls for the months from january to august 2018
        It will request and upload the data into elasticsearch for those periods. 
    """
    #harvest_quakes_by_time_period()
    # ------------------------------------------

    #---------------------------------------------------
    """special operation, to update the key days to quakes"""
    # ls_quakes =get_docs_with_no_days('usgs',2000)
    # print (len(ls_quakes['hits']['hits']))
    # print(ls_quakes['hits']['hits'])
    # update_values_quakes(ls_quakes['hits']['hits'], 'usgs')
    #---------------------------------------------------

    #------------------------------------------
    """ special operation to delete an index """
    # delete_by_index('usgs')

    #------------------------------------------
    """ special operation to group/count by days """
    # delete_by_index('usgs')

    #ls_counts=get_counts_per_day('usgs','indonesia')
    #print(len(ls_counts['hits']['hits']))
    #print(ls_counts['aggregations']['group_by_day']['buckets'])