#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Little helper with GBQ
Has no doctest
"""
import numpy as np
from google.cloud import bigquery
import requests


class TableNamesError():pass

def credentials(KEY_FILE):
    """Input filepath to json file to get credentials
    """
    bigquery_client = bigquery.Client.from_service_account_json(KEY_FILE)
    return bigquery_client

def get_datasets(bigquery_client):
    """Requires credentials and returns list of dataset names (dataset_ids)
    Returns ids and refs
    """
    dataset_id=[]
    dataset_ref=[]
    buckets = list(bigquery_client.list_datasets())
    for bucket in buckets:
        dataset_id.append(bucket.dataset_id)
        dataset_ref.append(client.dataset(bucket.dataset_id))    
    return dataset_id, dataset_ref

def get_tables(bigquery_client,datasetId,include=None,exclude=None):
    """ Requires: credentials, exact name of dataset, include - REGEX, exclude - REGEX
        Returns table_ids and table_refs where find 'include' and exclude 'exclude' 
        or all if both None or just exclude some tables
    """    
    table_ids = []
    table_refs = []
    buckets = list(bigquery_client.list_datasets())
    
    for bucket in buckets:
        if bucket.dataset_id == datasetId:
            datasetRef = bigquery_client.dataset(bucket.dataset_id)
            tables = list(bigquery_client.list_dataset_tables(datasetRef))

    if include and not exclude:
        for table in tables:
            if include in table.table_id:
                table_ids.append(table.table_id)
                table_refs.append(datasetRef.table(table.table_id))      
        print('got include but not exclude')
        return table_ids,table_refs
    
    elif include and exclude:
        for table in tables:
            if (include in table.table_id) and (exclude not in table.table_id):
                table_ids.append(table.table_id)
                table_refs.append(datasetRef.table(table.table_id)) 
        print('got include and exclude')
        return table_ids,table_refs
    
    elif not include and exclude:
        for table in tables:
            if exclude not in table.table_id:
                table_ids.append(table.table_id)
                table_refs.append(datasetRef.table(table.table_id)) 
        print('got just exclude')
        return table_ids,table_refs
    
    elif not include and not exclude:
        for table in tables:
            table_ids.append(table.table_id)
            table_refs.append(datasetRef.table(table.table_id))
        print('no any parametrs, return all tables')
        return table_ids,table_refs
    else:
        raise TableNamesError('something wrong in getting tables')

def get_schema_from_query(bigquery_client, query, legacy=False):
    """ Requires credentials,query and dialect, defaul dialect StandardSQL
        Returns list schema for creating table
        
        Use small queries with 2 rows
    """   
    try:
        job_config = bigquery.QueryJobConfig()
        job_config.use_legacy_sql = legacy
        query_job = bigquery_client.query(query, job_config=job_config)
        results=query_job.result()
        schema=results.schema
        print("got schema")
        return schema
    except Exception as err:
        exception='not got schema, because {0}'.format(err)
        print(exception)

def create_table_from_schema (bigquery_client, schema, table, dataset='test'):
    """Requires: credentials, schema, table name and dataset where table will be created
    Output: if created returns 'created' else 'not created'    
    
    Can't create 2 identical tables
    Creates table from path and schema
    """    
    try:
        buckets = list(bigquery_client.list_datasets())
        for bucket in buckets:
            if bucket.dataset_id == dataset:
                dataset_ref=bigquery_client.dataset(bucket.dataset_id)
        table_ref = dataset_ref.table(table)
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print('table ctreated')        
        return 'created'
    except Exception as err:
        exception='table not created, because {0}'.format(err)
        print(exception)
        return 'not created'

def get_iterable_results (bigquery_client, query, dialect='standard'):
    """Requires credentials, query, dialect type
    
    Returns iterator, usable only 1 time
    If data need more than once, transform them in list
    """   
    try:
        job_config = bigquery.QueryJobConfig()
        if dialect == 'standard':
            job_config.use_legacy_sql = False
        elif dialect == 'legacy':
            job_config.use_legacy_sql = True
        query_job = bigquery_client.query(query, job_config=job_config)
        results=query_job.result()    
        print('results received')
        return results    
    except Exception as err:
        exception='results not received, because {0}'.format(err)
        print(exception)
        return 'no results'

def write_results(bigquery_client,results,dataset,table):
    """ Requires credentials, results - bigquery RowIterator or list of dicts, dataset, table name
        Returns 'results written', or prints exception with text
    """
    try:
        buckets = list(bigquery_client.list_datasets())
        for bucket in buckets:
            if bucket.dataset_id == dataset:
                dataset_ref=bucket
        table_ref = dataset_ref.table(table)
        dest_table = bigquery_client.get_table(table_ref)
        rows=results
        bigquery_client.create_rows(dest_table, rows)
        print('results written')
        return 'results written'
    
    except Exception as err:
        exception='results not written, because {0}'.format(err)
        print(exception)
        return 'results not written'

def rows_names_from_schema(schema):
    """Returns row names from schema
    """
    list_names=[]
    for row in schema:
        list_names.append(row.name)
    return list_names

def transform_results_to_list_of_dicts(result,shema):
    """Requires results, schema and function rows_names_from_schema
    Returns list of dicts (rows)
    
    """
    try:
        list_of_rows=[]
        list_of_names=rows_names_from_schema(shema)
        for row in result:
            dict_of_row={}
            for name in list_of_names:
                dict_of_row[name]=row[name]
            list_of_rows.append(dict_of_row)
        print('got rows')
        return list_of_rows
    except Exception as err:
        print('not got rows because '+str(err))