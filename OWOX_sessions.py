#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module only for OWOX session tables
"""

def recount_totalsStreaming(list_of_rows_sessions):
    """Requires only list, because it's mutable object.
    """
    for row in list_of_rows_sessions:
        if row['totalsStreaming'] is not None:
            if row['totalsStreaming']['hits'] is not None:
                count_hits=len(row['hits'])

                count_events=0
                for hit_in_row in row['hits']:
                    if hit_in_row['type'] == 'event':
                        count_events+=1

                count_pageviews=0
                for hit_in_row in row['hits']:
                    if hit_in_row['type'] == 'pageview':
                        count_pageviews+=1

                count_screenviews=0
                for hit_in_row in row['hits']:
                    if hit_in_row['type'] == 'screenview':
                        count_screenviews+=1

                count_transactions=0
                for hit_in_row in row['hits']:
                    if hit_in_row['type'] == 'transaction':
                        count_transactions+=1

                row['totalsStreaming']['hits'] = count_hits
                row['totalsStreaming']['events'] = count_events
                row['totalsStreaming']['pageviews'] = count_pageviews
                row['totalsStreaming']['screenviews'] = count_screenviews
                row['totalsStreaming']['transactions'] = count_transactions
    
    print('recounted')
    return 'recounted'
