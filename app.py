#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 15 10:47:50 2023

@author: amritramesh
"""

from polygon import RESTClient
import pandas as pd
import numpy as np
import requests
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import pymysql
from sqlalchemy import create_engine
import json

client = RESTClient(api_key="juXa0hOTZMg1lafpdK5DBrSqR0YKJnGx")

host = "database-1.csxyhdnwgd0j.us-east-2.rds.amazonaws.com"
port=int(3306)
user="admin"
passw="TeamAWSome2024$"
database="stocks"

print("THIS WORKS")

def lambda_handler(event, context):
    json_data = json.loads(event['body'])
    
    ticker = json_data['ticker']
    # ticker = event.get('ticker', 'No ticker provided')
    try:
        print(ticker)
        trade = requests.get(f'https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}?apiKey=juXa0hOTZMg1lafpdK5DBrSqR0YKJnGx')
        current_price = trade.json()['ticker']['min']['c']
        aggs = []
        for a in client.list_aggs(ticker=ticker, multiplier=1, timespan="day", from_=(datetime.today()-relativedelta(years = 1)).strftime('%Y-%m-%d'), to=datetime.today().strftime('%Y-%m-%d'), limit=50000):
            aggs.append({'open':a.open,'high':a.high,'low':a.low,'close':a.close,'volume':a.volume,'timestamp':a.timestamp})
        
        prices_before = pd.DataFrame(aggs)
        prices_before['timestamp'] = prices_before['timestamp'].apply(lambda x: datetime.fromtimestamp(x/1000))
        prices_before = prices_before.sort_values(by = ['timestamp'],ascending = [False]).reset_index(drop = True)
        month_1_price = prices_before[prices_before['timestamp']<=(datetime.today()-relativedelta(months = 1))]['close'].iloc[0]
        month_3_price = prices_before[prices_before['timestamp']<=(datetime.today()-relativedelta(months = 3))]['close'].iloc[0]
        month_6_price = prices_before[prices_before['timestamp']<=(datetime.today()-relativedelta(months = 6))]['close'].iloc[0]
        year_before_price = prices_before['close'].iloc[-1]
        year_min = prices_before['close'].min()
        year_max = prices_before['close'].max()
        stock_info = (current_price,
                               current_price/month_1_price-1,current_price/month_3_price-1,
                              current_price/month_6_price-1,current_price/year_before_price-1,
                              year_min,year_max,ticker)
        conn = pymysql.connect(host=host, user=user, port=port, passwd=passw,db=database)
        try:
            print('THIS WORKS!!!')
            with conn.cursor() as cursor:
                # SQL command to update multiple columns
                # Replace 'table_name', 'column1', 'column2',..., 'new_value1', 'new_value2',..., and 'desired_ticker' with your actual table and column names and values
                update_sql = """
                UPDATE SNP500 
                SET current_price = %s, 1_mo_performance = %s, 3_mo_performance = %s, 6_mo_performance = %s, 1_year_performance = %s, year_min = %s, year_max = %s  
                WHERE ticker = %s
                """
                print('THIS WORKS!!!!')
                # Execute the SQL command
                cursor.execute(update_sql, stock_info)
            
            # Commit the changes
            conn.commit()
        finally:
            print('THIS WORKS!!!!!')
            # Close the connection
            conn.close()
        print('THIS WORKS!!!!!!!!!')
        return {
            'statusCode': 200,
                        'headers': {
                 'Content-Type': 'application/json'
             },
            'body': json.dumps({'ticker':ticker,'current_price':current_price,
                                   '1_mo_performance':current_price/month_1_price-1,'3_mo_performance':current_price/month_3_price-1,
                                  '6_mo_performance':current_price/month_6_price-1,'1_year_performance':current_price/year_before_price-1,
                                  'year_min':year_min,'year_max':year_max})
        }
    except Exception as e:
        return {
        'statusCode': 500,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps({'error': str(e)})
    }

