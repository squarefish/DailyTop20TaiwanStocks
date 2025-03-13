'''
Fetch top 20 stocks with highest amounts of traded shares each day from Taiwan Stock Exchange Corporation
Load the stock data to BigQuery
'''

import os
import requests
import json
import pandas as pd
from datetime import datetime
from flask import Flask
from google.cloud import bigquery
from google.cloud import logging

# initializes a logging instance
logging_client = logging.Client()

log_name = "fetch-top20-stock-data-cr"

logger = logging_client.logger(log_name)

def get_top20_stock_data():
    # daily data should be refreshed after 2:00 pm
    url = 'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX20?response=json'

    cols = ['Ranking', 
            'StockID', 
            'StockName',
            'SharesTraded', 
            'OrdersTraded', 
            'OpeningPrice',
            'HighestPrice', 
            'LowestPrice', 
            'ClosingPrice',
            'UpsOrDowns', 
            'Spread', 
            'FinalBuyingPrice',
            'FinalSellingPrice'
            ]

    formatted_columns = ['SharesTraded', 
                         'OrdersTraded', 
                         'OpeningPrice',
                         'HighestPrice', 
                         'LowestPrice', 
                         'ClosingPrice',
                         'Spread', 
                         'FinalBuyingPrice', 
                         'FinalSellingPrice']

    stock_data = pd.DataFrame()

    try:
        
        logger.log_text(f"[Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Start to fetch stock data from TWSE.")
        req = requests.get(url)

        if req.status_code == 200:
            stock_raw_data = json.loads(req.text)
            # in case of holidays 
            if stock_raw_data['date'] == datetime.strftime(datetime.today().date(), '%Y%m%d'):
                stock_data = pd.DataFrame(stock_raw_data['data'])
                indate = datetime.strptime(stock_raw_data['date'], '%Y%m%d').date()
                stock_data.columns = cols
                stock_data['UpsOrDowns'] = [s.strip("</span>").split(">")[1] if s not in ['', 'X'] else s for s in stock_data['UpsOrDowns']]
                stock_data['InDate'] = indate

                # not all the columns listed here are strings, remove comma only from valid data types 
                for c in formatted_columns:
                    try:
                        stock_data[c] = list(map(lambda x: x.replace(',', '') , stock_data[c]))
                    except AttributeError:
                        logger.log_text(f"Data type of column {c} is not string. Skip it.")

                stock_data[formatted_columns[:2]] = stock_data[formatted_columns[:2]].astype('int')
                stock_data[formatted_columns[2:]] = stock_data[formatted_columns[2:]].astype('float')
                logger.log_text(f"[Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Successfully downloaded top 20 stock data from TWSE.")
            else:
                logger.log_text(f"No stock trading today.")
        else:
            logger.log_text(f"[Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Failed to fetch and organize stock data from TWSE.")

    except requests.exceptions.ConnectionError:
        logger.log_text(f"[Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Connection to TWSE failed.")

    return stock_data

def load_data_to_bq(bq_client, target_table_id, load_job_config, input_df):
    job = bq_client.load_table_from_dataframe(
    input_df, target_table_id, job_config=load_job_config
    ) 
    
    job.result()  

    table = bq_client.get_table(target_table_id) 

    logger.log_text("Loaded {} rows and {} columns to {}".format(
    table.num_rows, len(table.schema), target_table_id
    ))

app = Flask(__name__)

@app.route("/", methods = ["GET"])
def main():
    
    # default API output
    status_code = 200
    access_twse_msg = '-'
    load_data_to_bq_msg = '-'

    # no stock trading during weekends
    if datetime.today().weekday() <= 4: 
        try:
            input_stock_data = get_top20_stock_data()
            access_twse_msg = 'OK'
        except:
            input_stock_data = []
            status_code = 701
            access_twse_msg = 'Could not access TWSE or the source data format/fields had changed.'

        if len(input_stock_data) != 0:
            try:
                client = bigquery.Client()

                table_id = "quant-trading-430011.public_stock_exchange.daily_top20_stocks"

                job_config = bigquery.LoadJobConfig(
                    # Specify STRING data types for applicable columns
                    # No need to specify INTEGER or FLOAT data types here
                    schema=[
                        bigquery.SchemaField("StockID", bigquery.enums.SqlTypeNames.STRING),
                        bigquery.SchemaField("StockName", bigquery.enums.SqlTypeNames.STRING),
                        bigquery.SchemaField("UpsOrDowns", bigquery.enums.SqlTypeNames.STRING),
                    ],
                    # append new data
                    write_disposition="WRITE_APPEND",
                )

                load_data_to_bq(client, table_id, job_config, input_stock_data)
                logger.log_text(f"[Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Finished loading stock data to BigQuery.")
                load_data_to_bq_msg = 'Successfully load data to BigQuery.'
            except:
                status_code = 601
                load_data_to_bq_msg = 'Could not load data to BigQuery.'
        else:
            logger.log_text(f"[Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] No new stock data to be loaded to BigQuery")
    else:
        logger.log_text(f"No update today. Have a nice weekend.")
 
    return {"accessStockData": access_twse_msg, "loadDataToBQ": load_data_to_bq_msg}, status_code

if __name__ == '__main__':
    app.run(port = 9000, host = '0.0.0.0', debug = False)
