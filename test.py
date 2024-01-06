import os


from webull_options.webull_options import WebullOptions
from webull_options.webull_trader import WebullTrader

opts = WebullOptions(access_token=os.environ.get('ACCESS_TOKEN'), did=os.environ.get('DID'), osv=os.environ.get('OSV'), database='fudstop', user='postgres')



import asyncio

import requests
import pytz
import datetime
trader = WebullTrader()
        

async def main():

    price, _, option_data = await opts.all_options('SPX')

    price_info = price.under_close
    data = await trader.place_trade(ticker_id="1041538549", price=price_info)




    print(data)

asyncio.run(main())