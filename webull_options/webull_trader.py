from .webull_options import WebullOptions
import os
import json
from dotenv import load_dotenv
import aiohttp
import asyncio
load_dotenv()

opts = WebullOptions(access_token=os.environ.get('ACCESS_TOKEN'), did=os.environ.get('DID'), osv=os.environ.get('OSV'))
class WebullTrader:
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


    async def place_trade(self, ticker_id, price, order_type:str='MKT', time_in_force:str='GTC', quantity:int=1, action:str='BUY',):

        payload = {"orderType":order_type,"timeInForce":time_in_force,"quantity":quantity,"action":action,"tickerId":ticker_id,"lmtPrice":price,"serialId":"yeajf9"}


        payload = json.dumps(payload)

        async with aiohttp.ClientSession(headers=opts.headers) as session:
            url=f"https://act.webullfintech.com/webull-paper-center/api/paper/1/acc/11512088/orderop/place/{ticker_id}"
            async with session.post(url, data=payload) as resp:
                data = await resp.json()


                print(data)

