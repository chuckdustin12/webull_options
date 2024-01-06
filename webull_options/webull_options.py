import os
from dotenv import load_dotenv
import numpy as np
load_dotenv()
import json
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime
import asyncpg
import time

from .models.options_data import From_, GroupData, BaseData, OptionData
from .models.db_manager import DBManager
from typing import List, Dict
from .helpers import process_candle_data, get_human_readable_string
from asyncio import Semaphore

sema = Semaphore(6)

class WebullOptions:
    def __init__(self, access_token, osv, did, database:str='fudstop', user:str='postgres'):
        self.database = database
        self.user=user
        self.access_token = access_token
        self.osv = osv
        self.did = did

        self.most_active_tickers = ['SPY', 'QQQ', 'SPX', 'TSLA', 'AMZN', 'IWM', 'NVDA', 'VIX', 'AAPL', 'F', 'META', 'MSFT', 'GOOGL', 'HYG', 'INTC', 'SQQQ', 'AMD', 'TQQQ', 'XLF', 'BAC', 'XLI', 'TLT', 'GOOG', 'GLD', 'SOFI', 'EEM', 'EFA', 'UVXY', 'NFLX', 'ENPH', 'SQ', 'COIN', 'CVX', 'PLTR', 'XBI', 'FXI', 'XOM', 'VXX', 'PYPL', 'GDX', 'AAL', 'MARA', 'JPM', 'XLE', 'EWZ', 'PFE', 'BABA', 'AMC', 'SLV', 'SOXL', 'DIS', 'UBER', 'DIA', 'GM', 'CVNA', 'RIVN', 'RIOT', 'VALE', 'KRE', 'C', 'VZ', 'USO', 'BA', 'ARKK', 'X', 'MPW', 'XSP', 'NIO', 'SNAP', 'RUT', 'KVUE', 'EDR', 'SHOP', 'SMH', 'BMY', 'JNJ', 'KWEB', 'CHPT', 'MRNA', 'BITO', 'GOLD', 'ZM', 'T', 'NEM', 'ET', 'KO', 'PBR', 'MS', 'SCHW', 'OXY', 'MU', 'DKNG', 'RIG', 'MO', 'WFC', 'NDX', 'VFS', 'XLU', 'BKLN', 'MCD', 'ABBV', 'JBLU', 'FSLR', 'AI', 'LCID', 'SNOW', 'ABNB', 'TNA', 'DVN', 'DAL', 'RTX', 'JD', 'UNG', 'RBLX', 'TGT', 'ADBE', 'UPS', 'WDC', 'LUV', 'TSM', 'UAL', 'PAA', 'ORCL', 'PLUG', 'GS', 'LQD', 'CCL', 'LABU', 'EPD', 'WE', 'AFRM', 'XPO', 'MSOS', 'IBM', 'XLV', 'NKE', 'MSTR', 'COST', 'QCOM', 'HD', 'CSCO', 'AVGO', 'SPXS', 'CLF', 'TFC', 'GME', 'ON', 'CVS', 'CMG', 'SPXU', 'AGNC', 'XLY', 'COF', 'FCX', 'PDD', 'WMT', 'MTCH', 'NEE', 'XOP', 'CRM', 'ROKU', 'MA', 'RUN', 'SBUX', 'PARA', 'SE', 'V', 'SAVE', 'UPST', 'DXCM', 'LLY', 'NCLH', 'ABT', 'AXP', 'ABR', 'CHWY', 'AA', 'DDOG', 'SVXY', 'LYFT', 'RCL', 'HOOD', 'BEKE', 'IBB', 'LI', 'PINS', 'PANW', 'ETSY', 'YINN', 'SAVA', 'OIH', 'WBA', 'TXN', 'FEZ', 'PG', 'CCJ', 'BOIL', 'SMCI', 'ALGN', 'XLP', 'CRWD', 'GE', 'MRVL', 'BX', 'WBD', 'SOXS', 'MRK', 'W', 'UVIX', 'SPXL', 'FSR', 'TZA', 'URNM', 'CAT', 'PEP', 'IMGN', 'XPEV', 'LULU', 'CVE', 'TTD', 'CMCSA', 'BIDU', 'NLY', 'AX', 'XRT', 'AG', 'BYND', 'BRK B', 'HL', 'M', 'NWL', 'SEDG', 'SIRI', 'EBAY', 'FLEX', 'BTU', 'NKLA', 'DISH', 'MDT', 'PSEC', 'VMW', 'ZS', 'COP', 'DG', 'AMAT', 'UCO', 'MDB', 'SLB', 'PTON', 'OKTA', 'U', 'HSBC', 'XHB', 'TMUS', 'UNH', 'OSTK', 'CGC', 'NOW', 'TLRY', 'DOCU', 'TDOC', 'MMM', 'HPQ', 'PCG', 'CHTR', 'Z', 'LOW', 'PENN', 'LMT', 'WOLF', 'KMI', 'VLO', 'SPWR', 'XLK', 'DLTR', 'WHR', 'NVAX', 'ARM', 'JETS', 'VNQ', 'DE', 'DLR', 'NET', 'FAS', 'WPM', 'DASH', 'ACN', 'ASHR', 'FUBO', 'CLX', 'ADM', 'SRPT', 'MRO', 'KGC', 'DPST', 'TWLO', 'AR', 'CNC', 'FDX', 'AMGN', 'VRT', 'CLSK', 'EMB', 'KOLD', 'CD', 'HES', 'SPOT', 'XLC', 'ZIM', 'GILD', 'EQT', 'CRSP', 'GDXJ', 'STNG', 'NAT', 'HAL', 'SGEN', 'GPS', 'USB', 'QS', 'UPRO', 'KSS', 'IDXX', 'FTNT', 'BALL', 'TMF', 'PACW', 'EL', 'MULN', 'NVO', 'GDDY', 'BBY', 'SPCE', 'SNY', 'KEY', 'MGM', 'FREY', 'CZR', 'LVS', 'TTWO', 'LRCX', 'MXEF', 'PAGP', 'ANET', 'VFC', 'GRPN', 'EW', 'BKNG', 'EOSE', 'TMO', 'SPY', 'SPX', 'QQQ', 'VIX', 'IWM', 'TSLA', 'HYG', 'AMZN', 'AAPL', 'BAC', 'XLF', 'TLT', 'SLV', 'EEM', 'F', 'NVDA', 'GOOGL', 'AMD', 'AAL', 'META', 'INTC', 'PLTR', 'C', 'GLD', 'MSFT', 'GDX', 'FXI', 'VALE', 'GOOG', 'XLE', 'SOFI', 'BABA', 'NIO', 'PFE', 'EWZ', 'PYPL', 'T', 'CCL', 'SNAP', 'DIS', 'GM', 'NKLA', 'WFC', 'TQQQ', 'AMC', 'UBER', 'RIVN', 'KRE', 'PBR', 'XOM', 'LCID', 'MARA', 'JPM', 'GOLD', 'ET', 'PLUG', 'JD', 'VZ', 'WBD', 'EFA', 'KVUE', 'RIG', 'SQ', 'CHPT', 'KWEB', 'KO', 'MU', 'BITO', 'TSM', 'SQQQ', 'SHOP', 'DKNG', 'CSCO', 'XLU', 'COIN', 'MPW', 'OXY', 'SOXL', 'FCX', 'RIOT', 'DAL', 'SCHW', 'TLRY', 'BA', 'NFLX', 'UAL', 'SIRI', 'MS', 'AGNC', 'UVXY', 'XBI', 'PARA', 'ARKK', 'CMCSA', 'DVN', 'UNG', 'VXX', 'CVX', 'CLF', 'RBLX', 'PINS', 'XLI', 'SE', 'CVNA', 'QCOM', 'SGEN', 'USO', 'TMF', 'BMY', 'RTX', 'XSP', 'ORCL', 'WBA', 'NKE', 'PDD', 'X', 'KMI', 'GME', 'NCLH', 'NEM', 'SMH', 'MSOS', 'TEVA', 'M', 'XPEV', 'ABBV', 'JETS', 'ABNB', 'MULN', 'JNJ', 'MO', 'CVS', 'AFRM', 'LUV', 'NEE', 'FSR', 'AI', 'SAVE', 'JBLU', 'HOOD', 'ENPH', 'DIA', 'WMT', 'LYFT', 'NU', 'BP', 'XOP', 'ENVX', 'SPCE', 'NOK', 'GRAB', 'BYND', 'ZM', 'SLB', 'NVAX', 'U', 'MRVL', 'CCJ', 'OPEN', 'CRM', 'CGC', 'AA', 'V', 'IBM', 'PTON', 'SBUX', 'LABU', 'TGT', 'STNE', 'BRK B', 'ASHR', 'UPST', 'QS', 'MRK', 'MRNA', 'VFS', 'XHB', 'TMUS', 'SNOW', 'PANW', 'VFC', 'UPS', 'BX', 'DISH', 'USB', 'TFC', 'GE', 'COP', 'LI', 'MET', 'XRT', 'ROKU', 'XLP', 'CHWY', 'FSLR', 'PG', 'XLK', 'FUBO', 'XLV', 'W', 'AMAT', 'GOEV', 'TXN', 'PEP', 'RUN', 'SWN', 'DOW', 'HD', 'GS', 'KGC', 'Z', 'AG', 'ABR', 'CAT', 'UUP', 'AXP', 'ZIM', 'KHC', 'RCL', 'LAZR', 'BOIL', 'DDOG', 'PENN', 'TTD', 'TELL', 'XLY', 'EPD', 'CRWD', 'VMW', 'NYCB', 'HUT', 'BTU', 'DOCU', 'NET', 'BKLN', 'SU', 'BAX', 'ETSY', 'HE', 'BTG', 'NLY', 'BHC', 'TDOC', 'LUMN', 'CLSK', 'MCD', 'LVS', 'MMM', 'DM', 'ALLY', 'SPWR', 'VRT', 'ABT', 'DASH', 'ADBE', 'TNA', 'MA', 'ACB', 'MDT', 'MGM', 'COST', 'WDC', 'GSAT', 'GPS', 'ON', 'MRO', 'PAAS', 'EOSE', 'LQD', 'BILI', 'AR', 'ONON', 'HTZ', 'TWLO', 'GILD', 'MMAT', 'ASTS', 'STLA', 'LLY', 'SABR', 'BIDU', 'EDR', 'AVGO', 'HAL', 'DG', 'WYNN', 'AEM', 'PATH', 'DB', 'IYR', 'UNH', 'HL', 'IEF', 'SPXS', 'CPNG', 'URA', 'NVO', 'BITF', 'URNM', 'KSS', 'FTCH', 'KEY', 'TH', 'GEO', 'FDX', 'CL', 'AZN', 'HPQ', 'DNN', 'BSX', 'SHEL', 'DXCM', 'PCG', 'BEKE', 'DNA', 'PM', 'TTWO', 'IQ', 'WE', 'ALB', 'SAVA', 'GDXJ', 'SPXU', 'OSTK', 'COF', 'SNDL', 'OKTA', 'BXMT', 'UEC', 'VLO', 'KR', 'ZION', 'WW', 'RSP', 'XP', 'IAU', 'LULU', 'ARCC', 'SOXS', 'VOD', 'TJX', 'MOS', 'EQT', 'IONQ', 'STNG', 'NOVA', 'HLF', 'HSBC', 'ARM']
        self.access_token = self.access_token
        self.osv = self.osv
        self.headers =  { 
            'Access_token': self.access_token,
            "Content-Type": "application/json;charset=UTF-8",
            'Device-Type': 'Web', #
            'Did': self.did,
            'Osv': self.osv, #
            "Hl": "en",
            "Os": "web"}
        self.sync_db_params = {'database': self.database, 'user': self.user, 'password': 'fud', 'host': 'localhost'}
        self.db_manager = DBManager(host='localhost', user=self.user, password='fud', database=self.database, port=5432)

    def sanitize_value(self, value, col_type):
        """Sanitize and format the value for SQL query."""
        if col_type == 'str':
            # For strings, add single quotes
            return f"'{value}'"
        elif col_type == 'date':
            # For dates, format as 'YYYY-MM-DD'
            if isinstance(value, str):
                try:
                    datetime.strptime(value, '%Y-%m-%d')
                    return f"'{value}'"
                except ValueError:
                    raise ValueError(f"Invalid date format: {value}")
            elif isinstance(value, datetime):
                return f"'{value.strftime('%Y-%m-%d')}'"
        else:
            # For other types, use as is
            return str(value)

    async def get_ticker_id(self, ticker):
        async with aiohttp.ClientSession() as session:
            url=f"https://quotes-gw.webullfintech.com/api/search/pc/tickers?keyword={ticker}&pageIndex=1&pageSize=1"
            async with session.get(url) as resp:
                r = await resp.json()
                data = r['data'] if 'data' in r else None
                if data is not None:
                    tickerId = data[0]['tickerId']

                    return tickerId

    async def all_options(self, ticker, direction='all'):
        
        ticker_id = await self.get_ticker_id(ticker)



        params = {
            "tickerId": f"{ticker_id}",
            "count": -1,
            "direction": direction,
            "type": 0,
            "quoteMultiplier": 100,
            "unSymbol": f"{ticker}"
        }
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with sema:
                url=f"https://quotes-gw.webullfintech.com/api/quote/option/strategy/list"
                async with session.post(url, data=json.dumps(params)) as resp:
                    data = await resp.json()

                    from_ = 0
                    base_data = OptionData(data)


                    underlying_price = base_data.close
                    vol1y = base_data.vol1y

                    option_data = OptionData(data)
                    
                    
        

                    return base_data, from_, option_data
                
    async def zeroDTE_options(self, ticker, direction='all'):
        
        ticker_id = await self.get_ticker_id(ticker)
 


        params = {
            "tickerId": f"{ticker_id}",
            "count": -1,
            "direction": direction,
            "type": 0,
            "quoteMultiplier": 100,
            "unSymbol": f"{ticker}"
        }
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with sema:
                url=f"https://quotes-gw.webullfintech.com/api/quote/option/strategy/list"
                async with session.post(url, data=json.dumps(params)) as resp:
                    data = await resp.json()

                    from_ = 0
                    base_data = OptionData(data)


                    underlying_price = base_data.close
                    vol1y = base_data.vol1y

                    option_data = OptionData(data)
                    
                    
        

                    return base_data, from_, option_data



    async def option_chart_data(self, derivative_id, timeframe:str='1m'):
        now_timestamp = int(time.mktime(datetime.utcnow().timetuple()))
        url = f"https://quotes-gw.webullfintech.com/api/quote/option/chart/kdata?derivativeId={derivative_id}&type={timeframe}&count=800&timestamp={now_timestamp}"

        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(url) as resp:
                data = await resp.json()

                data = [i.get('data') for i in data]


                # Assuming data is a list of strings from your original code
                processed_data = process_candle_data(data)

                print(processed_data)

    async def associate_dates_with_data(self, dates, datas):
        if datas is not None and dates is not None:
        # This function remains for your specific data handling if needed
            return [{**data, 'date': date} for date, data in zip(dates, datas)]
        



    async def filter_options(self):
        pass



    async def fetch_volume_analysis(self, option_pairs):
        all_dfs = []  # List to store individual DataFrames

        async with aiohttp.ClientSession(headers=self.headers) as session:
            for id, option_symbol in option_pairs:
                url = f"https://quotes-gw.webullfintech.com/api/statistic/option/queryVolumeAnalysis?count=200&tickerId={id}"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        vol_anal = await resp.json()
                        dates = vol_anal.get('dates')
                        datas = vol_anal.get('datas')
                        associated_data = await self.associate_dates_with_data(dates, datas)

                        df = pd.DataFrame(associated_data)
                        df['option_symbol'] = option_symbol
                        components = get_human_readable_string(option_symbol)
                        df['underlying_ticker'] = components.get('underlying_symbol')
                        df['strike'] = float(components.get('strike_price'))
                        df['call_put'] = components.get('call_put')
                        df['expiry'] = components.get('expiry_date')
                        all_dfs.append(df)
                    else:
                        print(f"Failed to fetch data for ID {id}: HTTP Status {resp.status}")

        # Concatenate all DataFrames
        concatenated_df = pd.concat(all_dfs, ignore_index=True)
        return concatenated_df.sort_values('volume', ascending=False)
    def dataframe_to_tuples(self, df):
        """
        Converts a Pandas DataFrame to a list of tuples, each tuple representing a row.
        """
        return [tuple(x) for x in df.to_numpy()]
    async def get_volume_analysis(self, ticker):
        
        data, _, option_data = await self.all_options(ticker)
        derivative_data = await self.db_manager.query_derivative_ids_in_price_range(ticker, current_price=data.close)
        async with aiohttp.ClientSession(headers=self.headers) as session:
            
            for id, option_symbol in derivative_data:
                url = f"https://quotes-gw.webullfintech.com/api/statistic/option/queryVolumeAnalysis?count=200&tickerId={id}"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        vol_anal = await resp.json()
                        dates = vol_anal.get('dates')
                        datas = vol_anal.get('datas')

                        if dates is None:
                            dates = []
                        if datas is None:
                            datas = []

                        # Use a different variable name in the inner loop
                        for date, data in zip(dates, datas):
                            components = get_human_readable_string(option_symbol)
                            underlying_symbol = components.get('underlying_symbol')
                            strike_price = float(components.get('strike_price'))
                            expiry = components.get(datetime.strptime(components.get('expiry_date'), '%Y-%m-%d').date())
                            call_put = components.get('call_put')

                            buy = float(data['buy'])
                            sell = float(data['sell'])
                            price = float(data['price'])
                            ratio = round(float(data['ratio'])*100,2)
                            date = pd.to_datetime(date)

                            await self.db_manager.insert_volume_analysis(option_id=id, option_symbol=option_symbol, underlying_symbol=underlying_symbol, strike_price=strike_price, expiry=expiry, call_put=call_put, buy=buy, sell=sell, price=price, ratio=ratio, date=date)

    async def filter_options(self, order_by=None, **kwargs):
        """
        Filters the options table based on provided keyword arguments.
        Usage example:
            await filter_options(strike_price_min=100, strike_price_max=200, call_put='call',
                                 expire_date='2023-01-01', delta_min=0.1, delta_max=0.5)
        """
        # Start with the base query
        query = f"SELECT * FROM public.wb_opts WHERE "
        params = []
        param_index = 1

        # Mapping kwargs to database columns and expected types, including range filters
        column_types = {
            'ticker_id': ('ticker_id', 'int'),
            'belong_ticker_id': ('belong_ticker_id', 'int'),
            'open_min': ('open', 'float'),
            'open_max': ('open', 'float'),
            'open': ('open', 'float'),
            'high_min': ('high', 'float'),
            'high_max': ('high', 'float'),
            'high': ('high', 'float'),
            'low_min': ('low', 'float'),
            'low_max': ('low', 'float'),
            'low': ('low', 'float'),
            'strike_price_min': ('strike_price', 'int'),
            'strike_price_max': ('strike_price', 'int'),
            'strike_price': ('strike_price', 'int'),
            'pre_close_min': ('pre_close', 'float'),
            'pre_close_max': ('pre_close', 'float'),
            'open_interest_min': ('open_interest', 'float'),
            'open_interest_max': ('open_interest', 'float'),
            'volume_min': ('volume', 'float'),
            'volume_max': ('volume', 'float'),
            'latest_price_vol_min': ('latest_price_vol', 'float'),
            'latest_price_vol_max': ('latest_price_vol', 'float'),
            'delta_min': ('delta', 'float'),
            'delta_max': ('delta', 'float'),
            'delta': ('delta', 'float'),
            'vega_min': ('vega', 'float'),
            'vega_max': ('vega', 'float'),
            'imp_vol': ('imp_vol', 'float'),
            'imp_vol_min': ('imp_vol', 'float'),
            'imp_vol_max': ('imp_vol', 'float'),
            'gamma_min': ('gamma', 'float'),
            'gamma_max': ('gamma', 'float'),
            'gamma': ('gamma', 'float'),
            'theta': ('theta', 'float'),
            'theta_min': ('theta', 'float'),
            'theta_max': ('theta', 'float'),
            'rho_min': ('rho', 'float'),
            'rho_max': ('rho', 'float'),
            'close_min': ('close', 'float'),
            'close': ('close', 'float'),
            'close_max': ('close', 'float'),
            'change_min': ('change', 'float'),
            'change_max': ('change', 'float'),
            'change_ratio_min': ('change_ratio', 'float'),
            'change_ratio_max': ('change_ratio', 'float'),
            'change_ratio': ('change_ratio', 'float'),
            'expire_date_min': ('expire_date', 'date'),
            'expire_date_max': ('expire_date', 'date'),
            'expire_date': ('expire_date', 'date'),
            'open_int_change_min': ('open_int_change', 'float'),
            'open_int_change_max': ('open_int_change', 'float'),
            'active_level_min': ('active_level', 'float'),
            'active_level_max': ('active_level', 'float'),
            'cycle_min': ('cycle', 'float'),
            'cycle_max': ('cycle', 'float'),
            'call_put': ('call_put', 'str'),
            'option_symbol': ('option_symbol', 'str'),
            'underlying_symbol': ('underlying_symbol', 'str'),
            'oi_weighted_delta_min': ('oi_weighted_delta', 'float'),
            'oi_weighted_delta_max': ('oi_weighted_delta', 'float'),
            'iv_spread_min': ('iv_spread', 'float'),
            'iv_spread_max': ('iv_spread', 'float'),
            'oi_change_vol_adjusted_min': ('oi_change_vol_adjusted', 'float'),
            'oi_change_vol_adjusted_max': ('oi_change_vol_adjusted', 'float'),
            'oi_pcr_min': ('oi_pcr', 'float'),
            'oi_pcr_max': ('oi_pcr', 'float'),
            'oc_pcr': ('oi_pcr', 'float'),
            'volume_pcr_min': ('volume_pcr', 'float'),
            'volume_pcr_max': ('volume_pcr', 'float'),
            'volume_pcr': ('volume_pcr', 'float'),
            'vega_weighted_maturity_min': ('vega_weighted_maturity', 'float'),
            'vega_weighted_maturity_max': ('vega_weighted_maturity', 'float'),
            'theta_decay_rate_min': ('theta_decay_rate', 'float'),
            'theta_decay_rate_max': ('theta_decay_rate', 'float'),
            'velocity_min': ('velocity', 'float'),
            'velocity_max': ('velocity', 'float'),
            'gamma_risk_min': ('gamma_risk', 'float'),
            'gamma_risk_max': ('gamma_risk', 'float'),
            'delta_to_theta_ratio_min': ('delta_to_theta_ratio', 'float'),
            'delta_to_theta_ratio_max': ('delta_to_theta_ratio', 'float'),
            'liquidity_theta_ratio_min': ('liquidity_theta_ratio', 'float'),
            'liquidity_theta_ratio_max': ('liquidity_theta_ratio', 'float'),
            'sensitivity_score_min': ('sensitivity_score', 'float'),
            'sensitivity_score_max': ('sensitivity_score', 'float'),
            'dte_min': ('dte', 'int'),
            'dte_max': ('dte', 'int'),
            'dte': ('dte', 'int'),
            'time_value_min': ('time_value', 'float'),
            'time_value_max': ('time_value', 'float'),
            'time_value': ('time_value', 'float'),
            'moneyness': ('moneyness', 'str')
        }

        # Dynamically build query based on kwargs
        query = "SELECT * FROM public.wb_opts WHERE open_interest > 0"
        if order_by and isinstance(order_by, list):
                order_clauses = []
                for column, direction in order_by:
                    if column in column_types:  # Ensure the column is valid
                        direction = direction.upper()
                        if direction in ['ASC', 'DESC']:
                            order_clauses.append(f"{column} {direction}")
                if order_clauses:
                    order_by_clause = ', '.join(order_clauses)
                    query += f" ORDER BY {order_by_clause}"
        # Dynamically build query based on kwargs
        for key, value in kwargs.items():
            if key in column_types and value is not None:
                column, col_type = column_types[key]

                # Sanitize and format value for SQL query
                sanitized_value = self.sanitize_value(value, col_type)

                if 'min' in key:
                    query += f" AND {column} >= {sanitized_value}"
                elif 'max' in key:
                    query += f" AND {column} <= {sanitized_value}"
                else:
                    query += f" AND {column} = {sanitized_value}"
                print(query)
        conn = await self.db_manager.get_connection()

        try:
            # Execute the query
            return await conn.fetch(query)
        except Exception as e:
            print(f"Error during query: {e}")
            return []

    async def find_plays(self):
        db_config = {
            'user': 'postgres',
            'password': 'fud',
            'database': 'fudstop',
            'host': '127.0.0.1',
            'port': 5432
        }

        async with asyncpg.create_pool(**db_config) as pool:
            extreme_tickers_with_status = await self.opts.find_extreme_tickers(pool)

            # To separate the tickers and statuses, you can use list comprehension
            extreme_tickers = [ticker for ticker, status in extreme_tickers_with_status]
            statuses = [status for ticker, status in extreme_tickers_with_status]
            all_options_df_calls =[]
            all_options_df_puts = []
            for ticker, status in extreme_tickers_with_status:
                if status == 'overbought':
                    print(f"Ticker {ticker} is overbought.")
                    all_options = await self.opts.get_option_chain_all(ticker, expiration_date_gte='2024-03-01', expiration_date_lte='2024-06-30', contract_type='put')
                    
                    for i in range(len(all_options.theta)):  # Assuming all lists are of the same length
                        theta_value = all_options.theta[i]
                        volume = all_options.volume[i]
                        open_interest = all_options.open_interest[i]
                        ask = all_options.ask[i]
                        bid = all_options.bid[i]

                        # Conditions
                        theta_condition = theta_value is not None and theta_value >= -0.03
                        volume_condition = volume is not None and open_interest is not None and volume > open_interest
                        price_condition = ask is not None and bid is not None and 0.25 <= bid <= 1.75 and 0.25 <= ask <= 1.75

                        if theta_condition and volume_condition and price_condition:
                            df = pd.DataFrame([all_options.ticker, all_options.underlying_ticker, all_options.strike, all_options.contract_type, all_options.expiry])
                            all_options_df_puts.append(df)  #

                if status == 'oversold':
                    print(f"Ticker {ticker} is oversold.")
                    all_options = await self.opts.get_option_chain_all(ticker, expiration_date_gte='2024-03-01', expiration_date_lte='2024-11-30', contract_type='call')
                    
                    for i in range(len(all_options.theta)):  # Assuming all lists are of the same length
                        theta_value = all_options.theta[i]
                        volume = all_options.volume[i]
                        open_interest = all_options.open_interest[i]
                        ask = all_options.ask[i]
                        bid = all_options.bid[i]

                        # Conditions
                        theta_condition = theta_value is not None and theta_value >= -0.03
                        volume_condition = volume is not None and open_interest is not None and volume > open_interest
                        price_condition = ask is not None and bid is not None and 0.25 <= bid <= 1.75 and 0.25 <= ask <= 1.75

                        if theta_condition and volume_condition and price_condition:
                            # Assuming all_options.df is a DataFrame containing the current option data
                            df = pd.DataFrame([all_options.ticker, all_options.strike, all_options.contract_type, all_options.expiry])
                            all_options_df_calls.append(df)  #
            # Concatenate all the dataframes
            final_df_calls = pd.concat(all_options_df_calls, ignore_index=True)
            final_df_puts = pd.concat(all_options_df_puts, ignore_index=True)
            print(final_df_calls, final_df_puts)
            return final_df_calls, final_df_puts, extreme_tickers, statuses
        


    async def update_and_insert_options(self, ticker):

        data, _, options = await self.all_options(ticker)


   


        df = options.as_dataframe


        # Assuming opts.db_manager.get_connection() returns a connection,
    
        await self.db_manager.batch_insert_wb_dataframe(df, table_name='wb_opts', history_table_name='wb_opts_history')

    async def update_all_options(self):
        await self.db_manager.get_connection()

        tasks = [self.update_and_insert_options(i) for i in self.most_active_tickers]

        await asyncio.gather(*tasks)






  