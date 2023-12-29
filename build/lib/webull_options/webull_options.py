import os
from dotenv import load_dotenv
load_dotenv()
import json
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime
import time
from .models.options_data import From_, OptionData, GroupData, BaseData
from .models.db_manager import DBManager
from typing import List, Dict
from .helpers import process_candle_data, get_human_readable_string
from asyncio import Semaphore

sema = Semaphore(6)

class WebullOptions:
    def __init__(self, access_token, osv, did):
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
        self.sync_db_params = {'database': 'opts', 'user': 'postgres', 'password': 'fud', 'host': 'localhost'}
        self.db_manager = DBManager(host='localhost', user='postgres', password='fud', database='opts', port=5432)

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
                    base_data = BaseData(data)
                    expireDateList = base_data.expireDateList

                    if expireDateList is not None and base_data.close is not None:
                        underlying_price = base_data.close
                        vol1y = base_data.vol1y
                        print(expireDateList)
                        data = [i.get('data') for i in expireDateList if i.get('data') is not None]


                        flat_data = [item for sublist in data for item in sublist]

                        option_data = OptionData(flat_data)

            

                        await self.db_manager.batch_insert_option_data(ticker_ids=option_data.tickerId, belong_ticker_ids=option_data.belongTickerId,opens=option_data.open,highs=option_data.high, lows=option_data.low, strike_prices=option_data.strikePrice,pre_closes=option_data.preClose,open_interests=option_data.openInterest, open_int_changes=option_data.openIntChange,volumes=option_data.volume,latest_price_vols=option_data.latestPriceVol,deltas=option_data.delta,vegas=option_data.vega,thetas=option_data.theta,gammas=option_data.gamma,rhos=option_data.rho,closes=option_data.close,changes=option_data.change,change_ratios=option_data.changeRatio,expire_dates=option_data.expireDate,active_levels=option_data.activeLevel,cycles=option_data.cycle,call_puts=option_data.direction,option_symbols=option_data.symbol,underlying_symbols=option_data.unSymbol, imp_vols=option_data.impVol)

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

    async def filter_options(self, **kwargs):
        """
        Filters the options table based on provided keyword arguments.
        Usage example:
            await filter_options(strike_price_min=100, strike_price_max=200, call_put='call',
                                 expire_date='2023-01-01', delta_min=0.1, delta_max=0.5)
        """
        # Start with the base query
        query = f"SELECT underlying_symbol, strike_price, call_put, expire_date FROM public.options WHERE "
        params = []
        param_index = 1

        # Mapping kwargs to database columns and expected types, including range filters
        column_types = {
            'ticker_id': ('ticker_id', 'int'),
            'belong_ticker_id': ('belong_ticker_id', 'int'),
            'open_min': ('open', 'float'),
            'open_max': ('open', 'float'),
            'high_min': ('high', 'float'),
            'high_max': ('high', 'float'),
            'low_min': ('low', 'float'),
            'low_max': ('low', 'float'),
            'strike_price_min': ('strike_price', 'int'),
            'strike_price_max': ('strike_price', 'int'),
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
            'vega_min': ('vega', 'float'),
            'vega_max': ('vega', 'float'),
            'imp_vol_min': ('imp_vol', 'float'),
            'imp_vol_max': ('imp_vol', 'float'),
            'gamma_min': ('gamma', 'float'),
            'gamma_max': ('gamma', 'float'),
            'theta_min': ('theta', 'float'),
            'theta_max': ('theta', 'float'),
            'rho_min': ('rho', 'float'),
            'rho_max': ('rho', 'float'),
            'close_min': ('close', 'float'),
            'close_max': ('close', 'float'),
            'change_min': ('change', 'float'),
            'change_max': ('change', 'float'),
            'change_ratio_min': ('change_ratio', 'float'),
            'change_ratio_max': ('change_ratio', 'float'),
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
            'underlying_price_min': ('underlying_price', 'float'),
            'underlying_price_max': ('underlying_price', 'float'),
        }

        # Dynamically build query based on kwargs
        query = "SELECT underlying_symbol, strike_price, call_put, expire_date FROM public.options WHERE open_interest > 0"

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
        finally:
            await conn.close()



    async def update_all_options(self):

        await self.db_manager.get_connection()
        tasks = [self.all_options(i) for i in self.most_active_tickers]

        await asyncio.gather(*tasks)

    # Run the main function
    async def update_option_data(self):
        await self.update_all_options()

