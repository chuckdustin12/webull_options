import pandas as pd
from datetime import datetime

class From_:
    def __init__(self, from_):
        self.date = [i.get('date') for i in from_]
        self.days = [i.get('days') for i in from_]
        self.weekly = [i.get('weekly') for i in from_]
        self.unSymbol = [i.get('unSymbol') for i in from_]



        self.data_dict = { 

            'date': self.date,
            'days': self.days,
            'weekly': self.weekly,
            'symbol': self.unSymbol
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)


class BaseData:
    def __init__(self, data):
        self.tickerId = data['tickerId'] if 'tickerId' in data else None
        self.name = data['name'] if 'name' in data else None
        self.disSymbol = data['disSymbol'] if 'disSymbol' in data else None
        self.close = float(data['close']) if 'close' in data else None
        self.preClose = float(data['preClose']) if 'preClose' in data else None
        self.volume = float(data['volume']) if 'volume' in data else None
        self.open = float(data['open']) if 'open' in data else None
        self.high = float(data['high']) if 'high' in data else None
        self.low = float(data['low']) if 'low' in data else None
        self.change = float(data['change']) if 'change' in data else None
        self.changeRatio = round(float(data['changeRatio'])*100,2) if 'changeRatio' in data else None
        self.vol1y = float(data['vol1y']) if 'vol1y' in data else None
        self.expireDateList = data['expireDateList'] if 'expireDateList' in data else None




    @staticmethod
    async def create_table(connection):
        try:
            await connection.execute(
                '''
                CREATE TABLE IF NOT EXISTS base_data (
                    ticker_id VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    dis_symbol VARCHAR,
                    close FLOAT,
                    pre_close FLOAT,
                    volume FLOAT,
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    change FLOAT,
                    change_ratio FLOAT,
                    vol1y FLOAT,
                    expire_date_list TEXT[]
                );
                '''
            )
        except Exception as e:
            print(f"Error creating table asynchronously: {e}")




class OptionData:
    def __init__(self, data):
        self.open = [float(i.get('open')) if i.get('open') is not None else None for i in data]
        self.high = [float(i.get('high')) if i.get('high') is not None else None for i in data]
        self.low = [float(i.get('low')) if i.get('low') is not None else None for i in data]
        self.strikePrice = [int(float(i.get('strikePrice'))) if i.get('strikePrice') is not None else None for i in data]
        self.preClose = [float(i.get('preClose')) if i.get('preClose') is not None else None for i in data]
        self.openInterest = [float(i.get('openInterest')) if i.get('openInterest') is not None else None for i in data]
        self.volume = [float(i.get('volume')) for i in data]
        self.latestPriceVol = [float(i.get('latestPriceVol')) for i in data]
        self.delta = [round(float(i.get('delta')),4) if i.get('delta') is not None else None for i in data]
        self.vega = [round(float(i.get('vega')),4) if i.get('vega') is not None else None for i in data]
        self.impVol = [round(float(i.get('impVol')),4) if i.get('impVol') is not None else None for i in data]
        self.gamma = [round(float(i.get('gamma')),4) if i.get('gamma') is not None else None for i in data]
        self.theta = [round(float(i.get('theta')),4) if i.get('theta') is not None else None for i in data]
        self.rho = [round(float(i.get('rho')),4) if i.get('rho') is not None else None for i in data]
        self.close = [float(i.get('close')) if i.get('close') is not None else None for i in data]
        self.change = [float(i.get('change')) if i.get('change') is not None else None for i in data]
        self.changeRatio = [round(float(i.get('changeRatio'))*100,2) if i.get('changeRatio') is not None else None for i in data]
        self.expireDate = [datetime.strptime(i.get('expireDate'), '%Y-%m-%d').date() if i.get('expireDate') is not None else None for i in data]
        self.tickerId = [i.get('tickerId') for i in data]
        self.belongTickerId = [i.get('belongTickerId') for i in data]
        self.openIntChange = [float(i.get('openIntChange')) if i.get('openIntChange') is not None else None for i in data]
        self.activeLevel = [float(i.get('activeLevel')) for i in data]
        self.cycle = [float(int(i.get('cycle'))) for i in data]
        self.direction = [i.get('direction') for i in data]
        self.symbol = [i.get('symbol') for i in data]
        self.unSymbol = [i.get('unSymbol') for i in data]


        


      
        self.data_dict = {
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'strike_price': self.strikePrice,
            'pre_close': self.preClose,
            'open_interest': self.openInterest,
            'volume': self.volume,
            'latest_price_vol': self.latestPriceVol,
            'delta': self.delta,
            'vega': self.vega,
            'imp_vol': self.impVol,
            'gamma': self.gamma,
            'theta': self.theta,
            'rho': self.rho,
            'close': self.close,
            'change': self.change,
            'change_ratio': self.changeRatio,
            'expire_date': self.expireDate,
            'ticker_id': self.tickerId,
            'belong_ticker_id': self.belongTickerId,
            'open_int_change': self.openIntChange,
            'active_level': self.activeLevel,
            'cycle': self.cycle,
            'call_put': self.direction,
            'option_symbol': self.symbol,
            'underlying_symbol': self.unSymbol,

        }


        self.as_dataframe = pd.DataFrame(self.data_dict)



    @staticmethod
    def create_table(connection):
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    '''
                    CREATE TABLE IF NOT EXISTS option_data (
                        ticker_id VARCHAR,
                        belong_ticker_id VARCHAR,
                        open FLOAT,
                        high FLOAT,
                        low FLOAT,
                        strike_price INT,
                        pre_close FLOAT,
                        open_interest FLOAT,
                        volume FLOAT,
                        latest_price_vol FLOAT,
                        delta FLOAT,
                        vega FLOAT,
                        imp_vol FLOAT,
                        gamma FLOAT,
                        theta FLOAT,
                        rho FLOAT,
                        close FLOAT,
                        change FLOAT,
                        change_ratio FLOAT,
                        expire_date DATE,
                        open_int_change FLOAT,
                        active_level FLOAT,
                        cycle FLOAT,
                        call_put VARCHAR,
                        option_symbol VARCHAR,
                        underlying_symbol VARCHAR,
                        PRIMARY KEY (option_symbol, expire_date)
                    );
                    '''
                )
            connection.commit()
        except Exception as e:
            print(f"Error creating table: {e}")

    @staticmethod
    async def batch_insert_data(connection, data_frame: pd.DataFrame):
        try:
            records = data_frame.to_records(index=False)
            columns = data_frame.columns.tolist()
            values = ','.join([f"${i+1}" for i in range(len(columns))])
            query = f"INSERT INTO option_data ({', '.join(columns)}) VALUES ({values}) ON CONFLICT (option_symbol, expire_date) DO NOTHING"

            async with connection.transaction():
                await connection.executemany(query, records)
        except Exception as e:
            print(f"Error in batch insert: {e}")

    @staticmethod
    async def query_data(connection, query: str):
        try:
            async with connection.transaction():
                rows = await connection.fetch(query)
                return rows
        except Exception as e:
            print(f"Error in querying data: {e}")







      

class GroupData:
    def __init__(self, call_put):
        self.option = [i.get('option') for i in call_put]
        self.side = [i.get('side') for i in call_put]
        self.gravity = [i.get('gravity') for i in call_put]


        self.data_dict = { 
            'option_id': self.option,
            'side': self.side,
            'gravity': self.gravity
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)