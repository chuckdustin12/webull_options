import asyncpg

from asyncpg.exceptions import UniqueViolationError
class DBManager:
    def __init__(self, host='localhost', user='postgres', password='fud', database='opts', port=5432):
        self.db_params = {
            'host': host,
            'user': user,
            'password': password,
            'database': database,
            'port': port
        }
        self.pool = None

    async def create_pool(self, min_size=1, max_size=10):
        if self.pool is None:
            try:
                self.pool = await asyncpg.create_pool(
                    min_size=min_size, 
                    max_size=max_size, 
                    **self.db_params
                )
            except Exception as e:
                print(f"Error creating connection pool: {e}")
                raise

    async def close_pool(self):
        if self.pool:
            await self.pool.close()

    async def get_connection(self):
        if self.pool is None:
            await self.create_pool()

        return await self.pool.acquire()

    async def release_connection(self, connection):
        await self.pool.release(connection)



    async def insert_volume_analysis(self, option_id, option_symbol, underlying_symbol, strike_price, expiry, call_put, buy, sell, price, ratio, date):
        insert_query = '''
            INSERT INTO volume_analysis (option_id, option_symbol, underlying_symbol, strike_price, 
                                         expiry, call_put, buy, sell, price, ratio, date)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (option_id, date) DO NOTHING;
        '''

        connection = await self.get_connection()  # Ensure connection is established

        try:
            # Insert a single record
            await connection.execute(insert_query, option_id, option_symbol, underlying_symbol, strike_price, expiry, call_put, buy, sell, price, ratio, date)
        except Exception as e:
            print(f"Error during insert: {e}")
        finally:
            await connection.close()
    async def batch_insert_option_data(self, ticker_ids, belong_ticker_ids, opens, highs, lows, 
                                    strike_prices, pre_closes, open_interests, volumes, latest_price_vols, 
                                    deltas, vegas, imp_vols, gammas, thetas, rhos, closes, changes, 
                                    change_ratios, expire_dates, open_int_changes, active_levels, cycles, 
                                    call_puts, option_symbols, underlying_symbols):
        if not ticker_ids:
            return

        connection = await self.get_connection()
        try:


            # Create table and history table if they don't exist
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS options (
                    ticker_id INTEGER, belong_ticker_id INTEGER, open FLOAT, high FLOAT, low FLOAT, 
                    strike_price INTEGER, pre_close FLOAT, open_interest FLOAT, volume FLOAT, 
                    latest_price_vol FLOAT, delta FLOAT, vega FLOAT, imp_vol FLOAT, gamma FLOAT, 
                    theta FLOAT, rho FLOAT, close FLOAT, change FLOAT, change_ratio FLOAT, 
                    expire_date DATE, open_int_change FLOAT, active_level FLOAT, 
                    cycle FLOAT, call_put VARCHAR, option_symbol VARCHAR PRIMARY KEY, 
                    underlying_symbol VARCHAR
                );
                CREATE TABLE IF NOT EXISTS options_history (
                    id SERIAL PRIMARY KEY, ticker_id INTEGER, belong_ticker_id INTEGER, open FLOAT, 
                    high FLOAT, low FLOAT, strike_price INTEGER, pre_close FLOAT, open_interest FLOAT, 
                    volume FLOAT, latest_price_vol FLOAT, delta FLOAT, vega FLOAT, imp_vol FLOAT, 
                    gamma FLOAT, theta FLOAT, rho FLOAT, close FLOAT, change FLOAT, 
                    change_ratio FLOAT, expire_date DATE, open_int_change FLOAT, 
                    active_level FLOAT, cycle FLOAT, call_put VARCHAR, option_symbol VARCHAR, 
                    underlying_symbol VARCHAR, archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            await connection.execute(create_table_query)

            # Archive existing records to history table
            try:
                archive_query = f"""
                    INSERT INTO options_history (ticker_id, belong_ticker_id, open, high, low, strike_price, 
                                                    pre_close, open_interest, volume, latest_price_vol, delta, 
                                                    vega, imp_vol, gamma, theta, rho, close, change, 
                                                    change_ratio, expire_date, open_int_change, active_level, 
                                                    cycle, call_put, option_symbol, underlying_symbol)
                    SELECT ticker_id, belong_ticker_id, open, high, low, strike_price, 
                        pre_close, open_interest, volume, latest_price_vol, delta, 
                        vega, imp_vol, gamma, theta, rho, close, change, 
                        change_ratio, expire_date, open_int_change, active_level, 
                        cycle, call_put, option_symbol, underlying_symbol
                    FROM options
                    WHERE option_symbol = ANY($1);
                """
                await connection.execute(archive_query, ([symbol for symbol in option_symbols],))
            except UniqueViolationError:
                print(f'Exists - Skipping - No change')

            # Insert or update new records
            insert_query = f"""
                INSERT INTO options (
                    ticker_id, belong_ticker_id, open, high, low, strike_price, 
                    pre_close, open_interest, volume, latest_price_vol, delta, 
                    vega, imp_vol, gamma, theta, rho, close, change, 
                    change_ratio, expire_date, open_int_change, active_level, 
                    cycle, call_put, option_symbol, underlying_symbol
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 
                    $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26
                )
                ON CONFLICT (option_symbol) DO UPDATE SET
                    ticker_id = EXCLUDED.ticker_id,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    strike_price = EXCLUDED.strike_price,
                    pre_close = EXCLUDED.pre_close,
                    open_interest = EXCLUDED.open_interest,
                    volume = EXCLUDED.volume,
                    latest_price_vol = EXCLUDED.latest_price_vol,
                    delta = EXCLUDED.delta,
                    vega = EXCLUDED.vega,
                    imp_vol = EXCLUDED.imp_vol,
                    gamma = EXCLUDED.gamma,
                    theta = EXCLUDED.theta,
                    rho = EXCLUDED.rho,
                    close = EXCLUDED.close,
                    change = EXCLUDED.change,
                    change_ratio = EXCLUDED.change_ratio,
                    open_int_change = EXCLUDED.open_int_change,
                    active_level = EXCLUDED.active_level,
                    cycle = EXCLUDED.cycle,
                    call_put = EXCLUDED.call_put,
                    underlying_symbol = EXCLUDED.underlying_symbol;
            """

            values = list(zip(
                ticker_ids, belong_ticker_ids, opens, highs, lows, 
                strike_prices, pre_closes, open_interests, volumes, latest_price_vols, 
                deltas, vegas, imp_vols, gammas, thetas, rhos, closes, changes, 
                change_ratios, expire_dates, open_int_changes, active_levels, cycles, 
                call_puts, option_symbols, underlying_symbols
            ))

            await connection.executemany(insert_query, values)
        except Exception as e:
            print(f"Error in batch insert for option_data: {e}")
        finally:
            await self.release_connection(connection)

    async def query_derivative_ids_in_price_range(self, symbol, current_price, threshold_percentage=5, strike=None, expiry=None):
        table_name = f"{symbol}"
        connection = await self.get_connection()
        try:
            # Calculate price range
            upper_bound = current_price * (1 + threshold_percentage / 100)
            lower_bound = current_price * (1 - threshold_percentage / 100)

            # Base query
            query = f"""
                SELECT ticker_id, option_symbol FROM options
                WHERE strike_price BETWEEN $1 AND $2
            """
            
            # List to hold parameters
            params = [lower_bound, upper_bound]

            # Adding conditions for strike and expiry if they are provided
            if strike is not None:
                params.append(strike)
                query += f" AND strike_price = ${len(params)}"
            if expiry is not None:
                params.append(expiry)
                query += f" AND expire_date = ${len(params)}"

            # Fetch derivative IDs and option symbols
            results = await connection.fetch(query, *params)

            return [(row['ticker_id'], row['option_symbol']) for row in results]
        except Exception as e:
            print(f"Error querying derivative IDs in price range: {e}")
            return []
        finally:
            await self.release_connection(connection)

    async def insert_base_data(self, base_data):
        conn = await self.get_connection()

        # SQL to move existing data to history table
        move_to_history_query = '''
            INSERT INTO stock_data_history 
            SELECT *, CURRENT_TIMESTAMP FROM stock_data WHERE ticker_id = $1
        '''

        # SQL to update existing data
        update_query = '''
            UPDATE stock_data SET
                symbol = $2, underlying_volatility = $3, underlying_change = $4,
                underlying_change_percent = $5, underlying_volume = $6, underlying_close = $7,
                underlying_preclose = $8, underlying_high = $9, underlying_low = $10,
                underlying_open = $11, name = $12
            WHERE ticker_id = $1
        '''

        # SQL to insert new data
        insert_query = '''
            INSERT INTO stock_data (
                ticker_id, symbol, underlying_volatility, underlying_change, 
                underlying_change_percent, underlying_volume, underlying_close, 
                underlying_preclose, underlying_high, underlying_low, 
                underlying_open, name
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (ticker_id) DO NOTHING
        '''

        # Execute the queries
        await conn.execute(move_to_history_query, base_data.tickerId)
        await conn.execute(update_query,
                        base_data.tickerId, base_data.disSymbol, base_data.vol1y,
                        base_data.change, base_data.changeRatio, base_data.volume,
                        base_data.close, base_data.preClose, base_data.high,
                        base_data.low, base_data.open, base_data.name)
        await conn.execute(insert_query,
                        base_data.tickerId, base_data.disSymbol, base_data.vol1y,
                        base_data.change, base_data.changeRatio, base_data.volume,
                        base_data.close, base_data.preClose, base_data.high,
                        base_data.low, base_data.open, base_data.name)

        await conn.close()