import datetime
import os, sys
import numpy as np
import pandas as pd
from six import iteritems
from tqdm import tqdm
from trading_calendars import get_calendar
import logging
from dask import dataframe as dd

from zipline.assets.futures import CMES_CODE_TO_MONTH
#from zipline.data.bundles import core as bundles
from logbook import Logger, StreamHandler
from zipline.utils.cli import maybe_show_progress
from . import core as bundles

handler = StreamHandler(sys.stdout, format_string=" | {record.message}")
logger = Logger(__name__)
logger.handlers.append(handler)

def csvdir_futures(tframes, csvdir):
    return CSVDIRFutures(tframes, csvdir).ingest


#CME_CODE_TO_MONTH = dict(zip('FGHJKMNQUVXZ', range(1, 13)))


class CSVDIRFutures:
    """
    Wrapper class to call csvdir_bundle with provided
    list of time frames and a path to the csvdir directory
    """

    def __init__(self, tframes, csvdir):
        self.tframes = tframes
        self.csvdir = csvdir

    def ingest(self,
               environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir):
        futures_bundle(environ,
                       asset_db_writer,
                       minute_bar_writer,
                       daily_bar_writer,
                       adjustment_writer,
                       calendar,
                       start_session,
                       end_session,
                       cache,
                       show_progress,
                       output_dir,
                       self.tframes,
                       self.csvdir)


def third_friday(year, month):
    """Return datetime.date for monthly option expiration given year and
    month
    """
    # The 15th is the lowest third day in the month
    third = datetime.date(year, month, 15)
    # What day of the week is the 15th?
    w = third.weekday()
    # Friday is weekday 4
    if w != 4:
        # Replace just the day (of month)
        third = third.replace(day=(15 + (4 - w) % 7))
    return third


# def load_data(dir_csv):
#     """Given a parent_dir of cross-sectional daily files,
#        read in all the days and return a big dataframe.
#     """
#     df = dd.read_csv(os.path.join(dir_csv, '*.csv')).compute
#     return df
    # # list the files
    # filelist = os.listdir(dir_csv)
    # # read them into pandas
    # df_list = [
    #     pd.read_csv(os.path.join(parent_dir, file), parse_dates=[1])
    #     for file
    #     in tqdm(filelist)
    # ]
    # # concatenate them together
    # big_df = pd.concat(df_list)
    # big_df.columns = map(str.lower, big_df.columns)
    # big_df.symbol = big_df.symbol.astype('str')
    # mask = big_df.symbol.str.len() == 5  # e.g., ESU18; doesn't work prior to year 2000
    # return big_df.loc[mask]


def gen_asset_metadata(data, show_progress, exchange='CME'):
    if show_progress:
        logging.info('Generating asset metadata.')

    data = data.groupby(
        by='Symbol'
    ).agg(
        {'Date': [np.min, np.max]}
    )
    data.reset_index(inplace=True)
    #data['Date'] = pd.to_datetime(data['Date'], infer_datetime_format=True)
    data['symbol'] = data['Symbol']
    data['start_date'] = data.Date.amin
    data['end_date'] = data.Date.amax
    del data['date']
    data.columns = data.columns.get_level_values(0)

    data['exchange'] = exchange
    data['root_symbol'] = data['symbol'].str.slice(0, 2)

    data['exp_month_letter'] = data.symbol.str.slice(2, 3)
    data['exp_month'] = data['exp_month_letter'].map(CMES_CODE_TO_MONTH)
    data['exp_year'] = 2000 + data.symbol.str.slice(3, 5).astype('int')
    data['expiration_date'] = data.apply(lambda x: third_friday(x.exp_year, x.exp_month), axis=1)
    del data['exp_month_letter']
    del data['exp_month']
    del data['exp_year']

    data['auto_close_date'] = data['end_date'].values + pd.Timedelta(days=1)
    data['notice_date'] = data['auto_close_date']

    data['tick_size'] = 0.0001  # Placeholder for now
    data['multiplier'] = 1  # Placeholder for now

    return data


def parse_pricing_and_vol(data,
                          sessions,
                          symbol_map):
    for asset_id, symbol in iteritems(symbol_map):
        asset_data = data.xs(
            symbol,
            level=1
        ).reindex(
            sessions.tz_localize(None)
        )
        yield asset_id, asset_data


def parse_minute_data(data,
                      sessions,
                      symbol_map):
    for asset_id, symbol in iteritems(symbol_map):
        asset_data = data.xs(
            symbol,
            level=1
        ).reindex(
            sessions.tz_localize(None)
        )
        yield asset_id, asset_data


@bundles.register('csvdir_futures')
def futures_bundle(environ,
                   asset_db_writer,
                   minute_bar_writer,
                   daily_bar_writer,
                   adjustment_writer,
                   calendar,
                   start_session,
                   end_session,
                   cache,
                   show_progress,
                   output_dir,
                   tframes=None,
                   csvdir=None):
    import pdb;
    pdb.set_trace()
    #raw_data = load_data('/Users/jonathan/devwork/pricing_data/CME_2018')
    #if 'daily' in tframes:
    dir_daily = os.path.join(csvdir, 'daily')
    df_data = dd.read_csv(os.path.join(dir_daily, "*.csv")).compute()
    df_data['Date'] = pd.to_datetime(df_data['Date'], infer_datetime_format=True)
    asset_metadata = gen_asset_metadata(df_data, False)
    root_symbols = asset_metadata.root_symbol.unique()
    root_symbols = pd.DataFrame(root_symbols, columns=['root_symbol'])
    root_symbols['root_symbol_id'] = root_symbols.index.values

    asset_db_writer.write(futures=asset_metadata, root_symbols=root_symbols)

    symbol_map = asset_metadata.symbol
    sessions = calendar.sessions_in_range(start_session, end_session)
    df_data.set_index(['Date', 'symbol'], inplace=True)
    daily_bar_writer.write(
        parse_pricing_and_vol(
            df_data,
            sessions,
            symbol_map
        ),
        show_progress=show_progress
    )
    if 'minute' in tframes:
        dir_minute = os.path.join(csvdir, 'minute')
        df_data_minute = dd.read_csv(os.path.join(dir_minute, "*.csv")).compute()
        df_data_minute['Datetime'] = pd.to_datetime(df_data_minute['Datetime'], infer_datetime_format=True)
        minute_bar_writer.write(
            parse_minute_data(
                df_data_minute,
                sessions,
                symbol_map
            ),
            show_progress=show_progress
        )
