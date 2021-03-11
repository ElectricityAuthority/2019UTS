import pandas as pd
import numpy as np
from datetime import datetime, date, time, timedelta
import os
import re


def vSPD_loader(path, case, files=None, csv=True):
    """load vSPD output data, save to parquet for speed"""
    output = {}
    if csv:
        for k, v in files.items():
            filename = path + case + '/' + case + '_' + v
            print('Loading: ' + filename)
            output[k] = pd.read_csv(filename, index_col=[0, 1], parse_dates=True)
            output[k].to_parquet(path + 'parquet/' + case + '_' + v[:-4] + '.parquet')
        return output
    else:  # use FAST parquet datafiles
        for k, v in files.items():
            filename = path + case + '_' + v[:-4] + '.parquet'
            print('Loading: ' + filename)
            df = pd.read_parquet(filename)
            df = df.groupby(level=[0, 1]).last()
            output[k] = df
        return output


def get_prices(case_dict, case):
    """given case return price dataframe"""
    df = case_dict[case]['nod_res']
    df['Node'] = df.index.map(lambda x: x[1].split()[0])
    df = df.reset_index(level=1, drop=True).set_index('Node', append=True)
    df = df.groupby(level=[0, 1]).last()['Price ($/MWh)']
    df = df.unstack()
    df.index = df.index.map(lambda x: x+timedelta(seconds=15*60))
    df = df.stack()
    df = df.reset_index()
    df = df.rename(columns={'DateTime': 'datetime', 'Node': 'POC'}).set_index(['datetime', 'POC'])[0]
    return df


def get_reserve_prices(case_dict, case):
    """given case retrun calculated reserve prices"""
    df = case_dict[case]['res_res'].loc[:, ['FIR Price ($/MW)', 'SIR Price ($/MW)']]
    df = df.rename(columns=dict(zip(df.columns, ['FIR', 'SIR']))).unstack()
    df.columns = df.columns.set_names('Reserve_class', level=0)
    df.index = df.index.map(lambda x: x + timedelta(seconds=60*15))
    return df    


# FTR functions
def get_dec_2019_ftr_data(F):
    """get Dec 2019 data"""
    F = F.copy()
    F = F.xs(pd.Period("2019/12"), level=4)
    
    F['FTR']=F.index.map(lambda x: x[2] + '->' + x[3])
    F = F.reset_index().set_index(['HedgeType', 'FTR', 'CurrentOwner'])["MW"]
    F = F.groupby(level=[0,1,2]).sum().unstack()
    return F


def get_dec_2019_price(p, FTR_nodes):
    """get Dec 2019 final prices"""
    p = pd.read_parquet(p)
    p = p.unstack().unstack().unstack()["2019/12"].stack().stack().stack()
    p = p.reset_index(level=2, drop=True)
    p = timeseries_convert(p['$/MWh'].unstack().loc[:, FTR_nodes], keep_tp_index=False)
    return p


def synthesize_new_dec_prices(case_dict, case, dec_UTS_prices, FTR_nodes):
    """synthesize full December price series made of original prices for 1,2, 28,29,30,31 Dec and ATC prices for UTS period"""
    p_uts_atc = get_prices(case_dict, case)
    p_uts_atc = p_uts_atc.unstack().loc[:, FTR_nodes]
    p_dec_ftr = dec_UTS_prices["2019/12/1": "2019/12/2"].append(p_uts_atc).append(dec_UTS_prices["2019/12/28": "2019/12/31"])
    return p_dec_ftr


def calc_FTR_prices(F, p, opt=True):
    """given FTR price for Dec simulations calculate the new Dec FTR prices"""
    # first create a list of all FTRs from the FTR data
    FTR_list = list(set(F.index.levels[1]))  #map(lambda x: x[2] + '2201->' + x[3] + '2201')))
    # Calculate final FTR prices based on Final Prices
    FTR_PRICES = pd.DataFrame()

    for ftr in FTR_list:
        #print(ftr)
        source = ftr.split('->')[0] + '2201'
        sink = ftr.split('->')[1] + '2201'
        if opt:
            FTR_PRICES[ftr] = ((p[sink] - p[source]).clip_lower(0)).groupby(level=0).mean()
        else:
            FTR_PRICES[ftr] = ((p[sink] - p[source])).groupby(level=0).mean()
    return FTR_PRICES.mean()
    

def calc_FTR_dollars(F, p_bc, case_dict, case, FTR_nodes):
    """given FTR positions and prices return dollars per participant, per case."""
    FTR_opt = calc_FTR_prices(F, synthesize_new_dec_prices(case_dict, case, p_bc, FTR_nodes), opt=True)  # calc new FTR prices $/MWh
    FTR_obl = calc_FTR_prices(F, synthesize_new_dec_prices(case_dict, case, p_bc, FTR_nodes), opt=False)  # calc new FTR prices $/MWh
    FTR_opt_dollars = (F.xs('OPT', level=0).multiply(FTR_opt, axis=0)*744).sum() 
    FTR_obl_dollars = (F.xs('OBL', level=0).multiply(FTR_obl, axis=0)*744).sum()
    F_dollars = pd.DataFrame({'OPT': FTR_opt_dollars, 'OBL': FTR_obl_dollars}).stack().swaplevel(0,1).sort_index()
    return FTR_opt, FTR_obl, F_dollars
    
    
def get_FTR_data(filename):
    """get all FTR data"""
    F = pd.read_csv(filename, index_col=['DateAcquired', 'HedgeType', 'Source', 'Sink', 'StartDate', 'EndDate', 
                                         'CurrentOwner', 'FirstOwner', 'PreviousOwner'], parse_dates=True, dayfirst=True).sort_index()

    # sort out FTR data, including calculating orig MW and Sold Prices

    F['period'] = F.index.map(lambda x: pd.Period(x[4], freq='M'))
    F.set_index('period', append=True, inplace=True)
    F.reset_index(['StartDate', 'EndDate'], drop=True, inplace=True)

    F = F.reset_index().set_index(['DateAcquired', 'HedgeType', 'Source', 'Sink', 'period', 'CurrentOwner', 'FirstOwner',
                                   'PreviousOwner']).sort_index()

    def recalc_orig_MW(x):
        """attempt to recalc orgin MW"""
        try:
            return np.round(x.OriginalAcquisitionCost/x.Price/((x.name[4].end_time-x.name[4].start_time+timedelta(seconds=1)).days*24), decimals=1)
        except:
            return np.nan
        
    F['Orig_MW'] = F.apply(lambda x: recalc_orig_MW(x), axis=1)
    F['Sold_MW'] = F['Orig_MW'] - F['MW']

    # calc sold price

    def calc_sold_price(x):
        """calc sold price"""
        if x.Sold_MW>0:
            return np.round((x.OriginalAcquisitionCost-x.AcquisitionCost)/x.Sold_MW/((x.name[4].end_time-x.name[4].start_time+timedelta(seconds=1)).days*24), 
                                             decimals=2)

    F['Sold_Price'] = F.apply(lambda x: calc_sold_price(x), axis=1)
    F['hours'] = F.index.map(lambda x: (x[4].end_time-x[4].start_time+timedelta(seconds=1)).days*24) 
    
    return F


# misc functions
def daily_count(df):
    '''Count trading periods in each day'''
    c = df.fillna(0).groupby(df.fillna(0).index.map(lambda x: x[0])).count()
    return c[c.columns[0]]

def timeseries_convert(df, keep_tp_index=True):
    '''Convert multi-indexed ts dataframe from the Data Warehouse
    into a single datetime indexed timeseries.
    From Date, Trading_Period index --> datetime index
    This gives better ts plotting in matplotlib)

    Daylight savings is a nuisance, used the CDS Gnash method for this...
    ***NOTE***: only works on full days of data, make sure data set is not tuncated
    '''
    dc = daily_count(df, )
    tp46 = dc[dc == 46].index  # short days
    tp50 = dc[dc == 50].index  # long days
    tp48 = dc[dc == 48].index  # normal days
    ds = pd.DataFrame(
        columns=['dls'],
        index=df.index)  # Create temp dataframe for mapping
    # need to test that the index is a datetime type
    # this appears to have some dependence on the windows/linux systems...
    if str(type(df.index[0][0])) == "<type 'datetime.date'>":
        ds.loc[
            ds.index.map(
                lambda x: x[0] in tp46),
            'dls'] = 46  # set value to 46 on short days
        ds.loc[
            ds.index.map(
                lambda x: x[0] in tp48),
            'dls'] = 48  # to 48 on normal days, and,
        ds.loc[
            ds.index.map(
                lambda x: x[0] in tp50),
            'dls'] = 50  # to 50 on long days
    else:
        ds.loc[
            ds.index.map(
                lambda x: x[0].date() in tp46),
            'dls'] = 46  # set value to 46 on short days
        ds.loc[
            ds.index.map(
                lambda x: x[0].date() in tp48),
            'dls'] = 48  # to 48 on normal days, and,
        ds.loc[
            ds.index.map(
                lambda x: x[0].date() in tp50),
            'dls'] = 50  # to 50 on long days
    # create date and trading period columns
    ds['date'] = ds.index.map(lambda x: x[0])
    ds['tp'] = ds.index.map(lambda x: x[1])
    # short day mapping
    tp46map = dict(list(zip(list(range(1, 47)), list(range(1, 5)) + list(range(7, 49)))))
    tp48map = dict(list(zip(list(range(1, 49)), list(range(1, 49)))))  # normal day mapping
    # long day mapping
    tp50map = dict(
        list(zip(list(range(1, 51)), list(range(1, 4)) + [4, 4.5, 5, 5.5] + list(range(6, 49)))))
    ds['tp1'] = ds[
        ds['dls'] == 48].tp.map(
        lambda x: tp48map[x])  # create new trading period mapping
    ds['tp2'] = ds[ds['dls'] == 46].tp.map(lambda x: tp46map[x])
    ds['tp3'] = ds[ds['dls'] == 50].tp.map(lambda x: tp50map[x])
    ds['tp4'] = ds['tp1'].fillna(0) + ds['tp2'].fillna(0) + ds['tp3'].fillna(0)
    ds = ds.drop(['tp1', 'tp2', 'tp3'], axis=1)
    ds = ds.rename(columns={'tp4': 'tp1'})
    # convert from trading period mapping to time
    ds['time'] = ds.tp1.map(lambda x: time_converter2(x))
    ds['datetime'] = ds.apply(combine_date_time, axis=1)  # and create datetime
    # set the df index to the new datetime index
    df['datetime'] = ds['datetime']
    if keep_tp_index:
        df['tp'] = ds['tp']
    df = df.set_index('datetime')
    return df


def time_converter2(tp):
    '''Work out time from trading period'''
    if (tp > 0) & (tp < 49):
        return (datetime.combine(date.today(),
                                 time(int(np.floor(((int(tp) - 1) / 2.0))),
                                      int(((tp - 1) / 2.0 % 1) * 60 + 14.999),
                                      59)) + timedelta(seconds=1)).time()
    elif tp == 49:
        return time(23, 50, 59)
    else:
        return time(23, 55, 59)
    
def combine_date_time(df):
    '''Combine date and time columns, used with .apply'''
    return datetime.combine(df['date'], df['time']) 


def attempt_to_sort_out_parent_company_mappings(p):
    """given participant name, remove additional unwanted text and change ownership of some that we know"""
    p2 = p.strip().replace('Limited', '').replace('Ltd', '').replace('Energy', '').replace('NZ', '').replace('Genesis Power', 'Genesis')
    p3 = re.sub(r'\([^)]*\)', '', p2).replace('trading as Club Energy', '').replace('trading as megaTEL', '').strip()
    if p3=='Globug':
        p3='Mercury'
    if p3=='Powershop':
        p3='Meridian'
    if p3=='Powershop NZ':
        p3='Meridian'
    p3 = p3.replace('  ', ' ') 
    #print(p + ' >' + p3 + '<')))
    return p3

# some plotting functions
def legend_format(ax, cols=4, xpos=-0.021, ypos=-0.15, **kwargs):
    """Place legend outside of plot"""
    ax.legend(loc=3,
              bbox_to_anchor=(xpos, ypos),
              ncol=cols,
              frameon=False, **kwargs)
    

def plot_formatting(ax, rot=False, **kwargs):
    """A few tricks used for better looking plots"""
    ax.grid(b=True, which='major', color='k', linestyle='-',
            axis='y', alpha=0.6, clip_on=True, marker=None)
    ax.grid(b=False, axis='x', which='both')
    ax.set_frame_on(False)  # Remove plot frame
    ax.set_axisbelow(True)
    ax.xaxis.tick_bottom()
#     plt.xticks(ax.get_xticks(), rotation=0, **kwargs)
#     if rot:
#         plt.xticks(ax.get_xticks(), rotation=90, **kwargs)
#     else:
#         plt.xticks(ax.get_xticks(), rotation=0, **kwargs)