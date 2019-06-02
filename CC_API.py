#this files gets all off_chain exchange movement data regarding price and volume of exchanges

import os
#os.chdir('C:\\

import extract_time_series_spyder_09022019 as extract_ts

import requests
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import time 
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA

#%%


# get coinsnapshot

def CC_snapshot_limited():

    #limited snapshop
    symbol = 'BTC'
    transform = 'USDT'
    
    #url = 'https://min-api.cryptocompare.com/data/top/exchanges/full?fsym=BTC&tsym=USD'
    url = 'https://min-api.cryptocompare.com/data/top/exchanges/full?fsym={}&tsym={}'.format(symbol, transform)
    #"top" makes it top 5 exchanges, "full gives it all aggregate data"
    url_rq = requests.get(url)
    
    snapshot_small = url_rq.json()
    #snapshot_df = pd.DataFrame(snapshot_small['Data']['AggregatedData']['VOLUMEDAY'])
    
    #break down snapshot data 
    a_1 = snapshot_small['Data']['AggregatedData']
    a_2 = snapshot_small['Data']['CoinInfo']
    a_3 = snapshot_small['Data']['Exchanges']
    
    df_exchanges = pd.DataFrame(a_3)
    
    print(snapshot_small['Data'])
    
    return(snapshot_small)


#% get full_snapshop

#gets coin_id
#coin_id is hardcoded to BTC, feel free to change later
def CC_snapshot_full():

    #pull data
    url = 'https://www.cryptocompare.com/api/data/coinlist/'
    url_rq = requests.get(url)
    coinlist = url_rq.json()
    
    #generate list
    clist = coinlist['Data']
    clist = pd.DataFrame(clist).transpose()
    print(clist['Id'])
    
    #gets full_coin info, general descriptions etc.
    coin_id = 1182 #BTC= 1182
    url = 'https://www.cryptocompare.com/api/data/coinsnapshotfullbyid/?id={}'.format(coin_id)
    
    url_rq = requests.get(url)
    snapshot_full = url_rq.json()
    
    return(snapshot_full)


# exchange data

#% this provides all pairs

#https://min-api.cryptocompare.com/ for more information follow this.

def CC_exchange_names():
    url = 'https://min-api.cryptocompare.com/data/all/exchanges'
    url_rq = requests.get(url)
    exchange_pairs = url_rq.json()
    exchange_pairs = pd.DataFrame(exchange_pairs) # gives you matrix with all trading pairs, you can also skip this line
    
    exchange_list = list(exchange_pairs)
    
    return(exchange_list)

#%
#exchange volume
#pull data

freq = 'day' #'hour' 'day'
coin = 'BTC'
conversion = 'USD' # USD
period = 1000 #datapoints from today how many steps back per call
period_total = 36000 #how many steps we go back in total 36000  is a bit more than three and a half years
#"aggregate" smoothes over days/hours

exchange = 'Coinbase'

#p>Get total volume from the hourly historical exchange data.We store the data in BTC and we multiply by the BTC-tsym valueIf you want to get all the available historical data, you can use limit=2000 and keep going back in time using the toTs param. You can then keep requesting batches using: &limit=2000&toTs={the earliest timestamp received}.
def CC_exchange_volume(freq,coin,conversion,period,exchange_list):
    volume_df = pd.DataFrame()
    
    for exchange in exchange_list:
        url = 'https://min-api.cryptocompare.com/data/exchange/histo{}?e={}&fsym={}&tsym={}&limit={}&aggregate=1'.format(freq,exchange,coin,conversion,period)
        #print(url)
        url_rq = requests.get(url)
        exchange_volume = url_rq.json()
        
        exchange_volume = pd.DataFrame(exchange_volume['Data'],dtype='object') # strangely, this only works when dtype='object', if left empty or to dtype='int64' not working.
        
        #convert time
        exchange_volume['unix'] = exchange_volume['time']
        exchange_volume['time'] = pd.to_datetime(exchange_volume['time'],unit='s')
        exchange_volume.index = exchange_volume['time']
        
        #save volume for each exchange in pandas dataframe
        volume_df.set_index = exchange_volume['time']
        volume_df[exchange] = exchange_volume['volume']

    print(volume_df.describe())
    
    volume_clean_df = volume_df
    
    
#    eyeball = volume_clean_df.describe()
#    eyeball = eyeball.transpose()
#    
#    #some crude droping of outliers, needs to be improved.
#    volume_clean_df = volume_df.drop(['InstantBitex','Ethermium','TDAX','Bitmex','BXinth','IDEX'], axis=1)
#    
#    #volume_clean_df.plot()
#    volume_df.head(300).plot()
#    volume_clean_df.head(300).plot()
    
    return(volume_clean_df)

#this is the same function as before but now allows to download files in batches, period_batch = max 2000
def CC_exchange_volume_long(freq,coin,conversion,period_batch,exchange_list,period_final,period_start):
    volume_df = pd.DataFrame()
    
    for exchange in exchange_list:
        
        last_obs_unix = period_start
        final_obs_unix = period_final #end period unix 

        #while last_obs_unix > final_obs_unix :
        url = 'https://min-api.cryptocompare.com/data/exchange/histo{}?e={}&fsym={}&tsym={}&limit={}&toTs={}&aggregate=1'.format(freq,exchange,coin,conversion,period,period_start)

        #print(url)
        url_rq = requests.get(url)
        exchange_volume = url_rq.json()
        
        exchange_volume = pd.DataFrame(exchange_volume['Data'],dtype='object') # strangely, this only works when dtype='object', if left empty or to dtype='int64' not working.
        
        #convert time
        exchange_volume['unix'] = exchange_volume['time']
        exchange_volume['time'] = pd.to_datetime(exchange_volume['time'],unit='s')
        exchange_volume.index = exchange_volume['time']
        
        #save volume for each exchange in pandas dataframe
        volume_df.set_index = exchange_volume['time']
        volume_df[exchange] = exchange_volume['volume']

        volume_df2[exchange] = volume_df[exchange]


        while last_obs_unix > final_obs_unix :
            print("ammending")
        #here start to append the data with new observations
            last_obs = exchange_volume.iloc[0]
            last_obs_unix = exchange_volume.iloc[0]['unix']

            url = 'https://min-api.cryptocompare.com/data/exchange/histo{}?e={}&fsym={}&tsym={}&limit={}&toTs={}&aggregate=1'.format(freq,exchange,coin,conversion,period,last_obs_unix)

             #print(url)
            url_rq = requests.get(url)
            exchange_volume = url_rq.json()
            
            exchange_volume = pd.DataFrame(exchange_volume['Data'],dtype='object') # strangely, this only works when dtype='object', if left empty or to dtype='int64' not working.
            
            #convert time
            exchange_volume['unix'] = exchange_volume['time']
            exchange_volume['time'] = pd.to_datetime(exchange_volume['time'],unit='s')
            exchange_volume.index = exchange_volume['time']
            
            #save volume for each exchange in pandas dataframe
            #volume_df.set_index = exchange_volume['time']
            exchange_volume = exchange_volume[:-1]
            volume_df[exchange] = volume_df[exchange].iloc[::-1].append(exchange_volume['volume'].iloc[::-1])



    
    
    print(volume_df.describe())
    
    volume_clean_df = volume_df
    
    
#    eyeball = volume_clean_df.describe()
#    eyeball = eyeball.transpose()
#    
#    #some crude droping of outliers, needs to be improved.
#    volume_clean_df = volume_df.drop(['InstantBitex','Ethermium','TDAX','Bitmex','BXinth','IDEX'], axis=1)
#    
#    #volume_clean_df.plot()
#    volume_df.head(300).plot()
#    volume_clean_df.head(300).plot()
    
    return(volume_clean_df)

#%
#exchange prices, also include volumeto volumefrom

#Get open, high, low, close, volumefrom and volumeto from the daily historical data.The values are based on 00:00 GMT time. It uses BTC conversion if data is not available because the coin is not trading in the specified currency. If you want to get all the available historical data, you can use limit=2000 and keep going back in time using the toTs param. You can then keep requesting batches using: &limit=2000&toTs={the earliest timestamp received}.
def CC_exchange_price(freq,exchange,coin,conversion,period,exchange_list):
#def CC_exchange_price(freq,coin,conversion,period,exchange_list):
#use the parameter set before, conversion, freq etc.
    volume_p_df = pd.DataFrame() 
    closing_df = pd.DataFrame()
    
    
    
    for exchange in exchange_list:
        try:
            url = 'https://min-api.cryptocompare.com/data/histo{}?e={}&fsym={}&tsym={}&limit={}&aggregate=1'.format(freq,exchange,coin,conversion,period)
            url_rq = requests.get(url)
            exchange_OHLCV = url_rq.json()
            #time
            exchange_OHLCV = pd.DataFrame(exchange_OHLCV['Data'])
            exchange_OHLCV['time'] = pd.to_datetime(exchange_OHLCV['time'],unit='s')
            exchange_OHLCV.index = exchange_OHLCV['time']
            
            #save closing prices for each exchange in pandas dataframe, can also use other metric
            volume_p_df.set_index = exchange_OHLCV['time']
            closing_df.set_index = exchange_OHLCV['time']
            
            volume_p_df[exchange] = exchange_OHLCV['volumeto']
            closing_df[exchange] = exchange_OHLCV['close']
        except:
            pass
    
    
    #needs some outlier cleaning etc.    
    volume_p_df.plot()
    closing_df.head(200).plot() 
    
    #look at derivations from CCCAGG
    #positive values have a positive risk premium and vice versa
    deviation_df = closing_df.subtract(closing_df["CCCAGG"], axis ='index')
    deviation_df.plot()
       
    #look at deviation from equally weigthted mean
    closing_df.mean(axis=1)
     
    deviation_mean_df = closing_df.subtract(closing_df.mean(axis=1), axis ='index')
    deviation_mean_df.plot()
    
    return(volume_p_df, closing_df)



#% Social Media Data

def social_media():
    url = 'https://min-api.cryptocompare.com/data/v2/news/?lang=EN' #latest news articles
    url = 'https://min-api.cryptocompare.com/data/v2/news/?lang=EN&lTs=1506478327' #latest news articles and latest unix 31.1.2018
    #url = 'https://min-api.cryptocompare.com/data/news/feeds' #list news feeds
    #url = 'https://min-api.cryptocompare.com/data/news/feedsandcategories'#list news feeds and categories
    
    url_rq = requests.get(url)
    sentiment = url_rq.json()
    sentiment = sentiment['Data']
# simple sentiment analysis
#
#import nltk
#from nltk.sentiment.vader import SentimentIntensityAnalyzer
#SIA = SentimentIntensityAnalyzer()

    headlines=set()
     
    for i in range(len(sentiment)):
        print(sentiment[i]['body'])
        headlines.add(sentiment[i]['body'])
    
    sia = SIA()
    results = []
    
    for line in headlines:
        pol_score = sia.polarity_scores(line)
        pol_score['headline'] = line
        results.append(pol_score)
    
    # Put into dataframe and drop everything but headline and score
    df = pd.DataFrame.from_records(results)
    df.drop(columns = ['neg', 'pos', 'neu'], inplace = True)
    df.head()
    
    
    # Show the distribution of labelling 
    fig, ax = plt.subplots(figsize=(9, 6))
    df.hist(bins=np.linspace(-1, 1, 20), ax=ax)
    ax.set_title('')
    ax.set_xlabel('Compound score')
    ax.set_ylabel('Count');

    return(df)

#%%

if __name__ == "__main__":
    
    #set parameters
    freq = 'hour' #'hour' 'day'
    coin = 'BTC'
    conversion = 'USD' # USD
    period = 1000 #datapoints from today 
    #aggregate" smoothes over days/hours
    
    #get exchanges on cryptocompare
    exchange = 'Coinbase'
    exchange_list = CC_exchange_names()
    
    #get exchanges on walletexplorer
    exchanges_we_raw = extract_ts.parse_exchange_names()
    x_list = exchanges_we_raw[1]
    entities = x_list
    
    #compare exchanges of both sources
    entities_we = []
    for x in entities:
        head,sep,tail = x.partition('.')
        head = head.lower()
        entities_we.append(head)
    
    exchange_list2 = []    
    for x in exchange_list:
        y = x.lower()
        exchange_list2.append(y)
    
    set(entities_we)
    set(exchange_list)
    exchanges_common = set(entities_we) & set(exchange_list) 
    # 22 in common
    exchanges_common_small = set(entities_we) & set(exchange_list2) 
    len(exchanges_common_small)
    exchanges_common_small = list(exchanges_common_small)
    # it is 31 in common if we write all small
    
    #len = 22
    #only pick exchanges for which on-chain data is available
    exchange_list
    exchanges_common_small
    exchanges_common_small.append("CCCAGG")

    #2019-03-27 19:00:00	1553713200

   ts = int(period_start)
# if you encounter a "year is out of range" error the timestamp
# may be in milliseconds, try `ts /= 1000` in that case
    print(datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    #print(datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')

    
    
    #x_vol = CC_exchange_volume(freq,coin,conversion,period,exchange_list)
    x_vol = CC_exchange_volume(freq,coin,conversion,period,exchanges_common_small)
    #p>Get total volume from the hourly historical exchange data.We store the data in BTC and we multiply by the BTC-tsym valueIf you want to get all the available historical data, you can use limit=2000 and keep going back in time using the toTs param. You can then keep requesting batches using: &limit=2000&toTs={the earliest timestamp received}.

    
    #(x_volume_p, x_closing) = CC_exchange_price(freq,exchange,coin,conversion,period,exchange_list)
    (x_volume_p, x_closing) = CC_exchange_price(freq,exchange,coin,conversion,period,exchanges_common_small)
    #Get open, high, low, close, volumefrom and volumeto from the daily historical data.The values are based on 00:00 GMT time. It uses BTC conversion if data is not available because the coin is not trading in the specified currency. If you want to get all the available historical data, you can use limit=2000 and keep going back in time using the toTs param. You can then keep requesting batches using: &limit=2000&toTs={the earliest timestamp received}.
    
    
    #Cryptcompare has an API limit for minute data, check back with them.
    #x_highfreq = CC_exchange_price('minute','CCCAGG','BTC','USD','50000',['CCCAGG','ACX'])
    #
    
    
    
    #sentiment
    #sentiment = social_media()


#%%
