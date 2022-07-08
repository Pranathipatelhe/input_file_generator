import pandas as pd
from _csv import QUOTE_ALL
from datetime import datetime
import datetime
from itertools import groupby
from collections import Counter
from collections import defaultdict
import numpy as np
import json
import requests


class PredictionData:
    def __int__(self):
        pass

    @classmethod
    def get_weather_data(self, start_date, end_date, customerid):
        Weathercollection = dbname['weather_data']
        start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        Weather_response = Weathercollection.find({'date': {'$gte': start,
                                                            '$lte': end}, 'customer_id': customerid},
                                                  {'_id': 0, 'date': 1, 'customer_id': 1, 'avgtemp_f': 1})

        listweather = list(Weather_response)

        return listweather

    def get_influncere_data(cls, customer_id):
        Influencercollection = dbname['influencer_data']

        influ_response = Influencercollection.find({'customer_id': customer_id},
                                                   {'_id': 0, 'customer_id': 1, 'start_date': 1, 'end_date': 1,
                                                    'influencer_type': 1})

        list_influ = list(influ_response)

        return list_influ


    def get_prediction_data(self, start_date, end_date, customerid):
        Predictioncollection = dbname['prediction_data']
        start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.datetime.strptime(end_date, '%Y-%m-%d')

        pipeline_2 = [
            {
                "$match": {
                    "$and": [
                        {'date.date': {'$gte': start, '$lte': end}},
                        {"customer_id": customerid}
                    ],
                    "ticket_type": "Archy Adult"
                }
            },
            {'$group': {'_id': {'date': '$date.date', 'customer_id': '$customer_id','ticket_type': '$ticket_type'},
                        'sold_qty': {'$sum': '$sold_qty'}, 'scan_qty': {'$sum': '$scan_qty'}
                        }}]

        prediction_response = Predictioncollection.aggregate(
            pipeline_2,
            allowDiskUse=False)

        listprediction = list(prediction_response)

        return listprediction
    def customer_api(self,strng):
        a = json.loads(strng)
        L = []
        for k in range(len(a['data'])):
            dict = a['data'][k]
            #print(dict['influencer_name'])
            L.append(dict['influencer_name'])
        return  L

    def get_dataframe_withdates(self, list_api,start_date,end_date,customerid):
        date = pd.period_range(start=start_date, end=end_date, freq='D')
        date_li = (list(date.astype(str)))
        df2 = pd.DataFrame(date_li, columns=["date"])
        df2["customer_id"] = customerid
        df2['date'] = pd.to_datetime(df2.date, format='%Y-%m-%d')
        df3 = pd.DataFrame(columns=list_api)
        d = pd.concat((df2, df3), axis=1)
        return d





def get_database():
    CONNECTION_STRING = "mongodb://192.168.55.24:27017"
    from pymongo import MongoClient
    client = MongoClient(CONNECTION_STRING)

    return client['attendance_prediction']

if __name__ == "__main__":
    dbname = get_database()
    collections = dbname.list_collection_names()

    #print(dbname)
    #print(collections)
    p = PredictionData()
    start_date = '2020-09-01'
    end_date = '2020-09-30'
    customerid = '50000'
    api_response = requests.get("http://192.168.55.77/3.0/customer/"+customerid+"/influencers")
    object = api_response.json()
    text = json.dumps(object)
    List_api = p.customer_api(text)
    list_weather = p.get_weather_data(start_date, end_date, customerid)
    list_prediction = p.get_prediction_data(start_date, end_date, customerid)
    list_influencer = p.get_influncere_data(customerid)

    d = p.get_dataframe_withdates(List_api,start_date,end_date,customerid)

    for i, j in d.iterrows():
        d.loc[i, "avgtemp_f"] = ""
        for m in range(len(list_weather)):
            if d.loc[i, "date"] == list_weather[m]["date"]:
                d.loc[i, "avgtemp_f"] = list_weather[m]["avgtemp_f"]

        d.loc[i, "sold_qty"] = "0"
        d.loc[i, "scan_qty"] = "0"
        d.loc[i, "ticket_type"] = ""
        for k in range(len(list_prediction)):
            if d.loc[i, "date"] == list_prediction[k]["_id"]["date"]:
                d.loc[i, "sold_qty"] = list_prediction[k]["sold_qty"]
                d.loc[i, "scan_qty"] = list_prediction[k]["scan_qty"]
                d.loc[i, "ticket_type"] = list_prediction[k]["_id"]["ticket_type"]
        LL = []
        nest = {}
        for b in range(len(list_influencer)):
            if d.loc[i, "date"] >= list_influencer[b]["start_date"] and d.loc[i, "date"] <= list_influencer[b][
                "end_date"]:
                LL.append(list_influencer[b]["influencer_type"])
                dcc = {x: LL.count(x) for x in LL}
                nest = {h: dcc[h] for h in dcc.keys() & set(List_api)}
                nestli = list(nest.items())
                for k in range(len(nestli)):
                    for n in range(len(List_api)):
                        if nestli[k][0] == List_api[n]:
                            d.loc[i, List_api[n]] = nestli[k][1]



    d['yhat_min1'] = d['sold_qty'].rolling(window=3).mean().shift(0).round(0)
    d['yhat1'] = d['sold_qty'].rolling(window=5).mean().shift(0).round(0)
    d['yhat_max1'] = d['sold_qty'].rolling(window=7).mean().shift(0).round(0)

    d['yhat_min1'] = d['yhat_min1'].replace(np.nan, 0)
    d['yhat1'] = d['yhat1'].replace(np.nan, 0)
    d['yhat_max1'] = d['yhat_max1'].replace(np.nan, 0)

    min_vals = d[["yhat_min1", "yhat1", "yhat_max1"]].values.tolist()
    for s in range(len(min_vals)):
        lis = min_vals[s]
        lis.sort(reverse=False)
    #print(min_vals)
    df = pd.DataFrame(min_vals, columns=['yhat_min', 'yhat', 'yhat_max'])
    d = pd.concat((d, df), axis=1)



today = datetime.datetime.today().strftime('%d%m%Y_%H%M%S')
cus_id=customerid

filename = "file_"+cus_id+"_"+today


csv_export = d.to_csv(sep=",", quoting=QUOTE_ALL, quotechar='"', columns=['date', 'customer_id','ticket_type' ,'sold_qty', 'scan_qty',
    'avgtemp_f','Nearby Event','Holidays','Campaigns and Promotions','Special Days'
    ,'Weather','yhat_min','yhat','yhat_max'])
print("\n CSV data:", csv_export)

d.to_csv(f"{filename}.csv",
                  columns=['date', 'customer_id','ticket_type', 'sold_qty', 'scan_qty','avgtemp_f',
                           'Nearby Event','Holidays','Campaigns and Promotions','Special Days','Weather','yhat_min','yhat','yhat_max'],
                  index=False, quoting=QUOTE_ALL, quotechar='"')
















