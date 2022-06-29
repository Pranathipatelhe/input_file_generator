import pandas as pd
from _csv import QUOTE_ALL
from datetime import datetime
import datetime
from itertools import groupby
from collections import Counter
from collections import defaultdict
import numpy as np


class PredictionData:
    def __int__(self):
        pass

    @classmethod
    def get_weather_data(self, customer_id, date):
        Weathercollection = dbname['weather_data']
        Weather_response=Weathercollection.find(
            {'date': {'$eq': date},
             'customer_id': customer_id},
            {'_id': 0, 'avgtemp_f': 1})

        return Weather_response

    def get_influncere_data(cls, customer_id, date):
        Influencercollection = dbname['influencer_data']
        pipeline = [{
                   '$match' : {'$and': [
                   { 'start_date': { '$gte': date } } ,
                   { 'end_date': { '$lte': date } },
                   { 'customer_id': { '$eq': customer_id } }
                   ]
                     }},
                   {
                     '$group': {
                     '_id': {'influencer_type':'$influencer_type'},
                     'count': { '$sum': 1 }
                   }
                }]
        influencer_result = Influencercollection.aggregate(
                            pipeline,
                            allowDiskUse = False)
        return influencer_result

    def get_customer_influencer(cls, customerid):
        Customerinflucollection = dbname['customer_influencers']
        result_customer = Customerinflucollection.find(
            {
                'customer_id': customerid
            },
            {
                '_id': 0,'influencer_name':1
            }
        )
        return result_customer


    def get_prediction_data(self,customer_id, date):
        Predictioncollection = dbname['prediction_data']
        prediction_result = Predictioncollection.find(
            {'date.date': {'$eq': date},
             'customer_id': customer_id, 'ticket_type': 'Ticket Type 1'},
            {'_id': 0, 'sold_qty': 1, 'scan_qty': 1, 'temperature_celsius': 1, 'temperature_farenheit': 1})
        return prediction_result


    def get_dataframe_withdates(self):
        start_date = '2021-04-10'
        end_date = '2021-07-27'
        customerid = '1002'
        date = pd.period_range(start=start_date, end=end_date, freq='D')
        date_li = (list(date.astype(str)))
        df2 = pd.DataFrame(date_li, columns=["date"])
        df2["customer_id"] = customerid
        df2['date'] = pd.to_datetime(df2.date, format='%Y-%m-%d')
        result5 = p.get_customer_influencer('0000')
        list_customer = list(result5)
        values_of_key = [a_dict["influencer_name"] for a_dict in list_customer]
        df3 = pd.DataFrame(columns=values_of_key)
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

    print(dbname)
    print(collections)
    p = PredictionData()
    d=p.get_dataframe_withdates()



    for i, j in d.iterrows():
        prediction_response = p.get_prediction_data(d.loc[i, "customer_id"], d.loc[i,"date"])
        list_pred = list(prediction_response)
        d.loc[i, "sold_qty"] = ""
        d.loc[i, "scan_qty"] = ""
        d.loc[i, "temperature_celsius"] = ""
        d.loc[i, "temperature_farenheit"] = ""

        if len(list_pred) > 0:
            d.loc[i, "sold_qty"] = list_pred[0]["sold_qty"]
            d.loc[i, "scan_qty"] = list_pred[0]["scan_qty"]
            d.loc[i, "temperature_celsius"] = list_pred[0]["temperature_celsius"]
            d.loc[i, "temperature_farenheit"] = list_pred[0]["temperature_farenheit"]


        Weather_response = p.get_weather_data(d.loc[i, "customer_id"], d.loc[i,"date"])
        list_weather = list(Weather_response)
        d.loc[i, "avgtemp_f"] = ""
        if len(list_weather) > 0:
            d.loc[i, "avgtemp_f"] = list_weather[0]["avgtemp_f"]



        Influ_response = p.get_influncere_data(d.loc[i, "customer_id"], d.loc[i,"date"])
        list_influ = list(Influ_response)

        column_headers = list(d.columns.values)

        d.loc[i, "Nearby Event"] = ""
        d.loc[i, "Holidays and Special Days"] = ""
        d.loc[i, "Campaigns and Promotions"] = ""
        d.loc[i, "Weather"] = ""

        if len(list_influ) > 0:
            for k in range(len(list_influ)):
                for n in range(len(column_headers)):
                    if list_influ[k]["_id"]["influencer_type"] == column_headers[n]:
                        d.loc[i, column_headers[n]] = list_influ[k]["count"]

    d['yhat_min'] = d['sold_qty'].rolling(window=3).mean().shift(0).round(0)
    d['yhat'] = d['sold_qty'].rolling(window=5).mean().shift(0).round(0)
    d['yhat_max'] = d['sold_qty'].rolling(window=7).mean().shift(0).round(0)


#print(d)


csv_export = d.to_csv(sep=",", quoting=QUOTE_ALL, quotechar='"', columns=['date', 'customer_id', 'sold_qty', 'scan_qty',
'temperature_celsius','temperature_farenheit','Nearby Event','Holidays and Special Days','Campaigns and Promotions'
    ,'Weather','yhat_min','yhat','yhat_max'])
print("\n CSV data:", csv_export)

d.to_csv(f"29062022round.csv",
                  columns=['date', 'customer_id', 'sold_qty', 'scan_qty','temperature_celsius','temperature_farenheit',
                           'Nearby Event','Holidays and Special Days','Campaigns and Promotions','Weather','yhat_min','yhat','yhat_max'],
                  index=False, quoting=QUOTE_ALL, quotechar='"')
















