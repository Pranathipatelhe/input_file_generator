import pandas as pd
from _csv import QUOTE_ALL
from datetime import datetime
import datetime


class PredictionData:
    def __int__(self):
        pass

    @classmethod
    def get_weather_data(self, customer_id, date):
        mycollection2 = dbname['weather_data']
        result3=mycollection2.find(
            {'date': {'$eq': date},
             'customer_id': customer_id},
            {'_id': 0, 'response.day.avgtemp_f': 1})

        return result3

    def get_influncere_data(cls, customer_id, date):
        mycollection4 = dbname['influencer_data']
        result4 = mycollection4.find(
            {'date': {'$eq': date},
             'customer_id': customer_id},
            {'_id': 0, 'influencer_value': 1, 'influencer_type': 1})
        return result4

    def get_prediction_data(self,customer_id, date):
        mycollection = dbname['prediction_data']
        result2 = mycollection.find(
            {'date.date': {'$eq': date},
             'customer_id': customer_id, 'ticket_type': 'Ticket Type 1'},
            {'_id': 0, 'sold_qty': 1, 'scan_qty': 1, 'temperature_celsius': 1, 'temperature_farenheit': 1})
        return result2


    def get_dataframe_withdates(self):
        start_date = '2022-03-01'
        end_date = '2022-05-31'
        customerid = '1002'
        date = pd.period_range(start=start_date, end=end_date, freq='D')
        date_li = (list(date.astype(str)))
        df2 = pd.DataFrame(date_li, columns=["date"])
        df2["customer_id"] = customerid

        df2['date'] = pd.to_datetime(df2.date, format='%Y-%m-%d')
        df2['Year-Week'] = df2['date'].dt.strftime('%Y-%U')
        return df2



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
    df2=p.get_dataframe_withdates()


    for i, j in df2.iterrows():
        result2 = p.get_prediction_data(df2.loc[i, "customer_id"], df2.loc[i,"date"])
        list_cur = list(result2)

        if len(list_cur) > 0:
            df2.loc[i, "sold_qty"] = list_cur[0]["sold_qty"]
            df2.loc[i, "scan_qty"] = list_cur[0]["scan_qty"]
            df2.loc[i, "temperature_celsius"] = list_cur[0]["temperature_celsius"]
            df2.loc[i, "temperature_farenheit"] = list_cur[0]["temperature_farenheit"]


        result3 = p.get_weather_data(df2.loc[i, "customer_id"], df2.loc[i,"date"])
        list_cur2 = list(result3)
        if len(list_cur2) > 0:
            df2.loc[i, "avgtemp_f"] = list_cur2[0]["response"]["day"]["avgtemp_f"]

        result4 = p.get_influncere_data(df2.loc[i, "customer_id"], df2.loc[i,"date"])
        list_cur3 = list(result4)
        if len(list_cur3) > 0:

            df2.loc[i, "influencer_value"] = list_cur3[0]["influencer_value"]
            df2.loc[i, "influencer_type"] = list_cur3[0]["influencer_type"]


    df2['Holiday'] = df2['influencer_type'].apply(lambda x: '1' if x == 'Holidays and Special Days' else '0')
    df2['nearbyevent'] = df2['influencer_type'].apply(lambda x: '1' if x == 'Nearby Event' else '0')
    df2['Day of the week'] = df2['influencer_type'].apply(lambda x: '1' if x == 'Day of the week' else '0')
    df2['Promotion'] = df2['influencer_type'].apply(lambda x: '1' if x == 'Promotion' else '0')

    df3=df2.copy()
    df3 = df3.groupby(['customer_id', 'Year-Week']).agg({'temperature_celsius':'mean','temperature_farenheit':'mean','sold_qty':'sum','scan_qty':'sum'}
       ).reset_index()

csv_export = df3.to_csv(sep=",", quoting=QUOTE_ALL, quotechar='"', columns=[ 'customer_id','Year-Week', 'sold_qty', 'scan_qty','temperature_celsius','temperature_farenheit'])
print("\n CSV data:", csv_export)

df3.to_csv(f"exportweek.csv",
                  columns=['customer_id','Year-Week', 'sold_qty', 'scan_qty','temperature_celsius','temperature_farenheit'],
                  index=False, quoting=QUOTE_ALL, quotechar='"')

















