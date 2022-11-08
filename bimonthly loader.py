#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import datetime
from datetime import datetime
from datetime import timedelta
import warnings
import mysql.connector
warnings.filterwarnings('ignore')
import gspread
from oauth2client.service_account import ServiceAccountCredentials
gc = gspread.service_account(filename='key.json')
gsheet = gc.open_by_key("1w6ixWOuYCr1m63-NzsupkJyFM9loC89vY_wmlvvrdL4").sheet1


# In[2]:


def Get_AdjClosed_Price(ticker):
    gsheet.update('B1',ticker)
    df= pd.DataFrame(gsheet.get_all_records())
    new_header = df.iloc[0]
    df = df[-1*-1:]
    df.columns = new_header
    df['Date'] = [datetime.strptime(x,'%m/%d/%Y %H:%M:%S') for x in df['Date']]
    df['Date']=[df['Date'].iloc[x].date() for x in range(len(df['Date']))]
    return df


# In[3]:


import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Varshini15!",
  database="stockprice"
)
mycursor = mydb.cursor()


# In[4]:


mycursor.execute("SELECT MAX(date_close) FROM price")
tempdate = mycursor.fetchall()
last_date=tempdate[0][0]
end_date=last_date.strftime("%m/%d/%y")
gsheet.update('A1',end_date)


# In[5]:


sql = "INSERT INTO price (date_close, symbol, close_price) VALUES (%s, %s, %s)"


# In[6]:


Ticker =pd.read_csv(r"liststock.csv")
List=[]
List=Ticker['Symbol']
while(1):
    error=[]
    for ticker in List:
      ticker1='NSE:'+ticker
      try:
       df = Get_AdjClosed_Price(ticker1)
       for i in range (len(df)):
         a=df.iloc[i,0]
         b=df.iloc[i,1]
         val=a,ticker,b
         mycursor.execute(sql, val)
         mydb.commit()
       print(ticker)
      except:
       error.append(ticker)
      List=error
    if(len(error)==0):
        break


# In[7]:


#mycursor.execute("truncate price")


# In[8]:


error


# In[ ]:




