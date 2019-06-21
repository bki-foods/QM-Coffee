#!/usr/bin/env python3

import datetime
import urllib
import pandas as pd
from sqlalchemy import create_engine
import pyodbc


# Define server connection and SQL query:
server = r'sqlsrv04\tx'
db = 'TXprodDWH'
con = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db)
query = """SELECT V.[Varenr] AS [ItemNo], V.[Udmeldelsesstatus] AS [Status]
        ,V.[Vareansvar] AS [Department], SVP.[KG], SVP.[Amount]
        ,SVP.[Cost], ISNULL(SVP.[Count],0)  AS [Count]
        ,V.[Dage siden oprettelse] AS [Days]
		,CASE WHEN V.[Produktionskode] NOT LIKE '%HB%' THEN 'FORM' 
            ELSE 'HB' END AS [CType]
        FROM [TXprodDWH].[dbo].[Vare_V] AS V
        LEFT JOIN (
        SELECT [Varenr], SUM(ISNULL([AntalKgKaffe],0)) AS [KG]
        ,SUM([Oms excl. kampagneAnnonce]) AS [Amount]
        ,SUM([Kostbeløb]) AS [Cost], COUNT(*) AS [Count]
        FROM [factSTATISTIK VAREPOST_V]
        WHERE [VarePosttype] IN (-1, 1)
            AND [Bogføringsdato] >= DATEADD(year, -1, getdate())
        GROUP BY [Varenr]
        ) AS SVP
        ON V.[Varenr] = SVP.[Varenr]
        WHERE V.[Varekategorikode] = 'FÆR KAFFE'
            AND V.[Varenr] NOT LIKE '9%'
            AND V.[Rabatnr] = 'Nej'
            AND V.[Salgsvare] = 'Ja'
            AND V.[Udmeldelsesstatus] = '' """

# Read query and create Profit calculation:
df = pd.read_sql(query, con)
df['Profit'] = df['Amount'] - df['Cost']

# Empty dataframes for consolidating segmentation and quantiles:
dfCons = pd.DataFrame()
dfQuan = pd.DataFrame()

# Quantity and MonetaryValue score - bigger numbers are better:
def qm_score(x, para, dic):
    if x <= dic[para][0.25]:
        return 4
    elif x <= dic[para][0.5]:
        return 3
    elif x <= dic[para][0.75]:
        return 2
    else:
        return 1


# Define segment translation (Q, M):
segments = {11: 'Star', 12: 'Cost potential', 13: 'Cost potential',
            14: 'Critical attention', 21: 'Potential star', 22: 'Cost potential',
            23: 'Cost potential', 24: 'Critical attention', 31: 'Volume potential',
            32: 'Volume potential', 33: 'Potential', 34: 'Potential',
            41: 'Volume potential', 42: 'Volume potential', 43: 'Potential',
            44: 'Phase out', 0: 'Dead stock', 1: 'New item'}

# Get todays date and define lists of unique values for loops:
now = datetime.datetime.now()
scriptName = 'QM_Cofee.py'
executionId = int(now.timestamp())
departments = df.Department.unique()
coffeeTypes = df.CType.unique()

# =============================================================================
#                        SKUs with sales
# =============================================================================
dfSales = df.loc[df['Count'] != 0]

for dep in departments:
    for cType in coffeeTypes:
# Create Coffee dataframe, calculations and rename columns:
        dfCof = dfSales.loc[dfSales['Department'] == dep]
        dfCof = dfCof.loc[dfCof['CType'] == cType]
        dfCof.rename(columns={'KG': 'Quantity', 'Profit': 'MonetaryValue'}, inplace=True)
# Define quantiles for dfPro dataframe:
        quantiles = dfCof.quantile(q=[0.25, 0.5, 0.75]).to_dict()
# Identify quartiles per measure for each product:
        dfCof.loc[:, 'QuantityQuartile'] = dfCof['Quantity'].apply(qm_score, args=('Quantity', quantiles,))
        dfCof.loc[:, 'MonetaryQuartile'] = dfCof['MonetaryValue'].apply(qm_score, args=('MonetaryValue', quantiles,))
# Concetenate Quartile measurements to single string:
        dfCof.loc[:, 'Score'] = (dfCof.QuantityQuartile * 10 +
                                 dfCof.MonetaryQuartile)
# Create segmentation code and look up translation in dictionary:
        dfCof.loc[:, 'Segmentation'] = dfCof['Score'].map(segments)
# Create data stamps for dataframe and append to consolidated dataframe:
        dfCof.loc[:, 'Timestamp'] = now
        dfCof.loc[:, 'Type'] = scriptName + '/' + dep + '/' + cType
        dfCof.loc[:, 'ExecutionId'] = executionId
        dfCons = pd.concat([dfCons, dfCof])
# Append quantiles to dataframe
        dfTemp = pd.DataFrame.from_dict(quantiles)
        dfTemp.loc[:, 'Type'] = scriptName + '/' +  dep + '/' + cType
        dfTemp.loc[:, 'Quantile'] = dfTemp.index
        dfQuan = pd.concat([dfTemp, dfQuan], sort=False)
        dfQuan.loc[:, 'Timestamp'] = now
        dfQuan.loc[:, 'ExecutionId'] = executionId

# =============================================================================
#                        SKUs without sales
# =============================================================================
dfNoSales = df.loc[df['Count'] == 0]

dfNoSales.loc[:, 'Timestamp'] = now
dfNoSales.loc[:, 'Score'] = dfNoSales['Days'].apply(lambda x: 0 if x > 90 else 1)
dfNoSales.loc[:, 'Segmentation'] = dfNoSales['Score'].map(segments)
dfNoSales.loc[:, 'Type'] = scriptName + '/' +  dfNoSales['Department'] + '/' + dfNoSales['CType']
dfNoSales.loc[:, 'ExecutionId'] = executionId

# =============================================================================
#                       Prepare dataframes for SQL insert
# =============================================================================
ColsCof = (['ExecutionId', 'Timestamp', 'ItemNo', 'Quantity', 'MonetaryValue',
            'QuantityQuartile', 'MonetaryQuartile', 'Score', 'Segmentation',
            'Type'])
ColsNoS = (['ExecutionId', 'Timestamp', 'ItemNo', 'Score',
            'Segmentation', 'Type'])
ColsQuan = (['ExecutionId', 'Timestamp', 'Type', 'Quantile', 'Quantity',
             'MonetaryValue'])

dfCons = dfCons[ColsCof]
dfNoSales = dfNoSales[ColsNoS]
dfQuan = dfQuan[ColsQuan]

# =============================================================================
#                               Dataframe for logging
# =============================================================================
dfLog = pd.DataFrame(data= {'Date':now, 'Event':scriptName}, index=[0])
# =============================================================================
#                               Insert SQL
# =============================================================================

params = urllib.parse.quote_plus('DRIVER={SQL Server Native Client 10.0};SERVER=sqlsrv04;DATABASE=BKI_Datastore;Trusted_Connection=yes')
engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
dfCons.to_sql('ItemSegmentation', con=engine, schema='seg', if_exists='append', index=False)
dfNoSales.to_sql('ItemSegmentation', con=engine, schema='seg', if_exists='append', index=False)
dfQuan.to_sql('ItemSegmentationQuantiles', con=engine, schema='seg', if_exists='append', index=False)
dfLog.to_sql('Log', con=engine, schema='dbo', if_exists='append', index=False)
