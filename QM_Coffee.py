﻿#!/usr/bin/env python3

import datetime
import urllib
import pandas as pd
from sqlalchemy import create_engine


# Define server connection and SQL query:
server_tx = r"sqlsrv04\tx"
db_tx_prod_dwh = "TXprodDWH"
params_tx_prod_dwh = f"DRIVER={{SQL Server Native Client 11.0}};SERVER={server_tx};DATABASE={db_tx_prod_dwh};trusted_connection=yes"
con_tx_prod_dwh = create_engine('mssql+pyodbc:///?odbc_connect=%s' % urllib.parse.quote_plus(params_tx_prod_dwh))

server_04 = "sqlsrv04"
db_ds = "BKI_Datastore"
params_ds = f"DRIVER={{SQL Server Native Client 11.0}};SERVER={server_04};DATABASE={db_ds};trusted_connection=yes"
con_ds = create_engine('mssql+pyodbc:///?odbc_connect=%s' % urllib.parse.quote_plus(params_ds))

query = """ SELECT V.[Varenr] AS [ItemNo], V.[Udmeldelsesstatus] AS [Status]
        ,V.[Vareansvar] AS [Department], SVP.[KG], SVP.[Amount]
        ,SVP.[Cost], V.[Dage siden oprettelse] AS [Days]
		, CASE WHEN V.[Udmeldelsesstatus] = 'Er udgået'
			THEN 0 ELSE ISNULL(SVP.[Count],0) END AS [Count]
        ,CASE WHEN V.[Produktionskode] NOT LIKE '%HB%' THEN 'FORM' 
            ELSE 'HB' END AS [CType]
        FROM [TXprodDWH].[dbo].[Vare_V] AS V
        LEFT JOIN (
        SELECT [Varenr], SUM(ISNULL([AntalKgKaffe],0)) AS [KG]
        ,SUM([Oms excl. kampagneAnnonce]) AS [Amount]
        ,SUM([Kostbeløb]) AS [Cost], COUNT(*) AS [Count]
        FROM [TXprodDWH].[dbo].[factSTATISTIK VAREPOST_V]
        WHERE [VarePosttype] = 1
            AND [Bogføringsdato] >= DATEADD(year, -1, getdate())
        GROUP BY [Varenr]
        ) AS SVP
        ON V.[Varenr] = SVP.[Varenr]
        WHERE V.[Varekategorikode] = 'FÆR KAFFE'
            AND V.[Varenr] NOT LIKE '9%'
            AND V.[Salgsvare] = 'Ja'
			AND V.[DW_Account] = 'BKI foods a/s' """
# Read query and create Profit calculation:
df = pd.read_sql(query, con_tx_prod_dwh)
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


# Get todays date and define lists of unique values for loops:
now = datetime.datetime.now()
scriptName = 'QM_Cofee.py'
executionId = int(now.timestamp())
coffeeTypes = df.CType.unique()
departments = df.Department.unique()

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
# If dataframe is empty, skip department & type
        if len(dfCof) != 0:
# Define quantiles for dfPro dataframe:
            quantiles = dfCof.quantile(q=[0.25, 0.5, 0.75]).to_dict()
# Identify quartiles per measure for each product:
            dfCof.loc[:, 'QuantityQuartile'] = dfCof['Quantity'].apply(qm_score, args=('Quantity', quantiles,))
            dfCof.loc[:, 'MonetaryQuartile'] = dfCof['MonetaryValue'].apply(qm_score, args=('MonetaryValue', quantiles,))
# Concetenate Quartile measurements to single string:
            dfCof.loc[:, 'Score'] = dfCof.QuantityQuartile * 10 + dfCof.MonetaryQuartile
# Create data stamps for dataframe and append to consolidated dataframe:
            dfCof.loc[:, 'Timestamp'] = now
            dfCof.loc[:, 'Type'] = dep + '/' + cType
            dfCof.loc[:, 'ExecutionId'] = executionId
            dfCof.loc[:, 'Script'] = scriptName
            dfCons = pd.concat([dfCons, dfCof])
# Append quantiles to dataframe
            dfTemp = pd.DataFrame.from_dict(quantiles)
            dfTemp.loc[:, 'Type'] = dep + '/' + cType
            dfTemp.loc[:, 'Quantile'] = dfTemp.index
            dfQuan = pd.concat([dfTemp, dfQuan], sort=False)
            dfQuan.loc[:, 'Timestamp'] = now
            dfQuan.loc[:, 'ExecutionId'] = executionId

# =============================================================================
#                        SKUs without sales
# =============================================================================
dfNoSales = df.loc[df['Count'] == 0]

dfNoSales.loc[:, 'Timestamp'] = now
dfNoSales.loc[:, 'Score'] = dfNoSales['Days'].apply(lambda x: 1 if x > 90 else 2)
dfNoSales.loc[dfNoSales['Status'] == 'Er udgået', 'Score'] = 0
dfNoSales.loc[:, 'Type'] = dfNoSales['Department'] + '/' + dfNoSales['CType']
dfNoSales.loc[:, 'ExecutionId'] = executionId
dfNoSales.loc[:, 'Script'] = scriptName

# =============================================================================
#                       Prepare dataframes for SQL insert
# =============================================================================
ColsCof = ['ExecutionId', 'Timestamp', 'ItemNo', 'Quantity', 'MonetaryValue', 'Score', 'Type', 'Script']
ColsNoS = ['ExecutionId', 'Timestamp', 'ItemNo', 'Score', 'Type', 'Script']
ColsQuan = (['ExecutionId', 'Timestamp', 'Type', 'Quantile', 'Quantity',
             'MonetaryValue'])

dfCons = dfCons[ColsCof]
dfNoSales = dfNoSales[ColsNoS]
dfQuan = dfQuan[ColsQuan]

# =============================================================================
#                               Dataframe for logging
# =============================================================================
dfLog = pd.DataFrame(data= {'Date':now, 'Event':scriptName, 'Note':'Execution id: ' + str(executionId)}, index=[0])
# =============================================================================
#                               Insert SQL
# =============================================================================
dfCons.to_sql('ItemSegmentation', con=con_ds, schema='seg', if_exists='append', index=False)
dfNoSales.to_sql('ItemSegmentation', con=con_ds, schema='seg', if_exists='append', index=False)
dfQuan.to_sql('ItemSegmentationQuantiles', con=con_ds, schema='seg', if_exists='append', index=False)
dfLog.to_sql('Log', con=con_ds, schema='dbo', if_exists='append', index=False)

