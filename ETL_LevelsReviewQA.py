# -*- coding: utf-8 -*-
"""
Created on Wed Jun 14 09:31:19 2023

@author: haasbroekj
"""

#======================================================================================#
#                                  CREATE VARIABLES                                    #
#======================================================================================#

testing = True
try :
    from datetime import date, timedelta
    import pandas as pd
    from sqlalchemy import create_engine, text
    import numpy as np
    import pyodbc
    
    # Initialize a list to store the dates
    dateshistory = []
    datesforecast = []
    
    today = date.today()
    
    # SQL Connection
    server = '192.168.206.10'
    database = 'ExxaroConversion'
    username = 'FE-haasbroekj'
    password = '23nJi98uhb32'
    engine = create_engine("mssql+pymssql://{user}:{pw}@192.168.206.10/{db}"
                           .format(user="FE-haasbroekj", pw="23nJi98uhb32", db="ExxaroConversion"))
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = conn.cursor()
except ModuleNotFoundError:
    print("Module was not found.")
except NameError:
    print("Name is not defined.")
else :
    print("Variables created")


#try :
#======================================================================================#
#                         CALCULATE & STORE DATES FOR 12MONTHS                         #
#======================================================================================#
# Calculate and store the dates for the last 12 months
for i in range(0, -12, -1):
    delta = timedelta(days=i * 31)  # Assuming each month has 30 days
    date = today + delta
    dateshistory.append(date) 

for i in range(1, 14, 1):
    delta = timedelta(days=i * 31)  # Assuming each month has 30 days
    date = today + delta
    datesforecast.append(date) 

print("Datehistory and DateForecast lists created.")

#======================================================================================#
#                              INTERNAL SQL DB PREP                                    #
#======================================================================================#

with engine.connect() as conn:
   conn.execute(text("EXEC [LEVELS].[DATAINSERT]"))
   
#======================================================================================#
#                                LOAD AND PIVOT DATA                                   #
#======================================================================================#

dfFC = pd.read_sql_table('STAGINGForecast', engine, schema='LEVELS')
print("\nForecast table read.")
dfHS = pd.read_sql_table('STAGINGIssues', engine, schema='LEVELS')
print("History table read.")

dfFC = pd.pivot_table(dfFC, values='QTY', index=['PRODUCTCODE', 'LOCATIONCODE'], 
                    columns=['FORECASTDATE'], aggfunc=np.sum, fill_value=0)
print("\nForecast table pivoted")
print("FORECAST TABLE")

dfHS = pd.pivot_table(dfHS, values='ISSUES', index=['PRODUCTCODE', 'LOCATIONCODE'], 
                    columns=['ISSUEDATE'], aggfunc=np.sum, fill_value=0)
print("History table pivoted")
print("HISTORY TABLE")


#======================================================================================#
#                           ASSIGN STATIC VARIABLES                                    #
#======================================================================================#

# Assign the dates to variables
#dateshistory = []
#datesforecast = []

# Assign the dates to variables
one_months_ago = dateshistory[0]
two_months_ago = dateshistory[1]
three_months_ago = dateshistory[2]
four_months_ago = dateshistory[3]
five_months_ago = dateshistory[4]
six_months_ago = dateshistory[5]
seven_months_ago = dateshistory[6]
eight_months_ago = dateshistory[7]
nine_months_ago = dateshistory[8]
ten_months_ago = dateshistory[9]
eleven_months_ago = dateshistory[10]
twelve_months_ago = dateshistory[11]

# Assign forecast variables
one_months_fc = datesforecast[0]
two_months_fc = datesforecast[1]
three_months_fc = datesforecast[2]
four_months_fc = datesforecast[3]
five_months_fc = datesforecast[4]
six_months_fc = datesforecast[5]
seven_months_fc = datesforecast[6]
eight_months_fc = datesforecast[7]
nine_months_fc = datesforecast[8]
ten_months_fc = datesforecast[10]
eleven_months_fc = datesforecast[11]
twelve_months_fc = datesforecast[12]

print("Static date variables assigned.")

#======================================================================================#
#                           DROP & RECREATE FORECAST TABLE                             #
#======================================================================================#

#Drop and create the new ForecastTable
# Define the table structure without parameterized queries
sql_script = '''
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[LEVELS].[TransposedForecast]') AND type in (N'U'))
    DROP TABLE [LEVELS].[TransposedForecast];

SET ANSI_NULLS ON;

SET QUOTED_IDENTIFIER ON;

CREATE TABLE [LEVELS].[TransposedForecast](
    [PRODUCTCODE] [nvarchar](50) NULL,
    [LOCATIONCODE] [nvarchar](50) NULL,
    {oneFC} [decimal](18, 2) NULL,
    {twoFC} [decimal](18, 2) NULL,
    {threeFC} [decimal](18, 2) NULL,
    {fourFC} [decimal](18, 2) NULL,
    {fiveFC} [decimal](18, 2) NULL,
    {sixFC} [decimal](18, 2) NULL,
    {sevenFC} [decimal](18, 2) NULL,
    {eightFC} [decimal](18, 2) NULL,
    {nineFC} [decimal](18, 2) NULL,
    {tenFC} [decimal](18, 2) NULL,
    {elevenFC} [decimal](18, 2) NULL
) ON [PRIMARY];
'''

# Construct the script by formatting the values directly
formatted_script = sql_script.format(
    elevenFC='[' + str(eleven_months_fc) + ']',
    tenFC='[' + str(ten_months_fc) + ']',
    nineFC='[' + str(nine_months_fc) + ']',
    eightFC='[' + str(eight_months_fc) + ']',
    sevenFC='[' + str(seven_months_fc) + ']',
    sixFC='[' + str(six_months_fc) + ']',
    fiveFC='[' + str(five_months_fc) + ']',
    fourFC='[' + str(four_months_fc) + ']',
    threeFC='[' + str(three_months_fc) + ']',
    twoFC='[' + str(two_months_fc) + ']',
    oneFC='[' + str(one_months_fc) + ']'
)

# Execute the modified script
cursor.execute(formatted_script)
conn.commit()
print("Forecast table dropped and recreated.")

#======================================================================================#
#                           DROP & RECREATE HISTORY TABLE                              #
#======================================================================================#

sql_script2 = '''
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[LEVELS].[TransposedIssues]') AND type in (N'U'))
DROP TABLE [LEVELS].[TransposedIssues];

SET ANSI_NULLS ON;

SET QUOTED_IDENTIFIER ON;

CREATE TABLE [LEVELS].[TransposedIssues](
	[PRODUCTCODE] [nvarchar](50) NULL,
	[LOCATIONCODE] [nvarchar](50) NULL,
    {oneHS} [decimal](18, 2) NULL,
    {twoHS} [decimal](18, 2) NULL,
    {threeHS} [decimal](18, 2) NULL,
    {fourHS} [decimal](18, 2) NULL,
    {fiveHS} [decimal](18, 2) NULL,
    {sixHS} [decimal](18, 2) NULL,
    {sevenHS} [decimal](18, 2) NULL,
    {eightHS} [decimal](18, 2) NULL,
    {nineHS} [decimal](18, 2) NULL,
    {tenHS} [decimal](18, 2) NULL,
	{elevenHS} [decimal](18, 2) NULL
) ON [PRIMARY];
'''

# Construct the script by formatting the values directly
formatted_script2 = sql_script2.format(
    twelveHS='[' + str(twelve_months_ago) + ']',
    elevenHS='[' + str(eleven_months_ago) + ']',
    tenHS='[' + str(ten_months_ago) + ']',
    nineHS='[' + str(nine_months_ago) + ']',
    eightHS='[' + str(eight_months_ago) + ']',
    sevenHS='[' + str(seven_months_ago) + ']',
    sixHS='[' + str(six_months_ago) + ']',
    fiveHS='[' + str(five_months_ago) + ']',
    fourHS='[' + str(four_months_ago) + ']',
    threeHS='[' + str(three_months_ago) + ']',
    twoHS='[' + str(two_months_ago) + ']',
    oneHS='[' + str(one_months_ago) + ']'
)
print("History table dropped and recreated.")

# Execute the modified script
cursor.execute(formatted_script2)
conn.commit()

#======================================================================================#
#                          DROP AND CREATE THE FINAL TABLE                             #
#======================================================================================#

sql_script3 = '''
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[LEVELS].[STOCKMOVEMENT]') AND type in (N'U'))
DROP TABLE [LEVELS].[STOCKMOVEMENT]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [LEVELS].[STOCKMOVEMENT](
	[PRODUCTCODE] [varchar](50) NULL,
	ITEMDESCRIPTION [varchar](max) NULL,
    LEADTIME [varchar] (50) NULL,
	COSTPRICE [float] NULL,
	{oneHS} [INT] NULL,
    {twoHS} [INT] NULL,
    {threeHS} [INT] NULL,
    {fourHS} [INT] NULL,
    {fiveHS} [INT] NULL,
    {sixHS} [INT] NULL,
    {sevenHS} [INT] NULL,
    {eightHS} [INT] NULL,
    {nineHS} [INT] NULL,
    {tenHS} [INT] NULL,
	{elevenHS} [INT] NULL,
    {oneFC} [INT] NULL,
    {twoFC} [INT] NULL,
    {threeFC} [INT] NULL,
    {fourFC} [INT] NULL,
    {fiveFC} [INT] NULL,
    {sixFC} [INT] NULL,
    {sevenFC} [INT] NULL,
    {eightFC} [INT] NULL,
    {nineFC} [INT] NULL,
    {tenFC} [INT] NULL,
    {elevenFC} [INT] NULL,
	[SOH] [INT] NULL,
	REORDER [float] NULL,
	OUL [float] NULL,
	[SAP_MIN] [float] NULL,
	[SAP_MAX] [float] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
'''

formatted_script3 = sql_script3.format(
    twelveHS='[' + str(twelve_months_ago) + ']',
    elevenHS='[' + str(eleven_months_ago) + ']',
    tenHS='[' + str(ten_months_ago) + ']',
    nineHS='[' + str(nine_months_ago) + ']',
    eightHS='[' + str(eight_months_ago) + ']',
    sevenHS='[' + str(seven_months_ago) + ']',
    sixHS='[' + str(six_months_ago) + ']',
    fiveHS='[' + str(five_months_ago) + ']',
    fourHS='[' + str(four_months_ago) + ']',
    threeHS='[' + str(three_months_ago) + ']',
    twoHS='[' + str(two_months_ago) + ']',
    oneHS='[' + str(one_months_ago) + ']',
    elevenFC='[' + str(eleven_months_fc) + ']',
    tenFC='[' + str(ten_months_fc) + ']',
    nineFC='[' + str(nine_months_fc) + ']',
    eightFC='[' + str(eight_months_fc) + ']',
    sevenFC='[' + str(seven_months_fc) + ']',
    sixFC='[' + str(six_months_fc) + ']',
    fiveFC='[' + str(five_months_fc) + ']',
    fourFC='[' + str(four_months_fc) + ']',
    threeFC='[' + str(three_months_fc) + ']',
    twoFC='[' + str(two_months_fc) + ']',
    oneFC='[' + str(one_months_fc) + ']'
)


# Execute the modified script
cursor.execute(formatted_script3)
conn.commit()
print("FINAL table dropped and recreated.")


#======================================================================================#
#                           LOAD DATA BACK TO SQL                                      #
#======================================================================================#
#Clear tables
with engine.connect() as conn:
   conn.execute(text("TRUNCATE TABLE [LEVELS].[TransposedForecast]"))
   conn.execute(text("TRUNCATE TABLE [LEVELS].[TransposedIssues]"))
print("Tables cleared")

dfFC.reset_index(inplace=True)
dfFC.to_sql('TransposedForecast', engine, schema='LEVELS', if_exists='replace', index=False)
print("\nForecast data populated")

dfHS.reset_index(inplace=True)
dfHS.to_sql('TransposedIssues', engine, schema='LEVELS', if_exists='replace', index=False)
print("History data populated")


#======================================================================================#
#                             LOAD FINAL TABLE DATA                                    #
#======================================================================================#

insertDataQuery = '''
INSERT INTO [LEVELS].[STOCKMOVEMENT]
           ([PRODUCTCODE]
           ,[ITEMDESCRIPTION]
		   ,[LEADTIME]
           ,[COSTPRICE]
                ,{oneH}
                ,{twoH}
                ,{threeH}
                ,{fourH}
                ,{fiveH}
                ,{sixH}
                ,{sevenH}
                ,{eightH}
                ,{nineH}
                ,{tenH}
                ,{elevenH}
                ,{oneF}
                ,{twoF}
                ,{threeF}
                ,{fourF}
                ,{fiveF}
                ,{sixF}
                ,{sevenF}
                ,{eightF}
                ,{nineF}
                ,{tenF}
                ,{elevenF}
           ,[SOH]
           ,[REORDER]
           ,[OUL]
           ,[SAP_MIN]
           ,[SAP_MAX])
  SELECT D.[PRODUCTCODE]
      ,[ITEMDESCRIPTION]
      ,D.[LEADTIME]
      ,[COSTPRICE]
                ,{oneHS}
                ,{twoHS}
                ,{threeHS}
                ,{fourHS}
                ,{fiveHS}
                ,{sixHS}
                ,{sevenHS}
                ,{eightHS}
                ,{nineHS}
                ,{tenHS}
                ,{elevenHS}
                ,{oneFC}
                ,{twoFC}
                ,{threeFC}
                ,{fourFC}
                ,{fiveFC}
                ,{sixFC}
                ,{sevenFC}
                ,{eightFC}
                ,{nineFC}
                ,{tenFC}
                ,{elevenFC}
      ,[SOH]
      ,[REORDER]
      ,[OUL]
      ,[SAP_MIN]
      ,[SAP_MAX]
  FROM [LEVELS].[DIMENSIONS] D
  LEFT JOIN [LEVELS].[TransposedForecast] TF ON TF.PRODUCTCODE = D.PRODUCTCODE
  LEFT JOIN [LEVELS].[TransposedIssues] TI ON TF.PRODUCTCODE = TI.PRODUCTCODE;
  '''

insertData = insertDataQuery.format(
    twelveH='[' + str(twelve_months_ago) + ']',
    elevenH='[' + str(eleven_months_ago) + ']',
    tenH=   '[' + str(ten_months_ago) +    ']',
    nineH=  '[' + str(nine_months_ago) +	']',
    eightH= '[' + str(eight_months_ago) +  ']',
    sevenH= '[' + str(seven_months_ago) +	']',
    sixH=   '[' + str(six_months_ago) +	']',
    fiveH=  '[' + str(five_months_ago) +	']',
    fourH=  '[' + str(four_months_ago) +	']',
    threeH= '[' + str(three_months_ago) +	']',
    twoH=   '[' + str(two_months_ago) +	']',
    oneH=   '[' + str(one_months_ago) +	']',
    elevenF='[' + str(eleven_months_fc) +	']',
    tenF=   '[' + str(ten_months_fc) +		']',
    nineF=  '[' + str(nine_months_fc) +	']',
    eightF= '[' + str(eight_months_fc) +	']',
    sevenF= '[' + str(seven_months_fc) +	']',
    sixF=   '[' + str(six_months_fc) +		']',
    fiveF=  '[' + str(five_months_fc) +	']',
    fourF=  '[' + str(four_months_fc) +	']',
    threeF= '[' + str(three_months_fc) +	']',
    twoF=   '[' + str(two_months_fc) +		']',
    oneF=   '[' + str(one_months_fc) +		']',
    twelveHS='CONVERT(INT, CONVERT(FLOAT, [' + str(twelve_months_ago) + ']))',
    elevenHS='CONVERT(INT, CONVERT(FLOAT, [' + str(eleven_months_ago) + ']))',
    tenHS=   'CONVERT(INT, CONVERT(FLOAT, [' + str(ten_months_ago) +    ']))',
    nineHS=  'CONVERT(INT, CONVERT(FLOAT, [' + str(nine_months_ago) +	']))',
    eightHS= 'CONVERT(INT, CONVERT(FLOAT, [' + str(eight_months_ago) +  ']))',
    sevenHS= 'CONVERT(INT, CONVERT(FLOAT, [' + str(seven_months_ago) +	']))',
    sixHS=   'CONVERT(INT, CONVERT(FLOAT, [' + str(six_months_ago) +	']))',
    fiveHS=  'CONVERT(INT, CONVERT(FLOAT, [' + str(five_months_ago) +	']))',
    fourHS=  'CONVERT(INT, CONVERT(FLOAT, [' + str(four_months_ago) +	']))',
    threeHS= 'CONVERT(INT, CONVERT(FLOAT, [' + str(three_months_ago) +	']))',
    twoHS=   'CONVERT(INT, CONVERT(FLOAT, [' + str(two_months_ago) +	']))',
    oneHS=   'CONVERT(INT, CONVERT(FLOAT, [' + str(one_months_ago) +	']))',
    elevenFC='CONVERT(INT, CONVERT(FLOAT, [' + str(eleven_months_fc) +	']))',
    tenFC=   'CONVERT(INT, CONVERT(FLOAT, [' + str(ten_months_fc) +		']))',
    nineFC=  'CONVERT(INT, CONVERT(FLOAT, [' + str(nine_months_fc) +	']))',
    eightFC= 'CONVERT(INT, CONVERT(FLOAT, [' + str(eight_months_fc) +	']))',
    sevenFC= 'CONVERT(INT, CONVERT(FLOAT, [' + str(seven_months_fc) +	']))',
    sixFC=   'CONVERT(INT, CONVERT(FLOAT, [' + str(six_months_fc) +		']))',
    fiveFC=  'CONVERT(INT, CONVERT(FLOAT, [' + str(five_months_fc) +	']))',
    fourFC=  'CONVERT(INT, CONVERT(FLOAT, [' + str(four_months_fc) +	']))',
    threeFC= 'CONVERT(INT, CONVERT(FLOAT, [' + str(three_months_fc) +	']))',
    twoFC=   'CONVERT(INT, CONVERT(FLOAT, [' + str(two_months_fc) +		']))',
    oneFC=   'CONVERT(INT, CONVERT(FLOAT, [' + str(one_months_fc) +		']))'
)

print(insertData)

# Execute vIEW refresh after all the DROPS and CREATES
with engine.connect() as conn:
   conn.execute(text("exec sp_refreshview @viewname='[ExxaroConversion].[LEVELS].[DIMENSIONS]'"))
print("\nViews refreshed.")
cursor.execute(insertData)
conn.commit()
print("FINAL table populated.")


#======================================================================================#
#                          CLOSE THE CURSOR AND CONNECTION                             #
#======================================================================================#
if testing == True :
    print("In testing")
elif testing == False :
    cursor.close()
    conn.close()
    print("Cursor and connection closed")
        
    #======================================================================================#
    #                          PROCESS COMPLETED SUCCESSFUL                                #
    #======================================================================================#
    
#except :
#    print("Something went wrong.")
#else :
#    print("ETL finished successfully!")
    
"""

