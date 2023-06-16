#======================================================================================#
#                                  CREATE VARIABLES                                    #
#======================================================================================#

from datetime import date, timedelta
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import pyodbc

# Initialize a list to store the dates
dateshistory = []
datesforecast = []

today = date.today()

# SQL Connection
server = 'FE-SQL'
database = 'ExxaroConversion'
username = 'FE-haasbroekj'
password = '23nJi98uhb32'
engine = create_engine("mssql+pymssql://{user}:{pw}@FE-SQL/{db}"
                       .format(user="FE-haasbroekj", pw="23nJi98uhb32", db="ExxaroConversion"))
conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = conn.cursor()
print("Variables created")

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

# Print the assigned dates
"""count = 0
for datei in range(len(dateshistory)) :
    print(count+1, " months ago: ",dateshistory[count])
    count+=1

count = 0
for datek in range(len(datesforecast)) :
    print(count+1, " months fwd: ",datesforecast[count])
    count+=1 """

#======================================================================================#
#                           ASSIGN STATIC VARIABLES                                    #
#======================================================================================#

# Assign the dates to variables
one_month_ago = dateshistory[0]
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
    {elevenFC} [decimal](18, 2) NULL,
    {tenFC} [decimal](18, 2) NULL,
    {nineFC} [decimal](18, 2) NULL,
    {eightFC} [decimal](18, 2) NULL,
    {sevenFC} [decimal](18, 2) NULL,
    {sixFC} [decimal](18, 2) NULL,
    {fiveFC} [decimal](18, 2) NULL,
    {fourFC} [decimal](18, 2) NULL,
    {threeFC} [decimal](18, 2) NULL,
    {twoFC} [decimal](18, 2) NULL,
    {oneFC} [decimal](18, 2) NULL
) ON [PRIMARY];
'''

# Construct the script by formatting the values directly
formatted_script = sql_script.format(
    elevenFC='[' + str(twelve_months_fc) + ']',
    tenFC='[' + str(eleven_months_fc) + ']',
    nineFC='[' + str(ten_months_fc) + ']',
    eightFC='[' + str(nine_months_fc) + ']',
    sevenFC='[' + str(eight_months_fc) + ']',
    sixFC='[' + str(seven_months_fc) + ']',
    fiveFC='[' + str(six_months_fc) + ']',
    fourFC='[' + str(five_months_fc) + ']',
    threeFC='[' + str(four_months_fc) + ']',
    twoFC='[' + str(three_months_fc) + ']',
    oneFC='[' + str(two_months_fc) + ']'
)

# Execute the modified script
cursor.execute(formatted_script)
conn.commit()
print("Forecast table dropped and recreated.")


#======================================================================================#
#                           DROP & RECREATE HISTORY TABLE                              #
#======================================================================================#

sql_script = '''/****** Object:  Table [LEVELS].[TransposedIssues]    Script Date: 2023/06/07 13:13:59 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[LEVELS].[TransposedIssues]') AND type in (N'U'))
DROP TABLE [LEVELS].[TransposedIssues]
GO

/****** Object:  Table [LEVELS].[TransposedIssues]    Script Date: 2023/06/07 13:13:59 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [LEVELS].[TransposedIssues](
	[PRODUCTCODE] [nvarchar](50) NULL,
	[LOCATIONCODE] [nvarchar](50) NULL,
	{elevenHS} [decimal](18, 2) NULL,
    {tenHS} [decimal](18, 2) NULL,
    {nineHS} [decimal](18, 2) NULL,
    {eightHS} [decimal](18, 2) NULL,
    {sevenHS} [decimal](18, 2) NULL,
    {sixHS} [decimal](18, 2) NULL,
    {fiveHS} [decimal](18, 2) NULL,
    {fourHS} [decimal](18, 2) NULL,
    {threeHS} [decimal](18, 2) NULL,
    {twoHS} [decimal](18, 2) NULL,
    {oneHS} [decimal](18, 2) NULL
) ON [PRIMARY]
GO'''

# Construct the script by formatting the values directly
formatted_script = sql_script.format(
    elevenHS='[' + str(twelve_months_ago) + ']',
    tenHS='[' + str(eleven_months_ago) + ']',
    nineHS='[' + str(ten_months_ago) + ']',
    eightHS='[' + str(nine_months_ago) + ']',
    sevenHS='[' + str(eight_months_ago) + ']',
    sixHS='[' + str(seven_months_ago) + ']',
    fiveHS='[' + str(six_months_ago) + ']',
    fourHS='[' + str(five_months_ago) + ']',
    threeHS='[' + str(four_months_ago) + ']',
    twoHS='[' + str(three_months_ago) + ']',
    oneHS='[' + str(two_months_ago) + ']'
)
print("History table dropped and recreated.")

#======================================================================================#
#                          CLOSE THE CURSOR AND CONNECTION                             #
#======================================================================================#
cursor.close()
conn.close()
print("Cursor and connection closed")

#======================================================================================#
#                                  PIVOT AND LOAD DATA                                 #
#======================================================================================#

dfFC = pd.read_sql_table('STAGINGForecast', engine, schema='LEVELS')
print("\nForecast table read.")
dfHS = pd.read_sql_table('STAGINGIssues', engine, schema='LEVELS')
print("History table read.")

dfFC = pd.pivot_table(dfFC, values='QTY', index=['PRODUCTCODE', 'LOCATIONCODE'], 
                    columns=['FORECASTDATE'], aggfunc=np.sum, fill_value=0)
print("\nForecast table pivoted")
dfHS = pd.pivot_table(dfHS, values='ISSUES', index=['PRODUCTCODE', 'LOCATIONCODE'], 
                    columns=['ISSUEDATE'], aggfunc=np.sum, fill_value=0)
print("History table pivoted")
dfFC

dfFC.to_sql('TransposedForecast', engine, schema='LEVELS', if_exists='replace')
print("\nForecast data populated")
dfHS.to_sql('TransposedIssues', engine, schema='LEVELS', if_exists='replace')
print("History data populated")


