from sqlalchemy import create_engine, text
from datetime import date, timedelta

# Initialize a list to store the dates
dateshistory = []
datesforecast = []

today = date.today()

# Calculate and store the dates for the last 12 months
for i in range(0, -12, -1):
    delta = timedelta(days=i * 31)  # Assuming each month has 30 days
    date = today + delta
    dateshistory.append(date) 

for i in range(1, 13, 1):
    delta = timedelta(days=i * 31)  # Assuming each month has 30 days
    date = today + delta
    datesforecast.append(date) 

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
twelve_months_fc = datesforecast[11]

# Print the assigned dates
count = 0
for datei in range(len(dateshistory)) :
    print(count+1, " months ago: ",dateshistory[count])
    count+=1

count = 0
for datek in range(len(datesforecast)) :
    print(count+1, " months fwd: ",datesforecast[count])
    count+=1

# Print the assigned dates
"""print("Dates from the last 12 months:")
print("One month ago:", one_month_ago)
print("Two months ago:", two_months_ago)
print("Three months ago:", three_months_ago)
print("Four months ago:", four_months_ago)
print("Five months ago:", five_months_ago)
print("Six months ago:", six_months_ago)
print("Seven months ago:", seven_months_ago)
print("Eight months ago:", eight_months_ago)
print("Nine months ago:", nine_months_ago)
print("Ten months ago:", ten_months_ago)
print("Eleven months ago:", eleven_months_ago)
print("Twelve months ago:", twelve_months_ago)"""

"""month, year = (today.month-1, today.year) #if today.month != 1 else (12, today.year-1)
print(today)
print(hist12)
print(hist11)"""


# SQL Connection
engine = create_engine("mssql+pymssql://{user}:{pw}@FE-SQL/{db}"
                       .format(user="FE-haasbroekj", pw="23nJi98uhb32", db="UKPower_Landing"))

#engine.execute(text(""" DECLARE @return int EXEC @return = [dbo].[AnalyseFY23] '{User}', '{Status}' 
 #                                   SELECT 'RETURN' = @return """.format(User = author, Status = 'Pass')).execution_options(autocommit=True))
        