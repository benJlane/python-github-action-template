import slack
import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

client = slack.WebClient(token = os.environ['SLACK_TOKEN'])

import pypyodbc as odbc


DRIVER_NAME = 'SQL Server Native Client 11.0'
SERVER_NAME = '172.31.99.100'
DATABASE_NAME = 'DS'

connection_string = f"""
    DRIVER={{{DRIVER_NAME}}};
    SERVER={SERVER_NAME};
    DATABASE={DATABASE_NAME};
    Trusted_Connection=Yes;
    UID=SF\benjamin.lane;
    PWD=+RlpOna_$c?WejlthI9r;
    MARS_Connection=Yes;
"""

conn = odbc.connect(connection_string)
print(conn)

BASE = """
DROP TABLE IF EXISTS #AllArrowInbound
SELECT DISTINCT master_contact_id, contact_name, campaign_name,agent_name, team_name, agent_time, abandon, Start_DateTime, InQueue, PostQueue INTO #AllArrowInbound FROM 
(
SELECT * FROM DS.DBO.NiceInContactCalls
WHERE team_name = 'N/A'
AND contact_name = '+442038903750'
AND campaign_name LIKE '%collections%'

UNION

SELECT * FROM DS.DBO.NiceInContactCalls
WHERE contact_name = '+442039001912'

UNION 

SELECT * FROM DS.DBO.NiceInContactCalls
WHERE team_name = 'Team Arrow'

)
#tmp
WHERE team_name IN ('N/A','Team Arrow')
AND contact_name <> 'Outbound'
AND CAST(Start_DateTime AS DATE) >= '2022-10-21'
"""

conn.cursor().execute(BASE)

Query = """
SELECT 
CAST(Start_DateTime AS DATE) Date
,COUNT(DISTINCT master_contact_id) 'Volume'
,AVG(CASE WHEN abandon = 'y' THEN 1.0 ELSE 0.0 END) 'Abandon %'
FROM #AllArrowInbound
WHERE InQueue + Agent_time > 0
AND CAST(Start_DateTime AS DATE) > DATEADD(DAY,-10,GETDATE())
GROUP BY CAST(Start_DateTime AS DATE)
ORDER BY 1
"""
df = pd.read_sql(Query,conn)
df['date'] = pd.to_datetime(df['date'])

DayCheck = 'last Friday' if datetime.now().strftime("%A") == 'Monday' else 'yesterday'

DateProduce = datetime.now() - timedelta(1) if DayCheck != 'Monday' else datetime.now() - timedelta(3)
DateProduce = datetime.strftime(DateProduce, '%Y-%m-%d')

MinDate = datetime.strftime(datetime.now() - timedelta(10), '%Y-%m-%d')

Recent = df[df['volume'] > 10][df['date'] > MinDate]

col_list = Recent['abandon %'].values.tolist()
col_list2 = pd.to_datetime(Recent['date'].unique()).tolist()
col_list3 = Recent['volume'].values.tolist()
Output = ''
for i in range(len(col_list)):
    Output += str(datetime.strftime(col_list2[i], '%d %b')) + ': ' + str(int(round(100 * col_list[i]))) + '%' + ' (' + str(col_list3[i]) + '), '
    
Volume = str(int(df[df['date'] == DateProduce]['volume']))
Rate = str(int(round(100*df[df['date'] == DateProduce]['abandon %'],0))) + '%'

client.chat_postMessage(channel = '#slackbottest',text = """
This is an automated SlackBot message. 

Arrow collections stats for """ + DayCheck

+ """                        
Inbound Calls ---- """ + Volume
+"""
Abandon Rate  ---- """ + Rate 
+"""
The compares to recent days: """ + Output)
