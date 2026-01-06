An end-to-end API-driven Azure Data Lakehouse project using ADF, Databricks, ADLS Gen2, Synapse Serverless SQL, and Power BI for scalable analytics and reporting.

## FINAL ARCHITECTURE we FOLLOW ##

#Source Systems#
   ↓
#Azure Data Factory#
   ↓
#ADLS Gen2 – Bronze (raw files)#
   ↓
#Databricks Job (scheduled / event)#
   ↓
#Silver Delta tables#
   ↓
#Gold Delta tables#
   ↓
#Databricks SQL / Synapse#
   ↓
#Power BI#

