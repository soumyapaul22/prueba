import time
import sys
parms=sys.argv[1:]

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("test").getOrCreate()

from prueba_lib import *

#folder_name = "data-prueba-tecnica"
folder_name = parms[0]
#file_name = "data_prueba_tecnica.csv"
file_name = parms[1]

if __name__ == '__main__':
    batch_id = int(time.time())
    df2 = fileRead(spark,folder_name,file_name)
    #Main table load
    df2_final = filterQualityRow(spark, df2, batch_id)
    getDataLoaded(spark, df=df2_final, type="main")
    #Reject table load
    df2_rejected = rejectedRow(spark,df2,batch_id)
    getDataLoaded(spark, df=df2_rejected, type="rejected")
    #Transaction table load
    df2_transaction = getTransaction(spark,df2_final,batch_id)
    getDataLoaded(spark, df=df2_transaction, type="transaction")
    #Company table load
    df2_company = getCompany(spark, df2_final,batch_id)
    getDataLoaded(spark, df=df2_company, type="company")
    spark.stop()
