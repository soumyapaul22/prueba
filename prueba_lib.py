from pyspark.sql.functions import regexp_replace, col, length,lit
from pyspark.sql.functions import unix_timestamp,from_unixtime
from pyspark.sql.types import TimestampType,DecimalType

def fileRead(spark,folder_name,file_name):
    file_path = "s3n://{}/{}".format(folder_name,file_name)
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    # Formating columns to TimeStamp Type and Decimal Type
    df2 = df.withColumn("created_at", from_unixtime(unix_timestamp("created_at", "yyyy-MM-dd"))).withColumn("created_at", col("created_at").cast(TimestampType())).withColumn("amount", col("amount").cast(DecimalType(38, 2))).withColumn("amount_len", length(col("amount")))
    return (df2)

def filterQualityRow(spark,df2,batch_id):
    df2_final = df2.where(col("id").isNotNull() & col("company_id").isNotNull() & col("amount").isNotNull() & col("status").isNotNull() & col("created_at").isNotNull()).where("amount_len < 20").withColumn("batch_id",lit(batch_id))
    df2_final = df2_final.withColumnRenamed("paid_at", "updated_at").withColumnRenamed("name","company_name").withColumn("batch_id", lit(batch_id))
    df2_final = df2_final.select('id', 'company_name', 'company_id', 'amount', 'status', 'created_at', 'updated_at','batch_id')
    return (df2_final)

def rejectedRow(spark,df2,batch_id):
    df2_rejected = df2.where(col("id").isNull() | col("company_id").isNull() | col("amount").isNull() | col("status").isNull() | col("created_at").isNull() | (col("amount_len") > 19))
    df2_rejected = df2_rejected.withColumnRenamed("paid_at", "updated_at").withColumnRenamed("name", "company_name").withColumn("batch_id", lit(batch_id))
    df2_rejected = df2_rejected.select('id', 'company_name', 'company_id', 'amount', 'status', 'created_at','updated_at', 'batch_id')
    return (df2_rejected)

def getTransaction(spark,df2_final,batch_id):
    df2_transaction = df2_final.select('id', 'company_id', 'amount', 'status', 'created_at', 'updated_at', 'batch_id').where("batch_id == batch_id")
    return (df2_transaction)

def getCompany(spark,df2_final,batch_id):
    df2_company = df2_final.select("company_name", "company_id","batch_id").where("batch_id == batch_id").distinct()
    return (df2_company)

def getDataLoaded(spark,df,folder_name,type):
    aws_endpoint = "pruebatech.cncj1ormdfie.us-east-1.rds.amazonaws.com"
    port = "5432"
    db_name = "testdb"
    user = "******"
    password = "*****"
    pgurl = "jdbc:postgresql://{a}:{b}/{c}?user={d}&password={e}".format(a=aws_endpoint, b=port, c=db_name, d=user, e=password)
    properties = {"user": user, "password": password, "driver": "org.postgresql.Driver", "mode": "append"}
    if type == "main":
        table_name = "main_table"
    elif type == "rejected":
        table_name = "test_table_rejected"
    elif type == "company":
        table_name = "company_table"
    elif type == "transaction":
        table_name = "transaction_table"
    df.write.jdbc(url=pgurl, table=table_name, mode='append', properties=properties)
    table_path="s3n://{}/{}/".format(folder_name, table_name)
    df.write.mode("Append").option("partitionOverwriteMode", "dynamic").partitionBy("batch_id").parquet(table_path)
    print("{a} has been loaded with row count of {b} and file kept in folder {c}".format(a=table_name, b=df.count(),c=table_path))
