# prueba
1. pruebaMain.py is the main PySpark script to be executed.
2. prueba_lib.py is the library of function, which is getting imported by pruebaMain.py
3. Library has below functions,
  fileRead :: To read input csv file in with header and creats the dataframe with InferSchema, It also,, perform the schema change for column "created_at" and identfy the length of column "amount", which is a decimal field.
  filterQualityRow ::  It perform the Null quality check for columns "Id","company_id","amount","status","created_at" and length Check for column "Amount". The rename "paid_at" to "updated_at" and "name" to "company_name". Captured the out as a dataframe to load in main_table
  rejectedRow :: It capture all the rows where above quality check failed.
  getTransaction :: Identify Dimension table - Transactions from the output from function filterQualityRow.
  getCompany :: Identify Fact Table - Company from the output from function filterQualityRow. 
  getDataLoaded :: To load the data in PostgraseSql table, in append Mode.

4. Sample Spark Submit Command is as,

  "spark-submit --deploy-mode client --jars s3://emr-spark-dependencies/postgresql-42.2.11.jar --py-files s3://data-prueba-tecnica/prueba_lib.py s3://data-prueba-tecnica/pruebaMain.py data-prueba-tecnica data_prueba_tecnica.csv"
  
  
  
