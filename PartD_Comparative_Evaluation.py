# Doing part B again but using Spark instead of MRJob. Comparing how the job runs

#References: How To Read CSV File Using Python PySpark, NBSHARE, 2022, https://www.nbshare.io/notebook/187478734/How-To-Read-CSV-File-Using-Python-PySpark/ (accessed 15th April 2022)

# CSV Files, Apache Spark, https://spark.apache.org/docs/latest/sql-data-sources-csv.html (accessed 15th April 2022)

# PySpark Select Columns From DataFrame, Spark by {Examples}, https://sparkbyexamples.com/pyspark/select-columns-from-pyspark-dataframe/ (accessed 15th April 2022)

# PySpark Join Types | Join Two DataFrames, Spark by {Examples}, https://sparkbyexamples.com/pyspark/pyspark-join-explained-with-examples/ (accessed 15th April 2022)

# stackoverflow, https://stackoverflow.com/questions/46956026/how-to-convert-column-with-string-type-to-int-form-in-pyspark-data-frame (accessed 15th April 2022)

# stackoverflow, https://stackoverflow.com/questions/38054197/convert-spark-dataframe-to-float (accessed 15th April 2022)

# stackoverflow, https://stackoverflow.com/questions/46331077/how-to-convert-groupby-into-reducebykey-in-pyspark-dataframe (accessed 15th April 2022)

# Spark Groupby Example with DataFrame, Spark by {Examples}, https://sparkbyexamples.com/spark/using-groupby-on-dataframe/ (accessed 15th April 2022)

# PySpark orderBy() and sort() explained, Spark by {Examples}, https://sparkbyexamples.com/pyspark/pyspark-orderby-and-sort-explained/ (accessed 15th April 2022)

# stackoverflow, https://stackoverflow.com/questions/30332619/how-to-sort-by-column-in-descending-order-in-spark-sql (accessed 15th April 2022)

# Code also refers to some information/guidance from Big Data Science module

# https://spark.apache.org/docs/1.6.2/programming-guide.html





import pyspark

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("part D CourseWork") \
    .getOrCreate()

#googled this part about the spark session


transactions = spark.read.csv("hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions")
#transactions dataframe

contracts = spark.read.csv("hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts")
#contracts dataframe


transactions2 = transactions.select(transactions.columns[2],transactions.columns[3])
#select only the address and value columns

contracts2 = contracts.select(contracts.columns[0])
#select only the address

df = transactions2.join(contracts2, transactions2._c2 == contracts._c0,"inner")
#join on address key

df2 = df.withColumn("_c3",df["_c3"].cast("float"))
#convert the value to float so can sum

aggregation = df2.groupBy("_c2").sum("_c3")
#group the unique addresses together and sum their values

aggregation.sort(aggregation["sum(_c3)"].desc()).show(10)
#sort into descending order and show top 10
