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
