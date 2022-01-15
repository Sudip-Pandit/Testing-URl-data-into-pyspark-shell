import urllib
from urllib import request
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark import SparkContext
sc = SparkContext("local", "First App")
urldata = urllib.request.urlopen("https://randomuser.me/api/0.8/?results=5").read()
urlstring =urldata.decode("utf8")
print(urlstring)
print(urlstring)
rdd=sc.parallelize([urlstring])

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df=spark.read.json(rdd)
df.show()
df.printSchema()

flattendf = df.withColumn("results",
explode(col("results"))).select("nationality","results.user.cell","results.user.dob","results.user.email","results.user.gender","results.user.location.*")
flattendf.show()
