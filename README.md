# Testing-URl-data-into-pyspark-shell
This a small test regarding how to create a .py file in edge node and test into pyspark shell before spark-submit.
# testing to read url data in pyspark shell

     Step-1 Go to the edge node and create a file t_demo_spark_submit.py file
     Step-2 Copy the following code and paste in your VS code or pyspark shell
     Step-3 Flatten the data using exlode function and check the result

# Go to the pyspark shell and paste the code written in t_demo_spark_submit.py file 

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
     df=spark.read.json(rdd)
     df.show()
     df.printSchema()
     
# Flateen the column using explode funtion

    flattendf = df.withColumn("results",
    explode(col("results"))).select("nationality","results.user.cell","results.user.dob","results.user.email","results.user.gender","results.user.location.*")
    flattendf.show()
