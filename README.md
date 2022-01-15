# Testing-URl-data-into-pyspark-shell
This a small test regarding how to create a .py file in edge node and test into pyspark shell before spark-submit.
# testing to read url data in pyspark shell

# go to the pyspark shell and paste the code written in t_demo_spark_submit.py file 
>>> import urllib
>>> from urllib import request
0").read()a = urllib.request.urlopen("https://randomuser.me/api/0.8/?results=5
>>> urlstring =urldata.decode("utf8")
>>> print(urlstring)
{
    "results": [
        {
            "user": {
                "gender": "female",
                "name": {
                    "title": "ms",
                    "first": "melodie",
                    "last": "claire"
                },
                "location": {
                    "street": "7510 9th st",
                    "city": "stirling",
                    "state": "québec",
                    "zip": 55554
                },
                "email": "melodie.claire@example.com",
                "username": "ticklishcat175",
                "password": "hotone",
                "salt": "XBPPbY13",
                "md5": "ed865e273401ad3e279d415c5b2884cd",
                "sha1": "c826a6413db28b9e651165741d1f1ac5edcac562",
                "sha256": "590a5dcd8a2dc70c16d79b7374039af0405347c9b1662a0f03c81bb7618bba3b",
                "registered": 1017507188,
                "dob": 167546229,
                "phone": "273-482-4865",
                "cell": "520-652-2666",
                "picture": {
                    "large": "https://randomuser.me/api/portraits/women/64.jpg",
                    "medium": "https://randomuser.me/api/portraits/med/women/64.jpg",
                    "thumbnail": "https://randomuser.me/api/portraits/thumb/women/64.jpg"
                }
            }
        }
    ],
    "nationality": "CA",
    "seed": "e7ca0fbb688edc1602",
    "version": "0.8"
}
>>> print(urlstring)
{
    "results": [
        {
            "user": {
                "gender": "female",
                "name": {
                    "title": "ms",
                    "first": "melodie",
                    "last": "claire"
                },
                "location": {
                    "street": "7510 9th st",
                    "city": "stirling",
                    "state": "québec",
                    "zip": 55554
                },
                "email": "melodie.claire@example.com",
                "username": "ticklishcat175",
                "password": "hotone",
                "salt": "XBPPbY13",
                "md5": "ed865e273401ad3e279d415c5b2884cd",
                "sha1": "c826a6413db28b9e651165741d1f1ac5edcac562",
                "sha256": "590a5dcd8a2dc70c16d79b7374039af0405347c9b1662a0f03c81bb7618bba3b",
                "registered": 1017507188,
                "dob": 167546229,
                "phone": "273-482-4865",
                "cell": "520-652-2666",
                "picture": {
                    "large": "https://randomuser.me/api/portraits/women/64.jpg",
                    "medium": "https://randomuser.me/api/portraits/med/women/64.jpg",
                    "thumbnail": "https://randomuser.me/api/portraits/thumb/women/64.jpg"
                }
            }
        }
    ],
    "nationality": "CA",
    "seed": "e7ca0fbb688edc1602",
    "version": "0.8"
}
>>> rdd=sc.parallelize([urlstring])
>>> df=spark.read.json(rdd)
>>> df.show()
+-----------+--------------------+------------------+-------+
|nationality|             results|              seed|version|
+-----------+--------------------+------------------+-------+
|         CA|[[[520-652-2666, ...|e7ca0fbb688edc1602|    0.8|
+-----------+--------------------+------------------+-------+

>>> df.printSchema()
root
 |-- nationality: string (nullable = true)
 |-- results: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- user: struct (nullable = true)
 |    |    |    |-- cell: string (nullable = true)
 |    |    |    |-- dob: long (nullable = true)
 |    |    |    |-- email: string (nullable = true)
 |    |    |    |-- gender: string (nullable = true)
 |    |    |    |-- location: struct (nullable = true)
 |    |    |    |    |-- city: string (nullable = true)
 |    |    |    |    |-- state: string (nullable = true)
 |    |    |    |    |-- street: string (nullable = true)
 |    |    |    |    |-- zip: long (nullable = true)
 |    |    |    |-- md5: string (nullable = true)
 |    |    |    |-- name: struct (nullable = true)
 |    |    |    |    |-- first: string (nullable = true)
 |    |    |    |    |-- last: string (nullable = true)
 |    |    |    |    |-- title: string (nullable = true)
 |    |    |    |-- password: string (nullable = true)
 |    |    |    |-- phone: string (nullable = true)
 |    |    |    |-- picture: struct (nullable = true)
 |    |    |    |    |-- large: string (nullable = true)
 |    |    |    |    |-- medium: string (nullable = true)
 |    |    |    |    |-- thumbnail: string (nullable = true)
 |    |    |    |-- registered: long (nullable = true)
 |    |    |    |-- salt: string (nullable = true)
 |    |    |    |-- sha1: string (nullable = true)
 |    |    |    |-- sha256: string (nullable = true)
 |    |    |    |-- username: string (nullable = true)
 |-- seed: string (nullable = true)
 |-- version: string (nullable = true)

>>>
>>> flattendf = df.withColumn("results",
ser.dob","results.user.email","results.user.gender","results.user.location.*").u
>>> flattendf.show()
+-----------+------------+---------+--------------------+------+--------+------+-----------+-----+
|nationality|        cell|      dob|               email|gender|    city| state|     street|  zip|
+-----------+------------+---------+--------------------+------+--------+------+-----------+-----+
|         CA|520-652-2666|167546229|melodie.claire@ex...|female|stirling|québec|7510 9th st|55554|
+-----------+------------+---------+--------------------+------+--------+------+-----------+-----+
