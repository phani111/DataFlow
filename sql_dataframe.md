# dataframe全部步骤


```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnull
spark = SparkSession.builder.appName("dataproc").config('spark.jars','https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.11/2.4.4/spark-avro_2.11-2.4.4.jar').getOrCreate()
df1 = spark.read.format('avro').load('../data/jobdata/Prev_RD.avro').fillna(0)
df2 = spark.read.format('avro').load('../data/jobdata/Curr_RD.avro')
df3 = df2.select('ARNG_ID')
df4 = df2.select('ARNG_ID')
df5 = df3.join(df1,["ARNG_ID"],'left_outer')
df6 = df5.filter(df5.NUM_OF_MTHS_PD_30>=1)
df6.select('ARNG_ID').count()
df6.show(5) #867
df7 = df5.filter((isnull("NUM_OF_MTHS_PD_30")) | (df5.NUM_OF_MTHS_PD_30 < 1))
df8 = df7.fillna({"NUM_OF_MTHS_PD_30": 0})
df9 = df6.union(df8)
df10=df4.join(df9,['ARNG_ID'],'left')
df10.write.mode('overwrite').format('avro').save('./data/dataframejob')
```

    +-------+-----------------+--------+--------+--------+--------+--------+
    |ARNG_ID|NUM_OF_MTHS_PD_30| FIELD_1| FIELD_2| FIELD_3| FIELD_4| FIELD_5|
    +-------+-----------------+--------+--------+--------+--------+--------+
    | 004003|              1.0|4329.227|4564.128|8155.947|5364.827|6857.759|
    | 004004|              2.0|6434.676|2394.595| 9481.11|3354.332|1651.854|
    | 004005|              2.0|6630.857|1598.569|7658.347|1004.065|5822.402|
    | 004006|              6.0|1589.694|4613.918|9300.003|6964.704|4290.835|
    | 004008|              3.0|8536.384|8203.271|5190.138|7836.816|5566.414|
    +-------+-----------------+--------+--------+--------+--------+--------+
    only showing top 5 rows
    


# 初始化sparksession


```python
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("dataproc").config('spark.jars','https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.11/2.4.4/spark-avro_2.11-2.4.4.jar').getOrCreate()
```

#  读取两个文件创建两个dataframe对象,df1,df2,df1为prev_RD生成,df2为Curr_RD生成


```python
df1 = spark.read.format('avro').load('../data/jobdata/Prev_RD.avro').fillna(0)
df2 = spark.read.format('avro').load('../data/jobdata/Curr_RD.avro')
df1.show(2)
df2.show(2)

# sql
# df1.createOrReplaceTempView('prev')
# df2.createOrReplaceTempView('curr')
```

    +-------+-----------------+--------+--------+--------+--------+--------+
    |ARNG_ID|NUM_OF_MTHS_PD_30| FIELD_1| FIELD_2| FIELD_3| FIELD_4| FIELD_5|
    +-------+-----------------+--------+--------+--------+--------+--------+
    | 004001|              0.0|3141.444|3124.197|6605.492|8139.267|3914.171|
    | 004002|              0.0|5897.519|1928.391|7675.341|8396.764|2357.492|
    +-------+-----------------+--------+--------+--------+--------+--------+
    only showing top 2 rows
    
    +-------+-----------------+--------+--------+--------+--------+--------+
    |ARNG_ID|NUM_OF_MTHS_PD_30| FIELD_1| FIELD_2| FIELD_3| FIELD_4| FIELD_5|
    +-------+-----------------+--------+--------+--------+--------+--------+
    | 004001|              NaN|3141.444|3124.197|6605.492|8139.267|3914.171|
    | 004002|              NaN|5897.519|1928.391|7675.341|8396.764|2357.492|
    +-------+-----------------+--------+--------+--------+--------+--------+
    only showing top 2 rows
    


# 分部进行ETL操作

## 只保留curr_amg表的ARNG_ID字段,其他去掉,自我复制成两份df3,df4


```python
df3 = df2.select('ARNG_ID')
df4 = df2.select('ARNG_ID')
df3.show(2)
# df3.describe().show()
df4.show(2)
# df4.printSchema()

# sql

# df3 = spark.sql("select ARNG_ID from curr")
# df3.createOrReplaceTempView('curr')
# df4 = spark.sql("select ARNG_ID from curr")
# df3.createOrReplaceTempView('df4')
# df3.show(2)
# # df3.describe().show()
# df4.show(2)
# # df4.printSchema()

```

    +-------+
    |ARNG_ID|
    +-------+
    | 004001|
    | 004002|
    +-------+
    only showing top 2 rows
    
    +-------+
    |ARNG_ID|
    +-------+
    | 004001|
    | 004002|
    +-------+
    only showing top 2 rows
    


## df3为左表,left join df1,形成df5


```python
df5 = df3.join(df1,["ARNG_ID"],'left_outer')
df5.show(5)

#sql
# df1.columns[1:]
# fileds = ','.join(df1.columns[1:])
# spark.sql("select c1.ARNG_ID %s FROM curr c1 left join prev p1 on c1.ARNG_ID==p1.ARNG_ID "%(fileds)).show(1200)
```

    +-------+-----------------+--------+--------+--------+--------+--------+
    |ARNG_ID|NUM_OF_MTHS_PD_30| FIELD_1| FIELD_2| FIELD_3| FIELD_4| FIELD_5|
    +-------+-----------------+--------+--------+--------+--------+--------+
    | 004001|              0.0|3141.444|3124.197|6605.492|8139.267|3914.171|
    | 004002|              0.0|5897.519|1928.391|7675.341|8396.764|2357.492|
    | 004003|              1.0|4329.227|4564.128|8155.947|5364.827|6857.759|
    | 004004|              2.0|6434.676|2394.595| 9481.11|3354.332|1651.854|
    | 004005|              2.0|6630.857|1598.569|7658.347|1004.065|5822.402|
    +-------+-----------------+--------+--------+--------+--------+--------+
    only showing top 5 rows
    


## df5做filter处理, filter-Num >=1, 做Reset,形成df6, Num <1 形成 df8 ,df8走 Misc Exaction, 把null变成0值,形成df9


```python
df6 = df5.filter(df5.NUM_OF_MTHS_PD_30>=1)
df6.select('ARNG_ID').count()
df6.show(2) #867

#sql

# df5.createOrReplaceTempView('df5')
# df6 = spark.sql("select * from df5 where NUM_OF_MTHS_PD_30 >=1")
# df6.createOrReplaceTempView('df6')
# df6.show(1200)
```

    +-------+-----------------+--------+--------+--------+--------+--------+
    |ARNG_ID|NUM_OF_MTHS_PD_30| FIELD_1| FIELD_2| FIELD_3| FIELD_4| FIELD_5|
    +-------+-----------------+--------+--------+--------+--------+--------+
    | 004003|              1.0|4329.227|4564.128|8155.947|5364.827|6857.759|
    | 004004|              2.0|6434.676|2394.595| 9481.11|3354.332|1651.854|
    +-------+-----------------+--------+--------+--------+--------+--------+
    only showing top 2 rows
    



```python
from pyspark.sql.functions import isnull

# df7 = df5.subtract(df6) #这个更加简单
df7 = df5.filter((isnull("NUM_OF_MTHS_PD_30")) | (df5.NUM_OF_MTHS_PD_30 < 1))
# df7.show(400)
df7.select('ARNG_ID').count() #333
df8 = df7.fillna({"NUM_OF_MTHS_PD_30": 0})
df8.select('ARNG_ID').count()

#sql

# df7 =spark.sql("select * from df5 where NUM_OF_MTHS_PD_30 is null or NUM_OF_MTHS_PD_30 <1")
# # df7.show(400)
# df8 = df7.fillna({"NUM_OF_MTHS_PD_30": 0})
# df8.createOrReplaceTempView('df8')
# df8.show(400)
# df8.select('ARNG_ID').count()
```




    333



## df6与df8合并形成df9


```python
df9 = df6.union(df8)
df9.show()

#sql
# df9 = spark.sql("select * from df6 union select * from df8")
# df9.show()
# df9.select('ARNG_ID').count()
# df9.createOrReplaceTempView('df9')
```

    +-------+-----------------+--------+--------+--------+--------+--------+
    |ARNG_ID|NUM_OF_MTHS_PD_30| FIELD_1| FIELD_2| FIELD_3| FIELD_4| FIELD_5|
    +-------+-----------------+--------+--------+--------+--------+--------+
    | 004003|              1.0|4329.227|4564.128|8155.947|5364.827|6857.759|
    | 004004|              2.0|6434.676|2394.595| 9481.11|3354.332|1651.854|
    | 004005|              2.0|6630.857|1598.569|7658.347|1004.065|5822.402|
    | 004006|              6.0|1589.694|4613.918|9300.003|6964.704|4290.835|
    | 004008|              3.0|8536.384|8203.271|5190.138|7836.816|5566.414|
    | 004009|             12.0|3333.927|7885.698|9489.267| 4358.07|1337.423|
    | 004011|             11.0|9959.808|5829.583|9107.204| 3454.01|1628.629|
    | 004013|              5.0|8527.317|2553.808|5221.152|7567.694|4820.248|
    | 004014|              5.0|5286.703|1955.543|3218.909|4725.644|8440.842|
    | 004015|             10.0|6750.974|2929.389|5893.304|5844.209|2114.094|
    | 004016|              4.0|2355.397|9346.353|6164.897|7137.784|3008.927|
    | 004017|             10.0|6713.111|8459.452|1118.015|2736.671|6646.272|
    | 004018|              1.0| 8811.54|8259.064|2950.351|5981.983|9528.433|
    | 004019|             10.0|5708.108| 8203.23|3515.062|8245.311|6193.349|
    | 004020|             11.0|7670.525|2740.727|9246.192|3389.424|4569.728|
    | 004021|              3.0|7042.032| 3788.34|7890.763|8229.484| 9785.32|
    | 004022|              7.0|1576.219|6642.153|2436.278|7170.523|1419.198|
    | 004023|             11.0|7823.314| 7586.32|8173.526|8597.697|8725.358|
    | 004024|              7.0|6319.305|8690.981|2248.768|4019.903|3606.194|
    | 004026|             12.0|1279.075|1780.378|2140.166|8201.744|2060.012|
    +-------+-----------------+--------+--------+--------+--------+--------+
    only showing top 20 rows
    


## df9与df4做 leftjoin


```python
df10=df4.join(df9,['ARNG_ID'],'left')
df10.show(2)

#sql

# df9.columns[1:]
# fileds = ','.join(df9.columns[1:])
# df10 = spark.sql("select df4.ARNG_ID %s FROM df4 left join df9 on df4.ARNG_ID==df9.ARNG_ID "%(fileds))
# df10.show()
```

    +-------+-----------------+--------+--------+--------+--------+--------+
    |ARNG_ID|NUM_OF_MTHS_PD_30| FIELD_1| FIELD_2| FIELD_3| FIELD_4| FIELD_5|
    +-------+-----------------+--------+--------+--------+--------+--------+
    | 004294|              9.0|8318.182|5456.675|3705.907|  9535.3|4978.621|
    | 004902|              2.0|9203.703|2298.607|2226.882|7934.746|6098.114|
    +-------+-----------------+--------+--------+--------+--------+--------+
    only showing top 2 rows
    


# 导出成avro文件


```python
df10.write.mode('overwrite').format('avro').save('./data/dataframejob')
```

# 检测,读取文件


```python
df = spark.read.format('avro').load('./data/dataframejob/*')
df.show(2)
# df.describe().show(2)
```

    +-------+-----------------+--------+--------+--------+--------+--------+
    |ARNG_ID|NUM_OF_MTHS_PD_30| FIELD_1| FIELD_2| FIELD_3| FIELD_4| FIELD_5|
    +-------+-----------------+--------+--------+--------+--------+--------+
    | 004180|              3.0|4751.734|1117.979|5763.531| 3574.35|1260.792|
    | 004203|              6.0|8290.917|7907.448|6677.156|7582.947|9187.084|
    +-------+-----------------+--------+--------+--------+--------+--------+
    only showing top 2 rows
    

