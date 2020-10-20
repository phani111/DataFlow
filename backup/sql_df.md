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
df6.show(2) #867
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
    +-------+-----------------+--------+--------+--------+--------+--------+
    only showing top 2 rows
    


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
df5.show(2)

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
    +-------+-----------------+--------+--------+--------+--------+--------+
    only showing top 2 rows
    


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
df9.show(2)

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
    +-------+-----------------+--------+--------+--------+--------+--------+
    only showing top 2 rows
    


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
    

