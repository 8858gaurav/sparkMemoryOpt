# overhead(non-heap) memory used for VM related = Max(10 % of spark.executor.memory, 384 MB) = default
# we can change this by spark.executor.memoryOverhead
# onHeap Memory = spark.executor.memory
# Usable onheap memory on per executor = (X - 300)MB
# 300 MB = reserved memory for spark engine on every executors.
# (X-300) MB * .6 = Unified Area (Storage + execution, ideally it's 50-50 %)
# Storage Memory used for : Cache, & persist
# Execution Memory used for : shuffle, sort, join, map, agg, etc.
# (X-300) MB * .4 = User Memory, user defined Data Structures, all rdd operations will be done here. Hash table will be created here.
# .6 or .4 boundaries can also be changed, by spark.memory.fraction = .6 (by default)
# 0.5 boundaries b/w storage, & execution memory can also be changed, spark.memory.storageFraction = .5 (default)

# hash table will be created outside your JVM(means onheap Memory), so it take some memory from the off-heap.
# on heap Memory managed by JVM
# off Heap Memory will be managed by your OS.

# run this file by using spark-submit command: 
# spark-submit --deploy-mode cluster --master yarn --num-executors 1 --executor-cores 4 --executor-memory 8G --conf spark.dynamicAllocation.enabled=false scripts3.py

# Required executor memory (8192), overhead (819 MB),
# and PySpark memory (0 MB) is above the max threshold (8192 MB) of this cluster! 
# Please check the values of 'yarn.scheduler.maximum-allocation-mb' and/or 'yarn.nodemanager.resource.memory-mb'.

# in enviroment tab, it's yarn.scheduler.maximum-allocation-mb: 8192 MB
# size of machine: yarn.nodemanager.resource.memory-mb : 51 GB

# if we are not requesting, then offHeap, and Pyspark Memory will become 0 for both.
# if we want to use heavy -2 python library, then it will initiates a python worker, then it use Pyspark Memory, other wise spark with java or spark with scala, this is 0.
# how to request - spark.executor.pyspark.memory

# off Heap memory (Outside the JVM): No garbage collection required, i.e it's faster than onheap memory, in terms of gc times.
# onheap memory will takes take time to clean up the object, gc takes time here.

import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time, os
username = getpass.getuser()
print(username)


if __name__ == '__main__':
    print("creating spark session")

    spark = SparkSession \
           .builder \
           .appName("MemoryOpt") \
           .config("spark.shuffle.useOldFetchProtocol","true") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()

# now go to executor tab, you will see
# executorId           Storage Memory      cores
# driver               0.0 B / 397.5 MiB     0
# 1                    0.0 B / 366.3 MiB     1
# 2                    0.0 B / 366.3 MiB     1

# 384 Mb reserved for overhead
# 300 Mb reserved for spark engine on each executors
# each executors left with 1000 - 300 = 700MB
# 60 % of 700 Mb = 420 MB (Unified area)
# 40 % of 700 MB = 280 MB (User Memory)

# here in the executor tab, it's showing, storage area = unified area (366.3 MB): combination of storag + execution memory.

import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time, os
username = getpass.getuser()
print(username)


if __name__ == '__main__':
    print("creating spark session")

    spark = SparkSession \
           .builder \
           .appName("MemoryOpt") \
           .config("spark.executor.memory", "2G") \
           .config("spark.executor.instances", "1") \
           .config("spark.dynamicAllocation.enabled", False) \
           .config("spark.shuffle.useOldFetchProtocol","true") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
    
# now go to executor tab, you will see
# executorId           Storage Memory      cores
# driver               0.0 B / 397.5 MiB     0
# 1                    0.0 B / 912.3 MiB     1

# 384 Mb reserved for overhead
# 300 Mb reserved for spark engine on each executors
# executors left with 2G - 300 = 1.7GB
# 60 % of 1.7 Gb =  1048.8MB (Unified area)
# 40 % of 1.7 GB =  700MB (User Memory)

# here in the executor tab, it's showing, storage area = unified area (912.3MB): combination of storag + execution memory.

    orders_schema = "order_id long , order_date string, customer_id long,order_status string"
    orders_df = spark.read \
    .format("csv") \
    .schema(orders_schema) \
    .load("/public/trendytech/orders/orders_1gb.csv")
    new_cached_df = orders_df.cache()
    # 1 by 1, it will cache all the partitions not just once, cuz we called an action, not show, which just store only 1.
    new_cached_df.count()
# under the storage tabe it will shows us a 286.7MB
# ID     RDD Name        Storage Level                                  Cached Partitions   Size in Memory
# 1.      A              Disk Memory Deserialized 1x Replicated           9                    286.7Mb

# the same 286.6 MB, you'll see under your eexecutors tab:
# executorId           Storage Memory          cores
# driver               44.2 KiB / 397.5 MiB      0
# 1                    286.7 MiB / 912.3 MiB     1

# 286.7 Mb used out of 912.3 MB, storage area is being used. 

    customer_schema = " customerid long , customer_fname string , customer_lname string , user_name string,password string , address string, city string, state string, pincode long "
    customers_df = spark.read \
    .format("csv") \
    .schema(customer_schema) \
    .load("/public/trendytech/retail_db/customers")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    orders_df.join(customers_df, orders_df.customer_id == customers_df.customerid, "inner").write.format("noop").mode("overwrite").save()

# Now under summary metrics for 200 completed task: 
# Metric	                 Min	         25th percentile	   Median	      75th percentile	  Max
# Duration	                45.0ms	          59.0 ms	           65.0 ms	      73.0 ms	          0.2 s
# GC Time	                0.0 ms	          0.0 ms	           0.0 ms	      0.0 ms	         74.0 ms
# ShuffleReadSize/Records   1.2 MiB/91544	  1.8 MiB/116677	   2.1MiB/128307  2.6 MiB/140691	 3.5 MiB / 179714
# Peak Execution Memory	    68.1MiB	          68.1MiB	           68.1MiB	      72.1MiB	         72.1 MiB

# if this 68.1 MB (the memory which is required to work on partitions) > unified area (912.3Mb), then it would result
# it as a spill memory / spill disk. the sieze of each task is 1.5 mb to 3.3 mb of data, but for processing each 200 task
# it require execution memory from 68MB to 72MB. 

    spark.conf.set("spark.sql.shuffle.partitions", "2")
    # from 200 to 2
    customer_schema = " customerid long , customer_fname string , customer_lname string , user_name string,password string , address string, city string, state string, pincode long "
    customers_df = spark.read \
    .format("csv") \
    .schema(customer_schema) \
    .load("/public/trendytech/retail_db/customers")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    orders_df.join(customers_df, orders_df.customer_id == customers_df.customerid, "inner").write.format("noop").mode("overwrite").save()
# 2 partitions, we'll have 2 task, each task were handling around 196.9 Mb, andother one will be 200.7 MB
# the execution memory required to work on this task is: 480.3 MB
# 2 task will run 1 by 1

# Now under summary metrics for 2 completed task: 
# Metric	                 Min	         25th percentile	   Median	      75th percentile	  Max
# Duration	                45.0ms	          59.0 ms	           65.0 ms	        73.0 ms	          0.2 s
# GC Time	                0.0 ms	          0.0 ms	           0.0 ms	        0.0 ms	         74.0 ms
# ShuffleReadSize/Records   196.9MiB/91544	  196.9MB/116677	  196.9MiB/128307   196.9MiB/140691	 200.7MiB / 179714
# Peak Execution Memory	    480.3MiB	       480.3MiB	           480.3MiB	        480.3MiB	      480.3MiB
# Spill (memory)            960 MiB           960 MiB              960 MiB          960 MiB           960 MiB
# Spill (disk)               15.4MB            15.4MB              15.4MB           15.4MB            15.4MB

    spark.conf.set("spark.sql.shuffle.partitions", "4")
    # from 200 to 2
    customer_schema = " customerid long , customer_fname string , customer_lname string , user_name string,password string , address string, city string, state string, pincode long "
    customers_df = spark.read \
    .format("csv") \
    .schema(customer_schema) \
    .load("/public/trendytech/retail_db/customers")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    orders_df.join(customers_df, orders_df.customer_id == customers_df.customerid, "inner").write.format("noop").mode("overwrite").save()
# 2 partitions, we'll have 2 task, each task were handling around 196.9 Mb, andother one will be 200.7 MB
# the execution memory required to work on this task is: 480.3 MB
# 4 task will run 1 by 1

# Now under summary metrics for 4 completed task: 
# Metric	                 Min	         25th percentile	   Median	      75th percentile	  Max
# Duration	                45.0ms	          59.0 ms	           65.0 ms	        73.0 ms	          0.2 s
# GC Time	                0.0 ms	          0.0 ms	           0.0 ms	        0.0 ms	         74.0 ms
# ShuffleReadSize/Records   101.6MiB/91544	  103MiB/116677	       103.7MiB/128307  105MiB/140691	 105MiB / 179714
# Peak Execution Memory	    480.1MiB	       480.1MiB	           480.1MiB	        480.1MiB	      480.1MiB
# Spill (memory)            320 MiB           320 MiB              320 MiB          320 MiB           320 MiB
# Spill (disk)               4.7MB            4.7MB                 4.7MB            4.7MB             4.7MB

# whenever we are handling a job, we've given 4 cpu cores to the executors, say our unified area (storage + execution) = 2.4 Gb
# 2.4GB is divided across 4 cores, each cores will get 600 MB, if we cached our data, then again this No will become less
# buf if we have not cached anaything, then we can't use more than 600 MB for execution memory w.r.to each task.
# say we are processing the data for 300 MB, when we are doing sorting & all, then 800 mb execution memory is needed for this (say)
# then ofcourse, it'll spill to the disk. so give each core descent amount of memory, so that it will spill less.
# cause - spill to disk make our task slow

import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time, os
username = getpass.getuser()
print(username)


if __name__ == '__main__':
    print("creating spark session")

    spark = SparkSession \
           .builder \
           .appName("MemoryOpt") \
           .config("spark.executor.memory", "2G") \
           .config("spark.executor.instances", "1") \
           .config("spark.dynamicAllocation.enabled", False) \
           .config("spark.memory.offHeap.size", "2G") \
           .config("spark.shuffle.useOldFetchProtocol","true") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
    
# now go to executor tab, you will see
# executorId           Storage Memory      cores
# driver               0.0 B / 2.4 GiB     0
# 1                    0.0 B / 2.9 GiB     1

# earlier it was 912.3 MB, now it's 2.9 GB

# 384 Mb reserved for overhead
# 300 Mb reserved for spark engine on each executors
# executors left with 2G - 300 = 1.7GB
# 60 % of 2 + 1.7 Gb =  3.0GB (Unified area)
# 40 % of 1.7 GB =  700 MB (User Memory)

# here in the executor tab, it's showing, storage area = unified area (912.3MB): combination of storag + execution memory.


# run the same thing again with off heap memory
    orders_schema = "order_id long , order_date string, customer_id long,order_status string"
    orders_df = spark.read \
    .format("csv") \
    .schema(orders_schema) \
    .load("/public/trendytech/orders/orders_1gb.csv")
    new_cached_df = orders_df.cache()

# under the storage tabe it will shows us a 286.7MB
# ID     RDD Name        Storage Level                                  Cached Partitions   Size in Memory
# 1.      A              Disk Memory Deserialized 1x Replicated           9                    286.7Mb

# now go to executor tab, you will see
# executorId           Storage Memory      cores
# driver               0.0 B / 2.4 GiB        0
# 1                    286.6 MB / 2.9 GiB     1

# 2.9 GiB = storage + execution memory 

    customer_schema = " customerid long , customer_fname string , customer_lname string , user_name string,password string , address string, city string, state string, pincode long "
    customers_df = spark.read \
    .format("csv") \
    .schema(customer_schema) \
    .load("/public/trendytech/retail_db/customers")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    orders_df.join(customers_df, orders_df.customer_id == customers_df.customerid, "inner").write.format("noop").mode("overwrite").save()

# to makesure gc & all doesn't take so much of time, we can mix on-heap & off-heap memory
# so that we'll get a mixed performances.

# Now under summary metrics for 2 completed task: 
# Metric	                 Min	         25th percentile	   Median	      75th percentile	  Max
# Duration	                45.0ms	          59.0 ms	           65.0 ms	        73.0 ms	          0.2 s
# GC Time	                0.0 ms	          0.0 ms	           0.0 ms	        0.0 ms	         74.0 ms
# ShuffleReadSize/Records   196.9MiB/91544	  196.9MiB/116677	   200.7MiB/128307  200.7MiB/140691	 200.7MiB / 179714
# Peak Execution Memory	    480.3MiB	       480.3MiB	           480.3MiB	        480.3MiB	      480.3MiB
# Spill (memory)            960 MiB           960 MiB              960 MiB          960 MiB           960 MiB
# Spill (disk)               15.4MB            15.4MB              15.4MB            15.4MB            15.4MB

# # run the same thing again with off heap memory, & enable it 
# run the same thing again with off heap memory, spill to disk will not be there.

import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time, os
username = getpass.getuser()
print(username)


if __name__ == '__main__':
    print("creating spark session")

    spark = SparkSession \
           .builder \
           .appName("MemoryOpt") \
           .config("spark.executor.memory", "2G") \
           .config("spark.executor.instances", "1") \
           .config("spark.dynamicAllocation.enabled", False) \
           .config("spark.memory.offHeap.size", "2G") \
           .config("spark.memory.offHeap.enabled", True) \
           .config("spark.shuffle.useOldFetchProtocol","true") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
    
    orders_schema = "order_id long , order_date string, customer_id long,order_status string"
    orders_df = spark.read \
    .format("csv") \
    .schema(orders_schema) \
    .load("/public/trendytech/orders/orders_1gb.csv")
    new_cached_df = orders_df.cache()
    customer_schema = " customerid long , customer_fname string , customer_lname string , user_name string,password string , address string, city string, state string, pincode long "
    customers_df = spark.read \
    .format("csv") \
    .schema(customer_schema) \
    .load("/public/trendytech/retail_db/customers")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    orders_df.join(customers_df, orders_df.customer_id == customers_df.customerid, "inner").write.format("noop").mode("overwrite").save()

# to makesure gc & all doesn't take so much of time, we can mix on-heap & off-heap memory
# so that we'll get a mixed performances.

# under the storage tabe it will shows us a 286.7MB
# ID     RDD Name        Storage Level                                  Cached Partitions   Size in Memory
# 1.      A              Disk Memory Deserialized 1x Replicated           9                    286.7Mb

# now go to executor tab, you will see
# executorId           Storage Memory      cores 
# driver               0.0 B / 2.4 GiB        0
# 1                    286.6 MB / 2.9 GiB     1

# 2.9 GiB = storage + execution memory 

# Now under summary metrics for 2 completed task: 
# Metric	                 Min	         25th percentile	   Median	      75th percentile	  Max
# Duration	                45.0ms	          59.0 ms	           65.0 ms	        73.0 ms	          0.2 s
# GC Time	                0.0 ms	          0.0 ms	           0.0 ms	        0.0 ms	         74.0 ms
# ShuffleReadSize/Records   196.9MiB/91544	  196.9MiB/116677	   200.7MiB/128307  200.7MiB/140691	 200.7MiB / 179714
# Peak Execution Memory	    1.6GB	          1.6GB	               1.6GB	        1.6GB	         1.6GB
