## Querying the data stream

First, what is a data stream? In computer science, a stream is a sequence of unbounded data elements made available over a span of time. You can think of a stream as items on a conveyor belt being processed one at a time, in a continuous flow, rather than in large batches, or, to continue the warehouse analogy, a delivery truck periodically dropping off a large load of items all at once. Streams are processed differently than batch data—most normal system functions can’t operate on streams, because they have potentially unlimited data.

You can run queries on the data that’s in the stream. It wouldn’t be the same as querying all of the data in the data lake. You would also need a shorter time interval to query, such as 1 hour or 24 hours. You could ask questions like, how many users signed up in the past hour? How many financial transactions occurred in the past 2 hours?

Streams are not meant to hold massive amounts of data; they’re designed to hold data only for a short time, typically from 5 minutes to 24 hours. A streaming platform has three key capabilities. First, it can publish and subscribe to streams of data, like traditional message queues or enterprise messaging systems can. Second, it stores data streams in a reliable, fault-tolerant way. Finally, it can process streams as they arrive.

## Apache Kafka

Kafka is an open source stream-processing software platform that started as a distributed message queue for stream data ingestion. Over time, it developed into a full-fledged streaming platform by including processing capabilities as well. Written in Scala and Java, Kafka offers a unified, high-throughput, low-latency platform for handling real-time data feeds.

Apache Kafka is used for two types of applications:

Real-time streaming data pipelines that reliably get data between systems or applications

Real-time streaming applications that transform or react to the streams of data

Tools to use for stream processing
The following tools can be used to process streaming data:

## Spark Streaming

Although Kafka is often referred to as the “message bus,” Spark can provide a streaming engine in conjunction with the Kafka operation. The same data is going through Kafka; however, you are using the Spark engine to enable processing of live data streams. This is a streaming module of the Apache Spark ecosystem (Figure 6-2) known for scalable, high-throughput, and fault-tolerant stream data processing.

![Screen Shot 2021-08-12 at 4.20.18 PM](/Users/ling/Desktop/Screen Shot 2021-08-12 at 4.20.18 PM.png)
With Spark 2.0, Spark Streaming (now known as Structured Streaming) has evolved significantly in terms of capabilities and simplicity, enabling you to write code for streaming applications the same way you write batch jobs. Internally, it uses the same core APIs as Spark Batch with all of the optimizations intact. It supports Java, Scala, and Python. Structured Streaming can read data from HDFS, Flume, Kafka, Twitter, and ZeroMQ. You can also define your own custom data sources, which could be storage such as the Amazon S3 object store or a streaming database like Druid.

## Apache Flink

Apache Flink is a scalable, high-throughput, and fault-tolerant stream-processing framework popular for its very low-latency processing capabilities. Flink was built by developers from the Apache Software Foundation, most of whom are employed by data Artisans (recently acquired by Alibaba).

The core of Apache Flink is a distributed streaming dataflow engine written in Java and Scala. Flink executes operators in a continuous flow, allowing multiple jobs to be processed in parallel as new data arrives.

There are several key differences between Spark Streaming and Flink. Spark is a microbatch technology that allows latency to be counted in seconds. Alternatively, Flink offers event-by-event stream processing, so latency can be measured in milliseconds. Flink is usually used for business scenarios where very low end-to-end latency is important, such as real-time fraud detection. Spark is usually used for streaming ingestion and streaming processing, for which low latency is unimportant.

A point in favor of using Spark most of the time is its popularity. Data engineers are familiar with Spark for batch and ETL use cases, so they end up using Spark for streaming as well for familiarity and ease of use—unless there is a strong need for very low-latency stream processing.

## Apache Druid

This is an open source, high-performance, and column-oriented distributed database built for event-driven data that was designed for real-time, subsecond OLAP queries on large datasets. Druid is currently in incubation (see the following note) at the Apache Foundation. Druid has two query languages: a SQL dialect and a JSON-over-HTTP API. Druid is extremely powerful when it comes to running fast interactive analytics on real-time and historical information.

####  Spark Scala Shell

```
./bin/spark-shell
```
To exit the Scala Spark shell, type :quit or :q.

* :history - displays what was entered during the previous Spark shell session as well as the current session. 
* :load - loads and executes the code in the provided file. 
* :reset - resets the shell to a clean state.
* :silent - stop the shell from displaying the default output after evaluating an expression. To re-enable the output, simply type :silent again.
* :quit - quit the shell 
* :type - displays the type of a variable

```
val ages = Array(20, 50, 35, 41)
ages.foreach(println)
```

```
scala> spark.conf.getAll.foreach(println)
(spark.driver.host,172.133.8.170)
(spark.driver.port,50778)
(spark.repl.class.uri,spark://172.133.8.170:50778/classes)
(spark.jars,)
(spark.repl.class.outputDir,/private/var/folders/59/0pbnsns93t5cgn8b5n2kpxt00000gn/T/spark-6b33becf-5b7c-4e22-9e79-f5d0d28406b4/repl-ccbb0304-5a3b-462f-b252-851bd5d4e091)
(spark.app.name,Spark shell)
(spark.ui.showConsoleProgress,true)
(spark.executor.id,driver)
(spark.submit.deployMode,client)
(spark.master,local[*])
(spark.home,/usr/local/Cellar/apache-spark/2.3.1/libexec)
(spark.sql.catalogImplementation,hive)
(spark.app.id,local-1543337107085)
```

#### Spark Python Shell

```
./bin/pyspark
```

To exit the Python Spark shell, press Ctrl+D.

## Apache Spark with Scala - Learn Spark from a Big Data Guru

####  IntelliJ IDEA Tutorial
1. Download IntelliJ IDEA Community Edition

IntelliJ IDEA Community Edition is an open-source version of IntelliJ IDEA, a premier IDE for Java, Scala and other JVM-based programming languages. You can download it from the official website.

2. Install the Scala plugin

Before you create or open a Scala project, you need to install the Scala plugin. For that, use the Configure → Plugins → Browse JetBrains Plugins from the Welcome Screen, or Preferences (Settings) → Plugins.

3. Setup the JDK

From the welcome screen, go to Configure → Project defaults → Project structure and add the JDK.

4. Creating a project

The easiest way to create a project is to use the Project Wizard. To use it, Click Create New Project on the Welcome Screen, then select Scala, and finally SBT Project.

Click Next to specify project name and location. Once you've entered this information, IntelliJ IDEA will create an empty project containing a build.sbt file.

5. Creating a Scala worksheet

Simply use the Create New action from context menu or press Ctrl+N on a Scala source root.

To evaluate worksheets, use the corresponding toolbar icon, or press Alt+Ctrl+W (Alt+Cmd+W on OS X)

6. Creating a Scala class

Much akin to worksheets, Scala classes are created via context menu action Create New, or by using the Ctrl+N shortcut..

7. Opening an SBT project

To open an SBT project in IntelliJ IDEA, go to the Welcome Screen, click Import Project, and select SBT build file that you wish to open. IntelliJ IDEA will then create a new project and import the selected file to it.

If you are unable to add Scala class, you should properly specified the Java SDK and add Scala plugin. And also, right click on your project, "Add Framework support" and choose Scala framework, then by right click on the packages you can create Scala Class.

The project has the following structure:

.idea: These are IntelliJ configuration files.

project: Files used during compilation. For example, build.properties allows you to change the SBT version used when compiling your project.

src: Source Code. Most of your code should go into the main directory. The test folder should be reserved for test scripts.

target: When you compile your project it will go here.

build.sbt: The SBT configuration file. We’ll show you how to use this file to import third party libraries and documentation.

8. Add spark libraries
Next step is to add the libraries to the project. For this purpose update the content of the build.sbt file simply by copying and pasting the code below. 

```
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0"
)
```

#### Chapter 2 RDD (Resilient Distributed Datasets)

###### RDD Basics in Apache Spark
In spark, all work is expressed as either creating new RDDs or transforming existing RDDs, or calling operations on RDDs to compute a result.
Apply Transformations - apply some functions to the data in RDD to create a new RDD (e.g.,filter, ..)
Launch Actions - compute a result based on an RDD (e.g., first, ...)

###### Creating RDDs
Take an existing collection and pass it to SparkContext's parallelize method. All the elements in the collection will then be copied to form a distributed dataset that can be operated on in parallel.
Load RDDs from external storage by calling textFile method on SparkContent (e.g., txt, Amazon S3, HDFS, JDBC, Cassandra, Elastic Search, etc)

###### Transformations 
Transformations are operations on RDDs which will return a new RDD (e.g., filter and map)
filter() takes in a function returns an RDD formed by selecting those elements which pass the filter function.
```val cleanLines = Lines.filter(line => !line.isEmpty)```
map() takes in a function and passes each element in the input RDD through the function, with the result of the function being the new value of each element in the resulting RDD. The return type of the map function is not necessary the same as the input type.
object Airportsolution...
local[2]: 2 cores
local[*]: all available cores
local: 1 core
go to declaration to check comma delimitation using regex
```
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("airports").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val airports = sc.textFile("in/airports.text")
    val airportsInUSA = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(3) == "\"United States\"")

    val airportsNameAndCityNames = airportsInUSA.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + ", " + splits(2)
    })
    airportsNameAndCityNames.saveAsTextFile("out/airports_in_usa.text")
  }
```

###### flatMap transformation
map: 1 to 1 relationship
flatMap: 1 to many relationship

```
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")
    val words = lines.flatMap(line => line.split(" "))

    val wordCounts = words.countByValue()
    for ((word, count) <- wordCounts) println(word + " : " + count)
  }
```

###### Set operations
sample: the sample operation will create a random sample from an RDD.
distinct: the distinct operation will return the district rows from the input RDD.
other popular set operations which are performed on two RDDs and produce one resulting RDD: union, intersection, subtract, Cartesian product
union: union operation returns an RDD consisting of the data from both input RDDs. Contain duplicates in the input RDDs.
intersection: intersection operation returns the common elements which appear in both RDDS. Remove duplicates from single RDD before returning the results.
substract: subtract operation returns an RDD that only contains element present int he first RDD and not the second RDD.
cartesian: cartesian operation returns all possible pairs of a and b where a is in the source RDD and b is in the other RDD. 

```
   def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("unionLogs").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val julyFirstLogs = sc.textFile("in/nasa_19950701.tsv")
    val augustFirstLogs = sc.textFile("in/nasa_19950801.tsv")

    val aggregatedLogLines = julyFirstLogs.union(augustFirstLogs)
    //val intersection = julyFirstHosts.intersection(augustFirstHosts)

    val cleanLogLines = aggregatedLogLines.filter(line => isNotHeader(line))

    val sample = cleanLogLines.sample(withReplacement = true, fraction = 0.1)

    sample.saveAsTextFile("out/sample_nasa_logs.csv")
  }

  def isNotHeader(line: String): Boolean = !(line.startsWith("host") && line.contains("bytes"))
```

###### Actions
Action returns final value to the driver program or persist data to an external storage system. Action will force the evaluation to the transformations required for the RDD they were called on.
collect - retrieves the entire RDD, which mush fit in memory on a single machine, should not be used on large datasets, but largely used in unit test.

```
object CollectExample {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("collect").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
    val wordRdd = sc.parallelize(inputWords)

    val words = wordRdd.collect()

    for (word <- words) println(word)
  }
```

###### count and countbyValue
If you want to count how many row in an RDD, used count function
If you want to count unique values in an RDD, used count by value. CountbyValue will look at unique values in the each row of the RDD and return a map of each unique value to its count.
```
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("count").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
    val wordRdd = sc.parallelize(inputWords)
    println("Count: " + wordRdd.count())

    val wordCountByValue = wordRdd.countByValue()
    println("CountByValue:")

    for ((word, count) <- wordCountByValue) println(word + " : " + count)
  }
```

###### take
Take action takes n elements from an RDD. Might be biased.
```
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("take").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
    val wordRdd = sc.parallelize(inputWords)

    val words = wordRdd.take(3)
    for (word <- words) println(word)
  }
```

###### saveAsTextFile
saveAsTextFile can be used to write data out to a distributed storage system such as HDFS or S3 or local file system

###### reduce
Reduce take a function that operates on two elements of the type in the input RDD and returns a new element of the same type. 

```
val product = intergerRdd.reduce(x,y) => x * y

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("reduce").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputIntegers = List(1, 2, 3, 4, 5)
    val integerRdd = sc.parallelize(inputIntegers)

    val product = integerRdd.reduce((x, y) => x * y)
    println("product is :" + product)
  }
```

###### Important facts
RDDs are distributed, partition across clusters. 
RDDs are immutable
RDDs are resilient, can be recreated at any time

###### Summary of RDD Operation
Transformation - e.g., map and filter - return RDD
Actions - e.g., counts and collect - return other types
Transformations on RDDs are lazily. 
Tranformations return RDDs, whereas actions return some other data type.

###### Caching and Persistence - performance optimization
Action is called on the RDD, expensive, especially for some iterative algorithms
Reuse an RDD in multiple actions, use persist() method on the RDD, the first time it is computed in an action, it will be key in the mutiple nodes in memory
RDD.cache() = RDD.persist(MEMORY_ONLY)
RDD.cache() = RDD.persist(MEMORY_ONLY_SER)
Memory usage and CPU efficiency
```
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("reduce").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputIntegers = List(1, 2, 3, 4, 5)
    val integerRdd = sc.parallelize(inputIntegers)

    integerRdd.persist(StorageLevel.MEMORY_ONLY)

    integerRdd.reduce((x, y) => x * y)
    integerRdd.count()
```
* MEMORY_ONLY
* MEMORY_AND_DISK
* MEMORY_ONLY_SER
* MEMORY_AND_DISK_SER
* DISK_ONLY

If the RDD can fit comfortably with eat default storage level, MEMORY_ONLY is the ideal option. This is the most CPU-efficient option, allowing operations on the RDDs to run as fast as possible. If not, try using MEMORY_ONLY_SER to make the objects much more space efficient, but still reasonably fast to access. Caching unnecessary data can cause spark to evict useful data and leas to longer recomputation time.

####  Chapter 3 Spark Architecture and Components
###### Spark Architecture
master nodes - Driver Program (SparkContext) - control and coordinate jobs - convert jobs to tasks
worker - Distributed Worker Node (Executor, Cache, Task) - launch and run tasks and send back results to master nodes
All components run in the same process on the local machine.

###### Spark Components
* Spark core - API Execution model, the shuffle, caching
* Spark SQL - SQL-like interface for working with structured data which is build on top of Spark Core. Provides an SQL-like interface for working with structured data.
* Spark Streaming - real time for manipulating data streams that closely match that closely match the Spark Core's RDD API. Enable powerful interactive and analytical applications across both streaming and historical data while inheriting Spark's ease of use and fault tolerance characteristics.
* Spark MLlib - deliver both high-quality algorithms and blazing speed.
* GraphX - interactively create, transform and reason about graph structured data at scale. Introducing a Graph abstraction: a directed multigraph with properties attached to each vertex and edge.

####  Chapter 4 Pair RDD in Apache Spark

###### Introduction to Pair RDD in Spark
Spark provides a data structure called Pair RDD instead of regular RDDs, which makes working with key value pairs of data more simpler and more efficient. A pair RDDs is a particular type of RDD that can store key-value pairs. Pair RDDs are useful building blocks in many spark programs.

###### Create Pair RDDs in Spark

1. Return pair RDDDs from a list of key value data structure called tuple.
```    
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("create").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val tuple = List(("Lily", 23), ("Jack", 29), ("Mary", 29), ("James", 8))
    val pairRDD = sc.parallelize(tuple)

    pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_tuple_list")
  }
```

2. Turn a regular RDD into a pair RDD.
```
 def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("create").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val inputStrings = List("Lily 23", "Jack 29", "Mary 29", "James 8")
    val regularRDDs = sc.parallelize(inputStrings)

    val pairRDD = regularRDDs.map(s => (s.split(" ")(0), s.split(" ")(1)))
    pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_regular_rdd")
  }
```

###### Filter and MapValue Transformations on Pair RDD

Pair RDDs are allowed to use all the transformations available to regular RDDs, and thus support the same functions as regular RDDs.
The filter transformation that can be applied to a regular RDD can also be applied to a Pair RDD.

```
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("airports").setMaster("local")
    val sc = new SparkContext(conf)

    val airportsRDD = sc.textFile("in/airports.text")

    val airportPairRDD = airportsRDD.map(line => (line.split(Utils.COMMA_DELIMITER)(1),
      line.split(Utils.COMMA_DELIMITER)(3)))
    val airportsNotInUSA = airportPairRDD.filter(keyValue => keyValue._2 != "\"United States\"")

    airportsNotInUSA.saveAsTextFile("out/airports_not_in_usa_pair_rdd.text")
  }
```  

The map transformation also works for Pair RDDs. It can be used to convert an RDD to another one. Spark provides the mapValues function. The mapValues function will be applied to each key value pair and will convert the values based on mapValues function, but it will not change the keys.

```
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("airports").setMaster("local")
    val sc = new SparkContext(conf)

    val airportsRDD = sc.textFile("in/airports.text")

    val airportPairRDD = airportsRDD.map((line: String) => (line.split(Utils.COMMA_DELIMITER)(1),
      line.split(Utils.COMMA_DELIMITER)(3)))

    val upperCase = airportPairRDD.mapValues(countryName => countryName.toUpperCase)

    upperCase.saveAsTextFile("out/airports_uppercase.text")
  }
```

###### Reduce by Key Aggregation in Apache Spark

reduce actions on regular RDDs, and reduceByKey to pair RDD.
reduceByKey runs several parallel reduce options, one for echo key in the dataset, where each option combines values that have the same key.
reduceByKey options is not implemented as an action that returns a blue to the driver program, instead, ti returns a new RDD consisting of each key and the reduced value fo that key.

```
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")
    val wordRdd = lines.flatMap(line => line.split(" "))
    val wordPairRdd = wordRdd.map(word => (word, 1))

    val wordCounts = wordPairRdd.reduceByKey((x, y) => x + y)
    for ((word, count) <- wordCounts.collect()) println(word + " : " + count)
  }
```

House price solutions

```
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("avgHousePrice").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/RealEstate.csv")
    val cleanedLines = lines.filter(line => !line.contains("Bedrooms"))

    val housePricePairRdd = cleanedLines.map(line => (line.split(",")(3), (1, line.split(",")(2).toDouble)))

    val housePriceTotal = housePricePairRdd.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    println("housePriceTotal: ")
    for ((bedroom, total) <- housePriceTotal.collect()) println(bedroom + " : " + total)

    val housePriceAvg = housePriceTotal.mapValues(avgCount => avgCount._2 / avgCount._1)
    println("housePriceAvg: ")
    for ((bedroom, avg) <- housePriceAvg.collect()) println(bedroom + " : " + avg)
  }
```

###### GroupBy Key Transformation in Spark

groupByKey will group the data using the key in Pair RDD.
```
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("GroupByKeyVsReduceByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val words = List("one", "two", "two", "three", "three", "three")
    val wordsPairRdd = sc.parallelize(words).map(word => (word, 1))

    val wordCountsWithReduceByKey = wordsPairRdd.reduceByKey((x, y) => x + y).collect()
    println("wordCountsWithReduceByKey: " + wordCountsWithReduceByKey.toList)

    val wordCountsWithGroupByKey = wordsPairRdd.groupByKey().mapValues(intIterable => intIterable.size).collect()
    println("wordCountsWithGroupByKey: " + wordCountsWithGroupByKey.toList)
  }
```
groupByKey + [reduce, map, mapValues] can be replaced by one of the per-key aggregation functions such as reduceByKey.

```
 def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("GroupByKeyVsReduceByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val words = List("one", "two", "two", "three", "three", "three")
    val wordsPairRdd = sc.parallelize(words).map(word => (word, 1))

    val wordCountsWithReduceByKey = wordsPairRdd.reduceByKey((x, y) => x + y).collect()
    println("wordCountsWithReduceByKey: " + wordCountsWithReduceByKey.toList)

    val wordCountsWithGroupByKey = wordsPairRdd.groupByKey().mapValues(intIterable => intIterable.size).collect()
    println("wordCountsWithGroupByKey: " + wordCountsWithGroupByKey.toList)
  }
```

###### SortByKey Transformation in Spark

Once we have sorted Pair RDD, any subsequent call on the sorted Pair RDD to collect or save will return us ordered data.
```
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("averageHousePriceSolution").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/RealEstate.csv")
    val cleanedLines = lines.filter(line => !line.contains("Bedrooms"))
    val housePricePairRdd = cleanedLines.map(
      line => (line.split(",")(3).toInt, AvgCount(1, line.split(",")(2).toDouble)))

    val housePriceTotal = housePricePairRdd.reduceByKey((x, y) => AvgCount(x.count + y.count, x.total + y.total))

    val housePriceAvg = housePriceTotal.mapValues(avgCount => avgCount.total / avgCount.count)

    val sortedHousePriceAvg = housePriceAvg.sortByKey()

    for ((bedrooms, avgPrice) <- sortedHousePriceAvg.collect()) println(bedrooms + " : " + avgPrice)
  }
```

Word count sortByKey solution - Flip key and value, sortByKey, and then flip back
```
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")
    val wordRdd = lines.flatMap(line => line.split(" "))

    val wordPairRdd = wordRdd.map(word => (word, 1))
    val wordToCountPairs = wordPairRdd.reduceByKey((x, y) => x + y)

    val countToWordParis = wordToCountPairs.map(wordToCount => (wordToCount._2, wordToCount._1))

    val sortedCountToWordParis = countToWordParis.sortByKey(ascending = false)

    val sortedWordToCountPairs = sortedCountToWordParis.map(countToWord => (countToWord._2, countToWord._1))

    for ((word, count) <- sortedWordToCountPairs.collect()) println(word + " : " + count)
```

###### Data Partitioning in Spark
Reduce the amount of shuffle for groupByKey
```
    val partitionedWordPairRdd = wordPairRdd.partitionBy(new HashPartitioner(4))
    partitionedWordPairRdd.persist(StorageLevel.DISK_ONLY)
    partitionedWordPairRdd.gropByKey().collect()
```
Operations which would benefit from partitioning: 
* join
* leftOuterJoin
* rightOuterJoin
* groupByKey
* reduceByKey
* combineByKey
* lookup.

Running reduceByKey on a pre-partitioned RDD will cause all the values for each key to be computed locally on a single machine, requiring only the final, locally reduced value to be sent from each worker node back to the master.
Operations like map could cause the new RDD to forget the parent's partitioning information, as such operations could, in theory, change the keyof each element in the RDD. General guidance is to prefer mapValues over map operation.

###### Join Operations in Spark
Join operation to join two RDDs together including: leftOuterJoin, rightOuterJoin, crossJoin, innerJoin, etc.

```
    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("JoinOperations").setMaster("local[1]")
        val sc = new SparkContext(conf)

        val ages = sc.parallelize(List(("Tom", 29),("John", 22)))
        val addresses = sc.parallelize(List(("James", "USA"), ("John", "UK")))

        val join = ages.join(addresses)
        join.saveAsTextFile("out/age_address_join.text")

        val leftOuterJoin = ages.leftOuterJoin(addresses)
        leftOuterJoin.saveAsTextFile("out/age_address_left_out_join.text")

        val rightOuterJoin = ages.rightOuterJoin(addresses)
        rightOuterJoin.saveAsTextFile("out/age_address_right_out_join.text")

        val fullOuterJoin = ages.fullOuterJoin(addresses)
        fullOuterJoin.saveAsTextFile("out/age_address_full_out_join.text")
    }
```

* If both RDDs have duplicate keys, join operation can dramatically expand the size of the data, use distinct or combineByKey operation to reduce the key space if possible.
* Joins, in general, are expensive since they require that corresponds keys from each RDD are located at the same partition so that they can be combined locally. If the RDDs do not have known partitioners, they will need to be shuffled so that both RDDs share a partner and data with the same keys lives in the same partitions.
* The default implementation of join in Spark is a shuffled hash join. The shuffled hash join ensures that data on each portion will contain the same keys by portioning the second dataset with the same default partitioner as the first so that the keys with the same hash value from both datasets are int he same partition. 
* The shuffle can be avoided if both RDDs have a known partitioner.
```
       val partitioner = new HashPartitioner(26)
       ages.partitionBy(partitioner)
       addresses.partionBy(partitioner)
```

#### Chapter 5 Advanced Spark Topic

###### Accumulators
Tasks on worker notes cannot access the accumulator's value. Accumulators are read only. Could use reduce or reduceByKey.
Besides the Toal and missingSalaryMidPoint accumulators, add another accumulator to calculate the number of bytes.
Based on different granularity, calculate the bytes process.
Accumulator examples
```
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)

    val total = sparkContext.longAccumulator
    val missingSalaryMidPoint = sparkContext.longAccumulator
    val processedBytes = sparkContext.longAccumulator

    val responseRDD = sparkContext.textFile("in/2016-stack-overflow-survey-responses.csv")

    val responseFromCanada = responseRDD.filter(response => {

      processedBytes.add(response.getBytes().length)
      val splits = response.split(Utils.COMMA_DELIMITER, -1)
      total.add(1)

      if (splits(14).isEmpty) {
        missingSalaryMidPoint.add(1)
      }
      splits(2) == "Canada"

    })

    println("Count of responses from Canada: " + responseFromCanada.count())
    println("Number of bytes processed: " + processedBytes.value)
    println("Total count of responses: " + total.value)
    println("Count of responses missing salary middle point: " + missingSalaryMidPoint.value)
  }
```

###### Broadcast Variables
Keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. Use to share a copy of a large input dataset, in an efficient manner. Kept at all the worker nodes for use.

Create a Broadcast Variable T by calling SparkContext.broadcast() on an object of type T. The Broadcast variable can be any type as long as it's serializable because the broadcast needs to passed from the driver program to all the worker in the Spark cluster across the wire.
```
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("UkMakerSpaces").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)

    val postCodeMap = sparkContext.broadcast(loadPostCodeMap())

    val makerSpaceRdd = sparkContext.textFile("in/uk-makerspaces-identifiable-data.csv")

    val regions = makerSpaceRdd
      .filter(line => line.split(Utils.COMMA_DELIMITER, -1)(0) != "Timestamp")
      .filter(line => getPostPrefix(line).isDefined)
      .map(line => postCodeMap.value.getOrElse(getPostPrefix(line).get, "Unknown"))

    for ((region, count) <- regions.countByValue()) println(region + " : " + count)
  }

  def getPostPrefix(line: String): Option[String] = {
    val splits = line.split(Utils.COMMA_DELIMITER, -1)
    val postcode = splits(4)
    if (postcode.isEmpty) None else Some(postcode.split(" ")(0))
  }

  def loadPostCodeMap(): Map[String, String] = {
    Source.fromFile("in/uk-postcode.csv").getLines.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER, -1)
      splits(0) -> splits(7)
    }).toMap
  }
 ```
Procedures of using Broadcast Variable
* Create a Broadcast variable T by calling SparkContext.broadcst() on an object type T. The broadcst variable can be any type as long as it's erializable because the broadcast needs to passed from the driver program ot all the worker in the Spark cluster across the wire.
* The variable will be sent to each node only once and should be treated as read-only, meaning updates will ntoe be propagated to other nodes.
* The value of the broadcast can be accessed by calling the value method in each node.

Problems if NOT using broadcast variables
* Spark automatically sends all variables reference in the closures to the worker nodes. While this is convenient, it can also be inefficient because the default task launching mechanism is optimized for small task sizes.
* We can potentially use the same variable in multiple parallel operations, but Spark will send it separately foreach operation. This can lead to some performance issue if the site of data to tenser is significant. This can lead to some performance issue if the size of data to transfer is significant. If we use the same postcode map later, the same psotcode map would be sent again to each node.

#### Chapter 6 Apache Spark SQL

###### Introduction to Apache Spark SQL
* Structures data is any data that has a schema - that is, a known set of fields for each record.
* Spark SQL provides a dataset absraction taht simplifies working witb structured datasets. Dataset is similar to tables in a relational database.
* More and more Spark workfolw is moving towards Spark SQL.
* Dataset has a natural schema, and this lets Spark store data in a more efficient manner and can run SQL queries on it using actual SQL command.

DataFrame - tabular data abstraction. A DataFrame is a data abstraction or a domain-specific language for working with structured and semi-structured data. 
It uses the immutable, in-memory, resillient, distributed and parallel capapbilities of RDD, and applies a struture callled schema to dthe data, allowing Spark to manage the schema and only pass data between nodes, in a much more efficient way that using java serializaiotn. Unlike an RDD, data is organized into named columns, like a table in a relational database.

Dataset - the familiar object-oriented programming style, compile-time type safety of the RDD API, the benefits of leveraging schema to work with structured data
A dataset is a set of structured data, not necessarily a row but it could be of a particular type.
- a strongly-typed API and untyped API
- Java and Spark will know the type of the data in a dataset at compile time.

Starting in Spark 2.0, DataFrame APIs merge with Dataset APIs. Dataset takes two distinct APIs characteristics: a strongly-typed API and an untyped API.
Consider DataFrames as untyped view of a Dataset, which is a Dataset of Row where a Row is a generic untyped JVM object.
Dataset, by contrast, is a collection of strongly-typed JVM objects.

###### Spark SQL in Action
```
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()

    val dataFrameReader = session.read

    val responses = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/2016-stack-overflow-survey-responses.csv")

    System.out.println("=== Print out schema ===")
    responses.printSchema()

    val responseWithSelectedColumns = responses.select("country", "occupation", AGE_MIDPOINT, SALARY_MIDPOINT)

    System.out.println("=== Print the selected columns of the table ===")
    responseWithSelectedColumns.show()

    System.out.println("=== Print records where the response is from Afghanistan ===")
    responseWithSelectedColumns.filter(responseWithSelectedColumns.col("country").===("Afghanistan")).show()

    System.out.println("=== Print the count of occupations ===")
    val groupedDataset = responseWithSelectedColumns.groupBy("occupation")
    groupedDataset.count().show()

    System.out.println("=== Print records with average mid age less than 20 ===")
    responseWithSelectedColumns.filter(responseWithSelectedColumns.col(AGE_MIDPOINT) < 20).show()

    System.out.println("=== Print the result by salary middle point in descending order ===")
    responseWithSelectedColumns.orderBy(responseWithSelectedColumns.col(SALARY_MIDPOINT).desc).show()

    System.out.println("=== Group by country and aggregate by average salary middle point ===")
    val datasetGroupByCountry = responseWithSelectedColumns.groupBy("country")
    datasetGroupByCountry.avg(SALARY_MIDPOINT).show()

    val responseWithSalaryBucket = responses.withColumn(SALARY_MIDPOINT_BUCKET,
      responses.col(SALARY_MIDPOINT).divide(20000).cast("integer").multiply(20000))

    System.out.println("=== With salary bucket column ===")
    responseWithSalaryBucket.select(SALARY_MIDPOINT, SALARY_MIDPOINT_BUCKET).show()

    System.out.println("=== Group by salary bucket ===")
    responseWithSalaryBucket.groupBy(SALARY_MIDPOINT_BUCKET).count().orderBy(SALARY_MIDPOINT_BUCKET).show()

    session.stop()
  }
}
```

###### Catalyst Optimizer
* Spark SQL uses an optimizer called Catalyst to optimize all the queries written both in Spark SQL and DataFrame DSL.
* The Catalyst is a modular library with is built as a rule-based system. Each rule in the framework focuses on the specific optimization.
```
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("HousePriceSolution").master("local[1]").getOrCreate()

    val realEstate = session.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/RealEstate.csv")

    realEstate.groupBy("Location")
      .avg(PRICE_SQ_FT)
      .orderBy("avg(Price SQ Ft)")
      .show()
  }
 ```

###### Spark SQL Joins
* Spark SQL supports the same basic join types as core Spark.
* Spark SQL Catalyst optimier can do more of heavy lifting for us to optimize the join performance.
* Using Spark SQL join, we have to give up some of our control. Spark SQL can sometimes push down or re-order operations to make th joins moer efficient. The downside is that we don't have controls over the partitioner for Datasets, so we can't manaullay avoid shuffles as we did with core Spark joins.

Spark SQL Join types
The standard SQL join types are supported by Spark SQL , 
* inner, outer, left outer, right outer, left semi

```
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val session = SparkSession.builder().appName("UkMakerSpaces").master("local[*]").getOrCreate()

    val makerSpace = session.read.option("header", "true").csv("in/uk-makerspaces-identifiable-data.csv")

    val postCode = session.read.option("header", "true").csv("in/uk-postcode.csv")
       .withColumn("PostCode", functions.concat_ws("", functions.col("PostCode"), functions.lit(" ")))

    System.out.println("=== Print 20 records of makerspace table ===")
    makerSpace.select("Name of makerspace", "Postcode").show()

    System.out.println("=== Print 20 records of postcode table ===")
    postCode.show()

    val joined = makerSpace.join(postCode, makerSpace.col("Postcode").startsWith(postCode.col("Postcode")), "left_outer")

    System.out.println("=== Group by Region ===")
    joined.groupBy("Region").count().show(200)
  }
```

###### Strongly Typed Dataset
* A dataset is a strongly typed collection of domain of domain-specific objects that can be transformed in parallel using functional or relational operations.
* Each Dataset also has an untyped view called a DataFrame, which is a Dataset of Row. Dataset<row> = DataFrame
 * Row 
  - Row objects represent records inside dataset and are simply fixed-length arrays of fields. 
  - Row objects have a number of get functions to obtain the value of each filed given its index. The get method takes a column number and returns us an Any instance; we are reponnsible for casting the Any instance to the correct type.
  - For primitive and boxed types, there is a get type method, which returns the value of that type.
  
* Encoders
  - When it comes to serializing data, the Dataset API has the concept of encoders which translate between JVM represtations with are Java objects and Spark's itnernal binary format.
  - Spark has built-in encoers such as interger encoder or long encoder which are very advanced in that they generate bytecode to interact with off-heap data and provide on-demand access to individual attributes without having to de-serialize an entire object.
  Encoders.scalaInt
  Encoders.scalaLong
  
```
package com.sparkTutorial.sparkSql
 case class Response(country: String, age_midpoint: Option[Double], occupation: String, salary_midpoint: Option[Double])
```

```
   def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[*]").getOrCreate()
    val dataFrameReader = session.read

    val responses = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/2016-stack-overflow-survey-responses.csv")

    val responseWithSelectedColumns = responses.select("country", "age_midpoint", "occupation", "salary_midpoint")

    import session.implicits._
    val typedDataset = responseWithSelectedColumns.as[Response]

    System.out.println("=== Print out schema ===")
    typedDataset.printSchema()

    System.out.println("=== Print 20 records of responses table ===")
    typedDataset.show(20)

    System.out.println("=== Print the responses from Afghanistan ===")
    typedDataset.filter(response => response.country == "Afghanistan").show()

    System.out.println("=== Print the count of occupations ===")
    typedDataset.groupBy(typedDataset.col("occupation")).count().show()

    System.out.println("=== Print responses with average mid age less than 20 ===")
    typedDataset.filter(response => response.age_midpoint.isDefined && response.age_midpoint.get < 20.0).show()

    System.out.println("=== Print the result by salary middle point in descending order ===")
    typedDataset.orderBy(typedDataset.col(SALARY_MIDPOINT).desc).show()

    System.out.println("=== Group by country and aggregate by average salary middle point ===")
    typedDataset.filter(response => response.salary_midpoint.isDefined).groupBy("country").avg(SALARY_MIDPOINT).show()

    System.out.println("=== Group by salary bucket ===")
    typedDataset.map(response => response.salary_midpoint.map(point => Math.round(point / 20000) * 20000).orElse(None))
      .withColumnRenamed("value", SALARY_MIDPOINT_BUCKET)
      .groupBy(SALARY_MIDPOINT_BUCKET)
      .count()
      .orderBy(SALARY_MIDPOINT_BUCKET).show()
  }
```

###### Use Dataset or RDD

Dataset
* Datasets are the new hotness.
* MLlib is on a shift to Dataset based API.
* Spark streaming is also moving towards, somehting called structured streaming which is heavily based on Dataset API.
* Consider using Datasets over RDDs, if possible.
* RDD will remain to be the one of the most critical ore compoents of Spark, and it is the underlying building block for Dataset.

Use RDD when -
* The RDDs are still the core and fundamental building block of Spark. Both DataFrames and Datasets are build on top os RDDs.
* RDD is the primary user-facing API in Spark. At teh core, RDD is an immutable distributed collection of elements of your data, partitioned across nodes in your cluster that can be operated in parallel with a low-level API that offers transformations and actions.
* low-level transforamtion, actions and control on our dataset are needed.
* Unstructured data, such as media streams or streams of text. 
* Neeed to manipulate our data wiht functional programming contrstructs than domain specific expression.
* Optimization and performance benefits available with Datasets are NOT needed.

Use Datasets when -
- Rich semantics, high-level abstractins, and domain specific APIs are needed.
- Our processing requiress aggregation, averages, sum, SQL queries and columnar access on semi-structured data.
- a higher degree of type-safety at compile time, typed JVM objects and the benefit of Catalyst optimization.
- Unification and simplification of APIs across Spark Libraries are needed.

###### Dataset and RDD Convension
```
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[1]")

    val sc = new SparkContext(conf)

    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()

    val lines = sc.textFile("in/2016-stack-overflow-survey-responses.csv")

    val responseRDD = lines
      .filter(line => !line.split(Utils.COMMA_DELIMITER, -1)(2).equals("country"))
      .map(line => {
        val splits = line.split(Utils.COMMA_DELIMITER, -1)
        Response(splits(2), toInt(splits(6)), splits(9), toInt(splits(14)))
      })

    import session.implicits._
    val responseDataset = responseRDD.toDS()

    System.out.println("=== Print out schema ===")
    responseDataset.printSchema()

    System.out.println("=== Print 20 records of responses table ===")
    responseDataset.show(20)

    for (response <- responseDataset.rdd.collect()) println(response)
  }

  def toInt(split: String): Option[Double] = {
    if (split.isEmpty) None else Some(split.toDouble)
  }
```

###### Performance Tuning of Spark SQL

* Built-in optimizations - Spark SQL has built-in optimizations such as predicate push-down which allows Spark SQL to move some parts of our query down to the engine we are querying.

* Caching - performing some queries or transfomations on a dataset reepeatedly, we should consider catching the dataset which can be donw by callling the cache method on the dataset. Spark SQL uses an in-memory columnar storage for the dataset.
```
responseDataset.cache();
```
When caching a dataset, Spark SQL uses an in-memory columnar storage for the dataset. If our subsequent queries depend only on subsets of the data, Spark SQL which minimize the data read, and automatically tune compression to reduce garbage collection pressure and memory usage.

 * Configuation - 
 ```
 val session = SparkSession.builder()
 .config("spark.sql.codegen", val = false)
 .getOrCreate()
 ```
  spark.sql.codegen option could make long queries or repeated queries substantially faster, as Spark generates specific code to run them, for workflows which involves large queries, or with the smae repeated query.
  However, for short queries or some non-repeated adhoc queries, this option could add unnecessary overhead as Spark has to run a compiler for each query. 
  
```
val session = SparkSession.buider()
.config("spark.sql.inMemoryColumnarStorage.batchSize", value=1000)
.getOrCreate()
```
  spark.sql.inMemoryColumnarStorage.batchSize option - Spark groups togother the records in batches of the size given by this option and compresses each batch. Having a larger batch size can improve memory utilization and compression. 
  However, a batch wih a large number of records might be hard to build up in memory and can lead to an OutOfMemoryError.

#### Chapter 7 Running Spark in a cluster

###### Introduction to Running Spark in a cluster

Driver Program
Cluster Manager - pluggable component in Spark, Standalone, Hadoop Yarn, Apache Mesos
Worker Node

###### Package Spark Application and Use spark-submit

* Download Spark distribution to our local box.

* Export our Spark application to a jar file. 
Package Dependencies. If your program imports any libraries that are nto in the org.apache.spark package or part of the language library, need to ensure that all your dependencies are  present at the runtime of your Spark application. Must ship with its entire transitive dependency graph ot the cluster. 
Typically rely on a build tool(e.g., gradle) to produce a single large JAR containing the entire transitive dependency graph of an application.
```
./gradlew jar
ls build/libs/
```

* Submit our application to our local Spark cluster through spark-submit script. 

Running Spark Applications on a Cluster
* The user submits an application using spark-submit
* spark-submit launches the driver program and invokes the main methods specified by the user.
* The driver program contacts the cluster manager to ask for resources to start executors.
* The cluster manager launches executors on behalf of the driver program.
* The driver program runs through the user application. Based on the RDD or dataset operations in the program, the driver sends work to executors in the form of tasks.
* Tasks are run on executor processes to compute and save results.
* If the driver's main method exists or it calls SparkContext.stop(), it will terminate the executors.

spark-submit options
```
./bin/spark-submit\
-- executor-memory = 20G \
-- total-executor-cores 100 \
/path/to/examples.jar
```
* We can run Spark applications from a command line or execute the script periodically using a Cron job or other scheduling service.
* spark-submit script is an available script on any operating system that supports Java. 

###### Run Spark Application on Amazon EMR (Elastic MapReduce) cluster
S3 is a distributed storage system and AWS's equivalent to HDFS.


### Disclaimer

This repository and its contents are collected and shared solely for academic and research purposes.
All code, data, and related materials are intended to support independent study, experimentation, and learning.

If you believe any part of this repository inadvertently includes content that should not be shared publicly or may cause concern, please contact me immediately. I will review and, if necessary, remove the material without delay.

I do not claim ownership of any third-party data or content and have made every effort to respect intellectual property and privacy rights.
