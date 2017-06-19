/* week1 */
/* ksnt  */

//* Getting Started + Spark Basics *//
// NOTE: Psuedo codes might be inculded

//A.From Parallel to Distributed

// Distribution
// Distribution introduces important concern beyond what we had to worry about when dealing with parallelism in the shared memory case:
// Note: Spark handles these two issues well
// - Partial failure: crash failure of a subset of the machines involved in a distributed computation
// - Latency: certain operations havce a much higher latency than other operations due to communication

//important latency numbers
// https://gist.github.com/jboner/2841832

// humanized latency numbers
// https://gist.github.com/hellerbarde/2843375#file-latency_humanized-markdown



//B.Basics of Spark's RDDs

//1.Resilient Distributed Datasets(RDDs), Spark's Distributed Collection


// The "Hello, World!" of programming with large-scale data
// Create an RDD
/*
val rdd = spark.textfile("hdfs://...")           // separate lines into words
val count = rdd.flatMap(line => line.split(" ")) // include something to count
            .map(word => (word,1))               // sum up the 1s in the pairs
            .reduceByKey(_ + _)

*/ 

// How to Create an RDD
// 1. Transforming an existing RDD
//    use map with higher-order function
// 2. From a SparkContext (or SparkSession) object 
//    The SparkContext object (renamed SparkSession) can be used.
//    1. parallelize: convert a local Scala collection to an RDD
//    2. textFile: read a text file from HDFS or a local system and return an RDD od string


//2.RDDs: Transformation and Actions

/// Remark: Actions means operations on RDDs

// On Scala
// Transformers: Return new collections as results (Not single values)
// Example: map, fileter, flatMap, groupBy
// map(f: A => B): Traversable[B]

// Accessors: Return single calues as results (Not collections)
// Ecamples: reduce, fold, aggregate
// reduce(op: (A,A) => A):A 

// On Spark
// Transformation: Reutrn new RDDs as results
//    lazy: Their resuls RDD is not immediately computed
// Actions: Compute a result based on an RDD, and wither returened or saved to an external storage system (e.g., HDFS)
//     eager: Their result is immediately computed

// Example
/*
sc: SparkContext

val largeList: List[String] = ....
val wordsRdd = sc.parallelize(largeList)
val lengthsRdd = wordsRdd.map(_.length)
// Now nothing happens because map is deferred
val totalChars = lengthsRdd.reduce(_ + _) / / This is an action, so operation is executed
*/

// Common Transformations
// map
// flatMap
// fileter
// distinct

// Common Actions
// collect
// count
// take
// reduce
// foreach

// Transformations on Two RDDs
// union
// intersection
// subtract
// cartesian
// ex. val rdd3 = rdd1.union(rdd2)

// Other Useful RDD Actions
// takeSample
// takeOrdered
// saveAsTextFile
// saveAsSequenceFile


//3.Evaluation in Spark: Unlike Scala Collections!

//Why is Spark Good for Data Science?
// Most Data Science problems involcve iteration

/* Iteration Example :Logistic Regression

val points = sc.textFile(...).map(parsePoint)
var w = Vector.zero // weights
for(i <- 1 to numIterations){
	val gradient = points.map{p =>
		(1 / (1 + exp(-p.y * w.dot(p.x))) - 1) * p.y * p.y
	}.reduce(_ + _)
	w -= alpha + gradient
}

points is being re-evaluated upon every iteration. This is not necessary. => What can we do?

By default, RDDs are recomputed every time you run an action on them. If you need to use a data set more than once, this can be expensive(in time).

Spark allows you to control what is cached in memory.
=> Use persist() or cache()

val lastYearLogs: RDD[String] = ...
val logWithErrors = lastYearsLogs.filter(_.contains("ERRO")).persist()
val firstLogsWithErrors = logsWithErrors.take(10)
val numErrors = logWithErrors.count() // faster

-- Improved Logstic Regression Example

val points = sc.textFile(...).map(parsePoint).persist()
var w = Vector.zero(d) // weights
for(i <- 1 to numIterations){
	val gradient = points.map{ p =>
		(1 / (1 + exp(-p.y * w.dot(p.x))) - 1) * p.y * p.y
	}.reduce(_ + _)
	w -= alpha + gradient
}

*/

// There are many ways to configure how your data is persister
//  a.in memory as regular Java objects
//  b.on disk as regular Java objects
//  c.in memory as serialized Java objects (more compact)
//  d.on disk as serialized Java objects(more compact)
//  e. both in memory and on disk (spil over to disk to avoid re-computation)

// cache(): Easy way to use the default strage level
// persist(): can be customized


//4.Cluster Topology Matters!

// omit