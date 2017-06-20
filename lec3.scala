/* week3 */
/* ksnt  */

//* Partitioning and Shuffling *//
// NOTE: Psuedo codes might be inculded

//1. Shuffling: What it is and why it's important

// Grouping and Reducing Example
// Goal: calculate how many trips and how much money was spent by each individual customer over the course of the month

//val purchasesRdd: RDD[CFFPurchase] = sc.textFile(...)
// assuming there exists a class which can be used
val purchasesRdd.map(p => (p.customerId, p.price)).groupByKey()

// Grouping and Reducing Example - Optimized
// def reduceByKey(func: (V,V) => V): RDD[(K,V)]
val purchasesRdd:: RDD[CFFPurchase] = sc.textFile(...)

val purchasesPerMonth = purchasesRdd.map(p=>(p.cosutomerId,(1,p.price))) // Pair RDD
                        .reduceByKey((v1,v2) => (v1._1 + v2._1, v1._2 + v2._2))
                        .collect()
// Shuffling
// Grouping all values of key-value pairs with the same key requires collecting al lkey-value pairs with the same key on the same machine
// How does Spark know which key to put on which machine? => Partitioning

//2. Partitioning

// Properties od partitions 
//  -Partitions never span multiple machines
//  -Each machine in the cluster contains one or more partitions
//  -The number of partitions to use is configuable. By default, it equals the total number of cores on all executor nodes

// Two kinds of partitioning in Spark
//  -Hash partitioning
//  -Range partitioning

// Hash partitioning
val purchasePerCost = purchasesRdd.map(p => (p.cosutomerId,p.price)) // Pair RDD                          .groupByKey()

//groupByKey first computes per tuple (k,v) its partiion p
p = k.hashCode() % numPartitions
//Then, all tuples in the same partion p are sent to the machine hosting p
// Intuition: has pertitioning attemps to spread data evenly across pertitions based on the key

// Range partitioning

// using a range partitioner, keys are partitioned according to:
// 1. an ordering dor keys
// 2. a set of sorted ranges of keys


// Parttioning Data: partitionBy
val pairs = purchasesRdd.map(p => p.customerId,p.price)
val tunedPartitioner = new RnagePartitioner(8,pairs)
val partitioned = pairs.partitionBy(tunedPartitioner).persist()

//Important:
// the result of partitionBy should be persisted. Otherwise, the partitioning is repeatedly applied (involving shuffling!) each time the partiioned RDD is used.

//Partitioning Data Using Transformations
//All other operations will produce a result without a partitioner
//Why?
// Given that we have a hash partitioned Pair RDD, why would it make sense for map to lose the partitioner in its result RDD? 
// Because It's possibe for map to change the key


//3. Optimizing with Partitioners

// Optimization using range partitioning
val pairs = purchasesRdd.map(p => p.customerId,p.price)
val tunedPartitioner = new RnagePartitioner(8,pairs)
val partitioned = pairs.partitionBy(tunedPartitioner).persist()
val purchasePerCost = partitioned.map(p => (p._1, (1,p._2)))
val purchasePerMonth = purchasePerCost.reduceByKey((v1,v2) => (v1._1 + v2._1, v1._2 + v2._2)).collect()


// NOTE: operations might cause  shuffle
// ex. join

// Avoiding a network shuffle by partitioning
// There are a few ways to use operations that might cause a shuffle and to avoid much or all networking shuffling
// Example
// 1.reduceByKey running on a pre-partitioned RDD will cause the values to be computed locally, requiring only hte final reduced value has to be sent from the worker to the driver 
// 2.join called on two RDDs that are pre-partitionedwith the same partitioner and cached on the same machine will cause the join to be computed locally, with no shuffling across the network

//4. Wide vs Narrow Dependencies

// Def. Lineages
// Computing on RDDs are represented as a lineage graph; a Directed Acyclic Graph(DAG) representing the computations done on the RDD.

// Seeing lineages, we can investigate dependencies.
// Narrow dependencies vs Narrow dependencies


// How can I find outt?
// dependencies method on RDDw
// 1.Narrow dependency objects:
//  - OneToOneDependency
//  - PruneOneDependency
//  - RangeOneDependency
// 2.Wide dependency objects:
//  - ShuffleDependency

// Lineages and Fault Tolerane
// Lineages graphs are the key to fault tolerance in Spark
// Ideas from FP enable fault tolerance
// -RDD are immutable
// -higher-order functions like map to do fucntional transformations on this immutable data
// A function for computing the dataset based on its parent RDDs also is part of an RDD's representation
