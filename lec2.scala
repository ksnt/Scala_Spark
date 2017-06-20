/* week2 */
/* ksnt  */

//* Reduction Operations & Distributed Key-Value Pairs *//
// NOTE: Psuedo codes might be inculded

//1.Reduction Operations

// What do we mean by "reduction operations"?
// Reduction Operations: walk through a collection and combine neighboring elements od the collection together to produce a single combined result

// Example

case class Taco(kind: String, price: Double)
val tacoOrder = 
  List(
  	Taco("Carnitas",2.25),
  	Taco("Corn",1.75),
  	Taco("Barbacoa",2.50),
  	Taco("Chicken",2.00))

val cost = tacoOrder.foldLeft(0,0)((sum,taco) => sum + taco.price)

// Prallel Redution Operations: foldLeft vs fold
// foldLeft is not parallelzable
// fold can be parallelized

// Prallel Redution Operations: Aggregate
// aggregate[B](z: => B)(seqop: (B,A) => B, combop: (B,B) => B):B
// Properties of aggregate
//  1.Parallelzable
//  2.Possible to change the return type


//2.Distributed Key-Value Pairs(Pair RDDs)
// In single-node Scala, key-value pairs can be thought of as maps
// (Or associatie arrays or dictionaries in JS or Python)

// In Spark, distributed key-value pairs are "Pair RDDs"
// Useful because: Pair RDDs allow you to act on each key in parallel or regroup data across the network

// Pair RDDs
// val rdd: RDD[WikipediaPage] = ...
// val pairRdd = rdd.map(page => (page.title,page.text))


//3.Transformations and Actions on Pair RDDs

// Some interesting Pair RDDs operations
// *Transformations
//   groupByKey
//   reduceByKey
//   mapValues
//   key
//   join 
//   leftOuterJoin/rightOuterJoin
// *Action
//   countByKey

// Pair RDD Transformation: groupBy
// def groupBy[K](f: A=> K): Map[K,Traversable[A]]
// Example
val ages = List(2,52,44,23,17,14,12,82,51,64)
val grouped = ages.groupBy{age =>
    if(age >= 18 && age < 65) "adult"
    else if(age < 18) "child"
    else "senior"
    }

// Pair RDD Transformation: groupByKey
// def groupByKey(): RDD[K,Iterable[V]]
// Example
case class Event(organizer: String, name: String, budget: Int)
val eventsRdd = sc.parallelize(...).map(event => (event.organizer,event.budget))
val groupedRdd = eventsRdd.groupByKey()
groupedRdd.collect().foreach(println)


// Pair RDD Transformation: reduceByKey
// def reduceByKey(func: (V,V) => V): RDD[(K,V)]
// Example
case class Event(organizer: String, name: String, budget: Int)
val eventsRdd = sc.parallelized(...).map(event => (event.organizer,event.budget))
val budgetsRdd = eventsRdd.reduceByKey(_+_)
reducedRdd.collect().foreach(println)


// Pair RDD Transformation: mapValues and Action: countByKey
// def mapValues[U](f: V => U): RDD[(K,U)]
// Example
//Calculate a pair (as a key's value) containing (budget, #events)
val intermediate = eventsRdd.mapValues(b => (b,1)).reduceByKey((v1,v2) => (v1._1 + v2._2, v1._2 + v2._2))

val avgBudgets = intermediate.mapValues{
	case(budget, numberOfEvents) => budget / numberOfEvents
}

avgBudgets.collect().foreach(println)

// Pair RDD Transformation: keys
// def keys: RDD[K]
// Example
case class Visitor(ip: String, timestamp: String ,duration: String)
val visits: RDD[Visitor] = sc.textfile(...).map(v=>(v.ip,v.duration))
val numUniqueVisits = visits.keys.distinct().count()



//4.Joins 

// There are two kinds od joins:
//  1.Inner join (join)
//  2.Outer join(leftOuterJoin/rightOuterJoin)

// Example
val as = List((101,("Ruetli",AG)), (102,("Brelaz",DemiTarif)), (103,("Gress",DemiTarifVisa)),(104,("Schatten",DemiTarif)))
val abos = sc.parallelize(as)

val ls = List((101,"Bern"), (101,"Thun"), (102,"Lausanne"),(102,"Geneve"),(102,"Nyon"),(103,"Zurich"),(103,"St-Gallen"),(103,"Chur"))
vals localtions = sc.parallelize(ls)


// Inner Joins (join)
// def join[W](other: RDD[(K,W)]): RDD[(K,(V,W))]

val trackedCustomers = abos.join(locations)
trackedCustomers.collect().foreach(println)

// Outer joins

val abosWithOptionalLocations = abos.leftOuterJoin(locations)

val customersWithLocationDataAndOptionalAbos = abos.rightOuterJoin(locations)


