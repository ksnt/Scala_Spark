/* week4 */
/* ksnt  */

//* Structured data: SQL, Dataframes, and Datasets *//
// NOTE: Psuedo codes might be inculded

// 1.Structured vs Unstructured Data

case class Demographic(id: Int,
	                   age: Int,
	                   codingBootcamp: Boolean,
	                   country: String,
	                   gender: String,
	                   isEthnicMinority: Boolean,
	                   servedInMilitary: Boolean)
val demographics = sc.textfile(...) // Pair RDD (id,demographic)

case class Finances(id: Int,
	                hasDept: Boolean,
	                hasFinancialDependents: Boolean,
	                hasStudentLoans: Boolean,
	                income: Int)

val finances = sc.textfile(...) // Pair RDD, (id,finances)


// An example: Selecting Scholarship Recipients
// Count 
// -Swiss students 
// -who have debt & financial dependents 

// Possibility1:
// (Int,(Demographic,Finances))
// p-> id
// p._2 -> (Demographic,Finances)
// p._2._1 -> Demographic
// p._2._2 -> Finances
demographics.join(finances)
            .filter{p =>
              p._2._1.country == "Switzerland" &&
              p._2._2.hasFinancialDependents &&
              p._2._2.hasDept
          }.count

// Possibility2:
val filtered = finances.filter(p=>p._2.hasFinancialDependents && p._2.hasDept)

demographics.filter(p=>p._2.country=="Switzerland")
            .join(filtered)
            .count

val cartesian = demographics.cartesian(finanes)

//Possibility3
cartesian.filter{
	case (p1,p2) => p1._1 == p2._1 // same ids
}
.filater{
	case(p1,p2) => (p1._2.country == "Switzerland") &&
	               (p2._2.hasFinancialDependents) &&
	               (p2._2.hasDept)
}.count

//Remark
// While for all these 3 possible examples, the end result is the same, the time it takes to execute the job is vastly different.

// Possibility1 vs Possibility2 vs Possibility3
// Filtering data first(Possibility2) is 3.6x faster for 150000 people data!
// Possibility3 is 177x slower!


// Structured vs Unstructured Data
// Unstructured -> Log files, Images
// Semi-Structured -> JOSN, XML
// Structured -> Database tables

// Structured vs Unstructured Computation
// In Spark:
// -We do functional transformations on data
// -We pass user-defined function literals to higher-order function like map, flatMap, and filter
// In a database/Hive
// -We do declarative transformations on data
// -Specialized/structured, pre-defined operations

// Wouldn't it be nice if Spark could do some optimizations for us?
// => Spark SQL makes this possible!

// 2.Spark SQL

// Spark SQL: Three main goals
// 1.Support relational processing both withing Spark programs(on RDDs) and on external data sources with a friendly API.
// 2.High performance, achieved by usng techniques from research in databases.
// 3.Easily Support new data sources such as semi-structured data and external databases

// Note: Spark SQL is a component of the Spark stack
// Three main APIs:
// -SQL literal syntax
// -DataFrames
// -Datasets
// 
// Two specialized backend components:
// -Catalyst, query optimizer
// -Tungsten, off-heap serializer

// SparkSession
// To get started using Spark SQL, everything starts with the SparkSession
import org.apache.spark.SparkSession

val spark = SparkSession
            .builder()
            .appName("My App")
            .getOrCreate()

// Creating DataFrames
//  1. From an existing RDD
//  2. Reading in a specific data source from file

// (1a) Create Dataframe from RDD, schema reflectively inferred
// Gicen pair RDD, RDD[(T1,T2,...,TN)], a DataFrame can ne created with its schema automatically inferred by simply using the toDF method.

val tupleRDD = ... // Assume ADD[(Int, String, String, String)]
val tupleDF = tupleRDD.toDF("id","name","city","country") // column names

// If you already have an RDD containing some kind of case class instance, then Spark can infer the attributes from the case class's fields.
case class Persn(id:Int,name:String,city:String)
val peopleRDD = .... // Assume RDD[Person]
val peopleDF = peopleRDD.toDF

// (1b) Create Dataframe from existing RDD, schema explicitly specified
// In some cases, it is not possible to create a DataFrame with a pre-determined case class as its schema. For these cases, it's possible to explicitly specify a schema
// It takes three steps:
// - Create an RDD of Rows from the original RDD
// -Create the schema represented by a StructType matchin the structure of ROws in the RDD created in Step 1
// -Apply the schema to the RDD od Rows via createDataFrame methos provided by SparkSession

// Given:
case class Person(name:String, age:Int)
val peopleRdd = sc.textFile(...) // Assume RDD[Person]
// The schema is encoded in a String
val schemaString = "name age"
//Generate the schema based on the string of schema
val fields = schemaString.split(" ")
             .map(fieldName => StrutField(fieldName,StrinType,nullable=true))
val schema = StructType(fields)
//Convert records of the RDD (people) to Rows
val rowRDD = peopleRDD
             .map(_.split(","))
             .map(attributes => Row(attributes(0),attributes(1).trim))
//Apply the schema to the RDD
val peopleDF = sparl.createDataFrame(rowRDD,schema)

// (2) Create Dataframe by reading in a data source from file
// Usin SparkSession obeject, you can read in semi-structured/structured data by using the read method

//'spark' is the SparkSession object we created a few slides back
val df = spark.read.json("examples/src/main/resources/people.json")

// SQl literals
// A DataFrame called peopleDF, we just hace to register our DataFrame as a temporary SQL view first

// Register the DataFrame as a SQL temporary view
peopleDF.createOrReplaceTempView("people")
// This essentially gives a name to out DataFrame in SQL
// so we can refer to it in an SQL FROM statement
// SQL literals can be passed to Spark SQL's sql method
val adultsDF = spark.sql("""SELECT * FROM people WHERE age > 17""")


// 3.DataFrames(1)

// Acessing Spark SQL Types
// Important: In order to access any of these data types, wither basic or complex, you must first import Spark SQL types
import org.apache.spark.sql.types._

// Getting a look at your data
// show() pretty-prints DataFrame in tabular form.
// Example
case class Employee(id:Int, fname:String,lname:String,age:Int,city:String)
val employees = 
  List(
  	Employee(1,"Tom","White",25,"Tokyo"),
  	Employee(2,"Sam","Black",35,"London"),
  	Employee(3,"Barak","Obama",55,"Washington"),
  	Employee(4,"Donald","Trumph",60,"Washington"))

val employeeDF = sc.parallelize(employees).toDF
employeeDF.show()
/* 
+---+------+------+---+----------+
| id| fname| lname|age|      city|
+---+------+------+---+----------+
|  1|   Tom| White| 25|     Tokyo|
|  2|   Sam| Black| 35|    London|
|  3| Barak| Obama| 55|Washington|
|  4|Donald|Trumph| 60|Washington|
+---+------+------+---+----------+
*/

// printSchema() : prints the schema of your DataFrame in a tree format
employeeDF.printSchema()
/*
root
 |-- id: integer (nullable = true)
 |-- fname: string (nullable = true)
 |-- lname: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- city: string (nullable = true)
*/

//Specifying Columns
// 1.using $-notation
val over30 = employeeDF.filter($"age" > 30 )
over30.show()
/*
+---+------+------+---+----------+
| id| fname| lname|age|      city|
+---+------+------+---+----------+
|  2|   Sam| Black| 35|    London|
|  3| Barak| Obama| 55|Washington|
|  4|Donald|Trumph| 60|Washington|
*/

// 2.Reffering to the DataFrame
val over30_2 = employeeDF.filter(employeeDF("age") > 30)
over30_2.show()
/*
+---+------+------+---+----------+
| id| fname| lname|age|      city|
+---+------+------+---+----------+
|  2|   Sam| Black| 35|    London|
|  3| Barak| Obama| 55|Washington|
|  4|Donald|Trumph| 60|Washington|
+---+------+------+---+----------+
*/

// Using SQL quering string
val over30_3 = employeeDF.filter("age > 30") 
 over30_3.show()
 /*
+---+------+------+---+----------+
| id| fname| lname|age|      city|
+---+------+------+---+----------+
|  2|   Sam| Black| 35|    London|
|  3| Barak| Obama| 55|Washington|
|  4|Donald|Trumph| 60|Washington|
+---+------+------+---+----------+
*/

val washingtonEmployeeDF = employeeDF.select("id","lname").where("city == 'Washington'").orderBy("id")
washingtonEmployeeDF.show()
/*
+---+------+
| id| lname|
+---+------+
|  3| Obama|
|  4|Trumph|
+---+------+
*/

// calculate total age in each city // this might not be useful...
val cityTotalAgeDF = employeeDF.groupBy($"city").agg(sum($"age"))
cityTotalAgeDF.show()
/*
+----------+--------+                                                           
|      city|sum(age)|
+----------+--------+
|    London|      35|
|     Tokyo|      25|
|Washington|     115|
+----------+--------+
*/


// More example
case class Post(authorID: Int, subforum: String, likes: Int, date:String)
val post = List(
  	Post(1,"a",25,"2014-08-01 23:01:05"),
  	Post(2,"b",35,"2014-11-01 21:00:37"),
  	Post(3,"b",105,"2015-01-01 22:10:05"),
  	Post(4,"c",10,"2016-08-06 01:02:05"),
    Post(1,"c",100,"2014-08-06 03:02:05"))

val postDF = sc.parallelize(post).toDF

val rankedDF = postDF.groupBy($"authorID",$"subforum").agg(count($"authorID")).orderBy($"subforum",$"count(authorID)".desc)

rankedDF.show()
/*
+--------+--------+---------------+                                             
|authorID|subforum|count(authorID)|
+--------+--------+---------------+
|       1|       a|              1|
|       3|       b|              1|
|       2|       b|              1|
|       1|       c|              1|
|       4|       c|              1|
+--------+--------+---------------+
*/

// 4.DataFrames(2)






// 5.Datasets





