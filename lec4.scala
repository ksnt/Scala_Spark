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






// 4.DataFrames(2)






// 5.Datasets





