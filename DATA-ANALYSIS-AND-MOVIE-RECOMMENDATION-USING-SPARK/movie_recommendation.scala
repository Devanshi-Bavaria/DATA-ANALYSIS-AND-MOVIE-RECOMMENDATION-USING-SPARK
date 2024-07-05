import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS

case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
def parseRating(str: String): Rating = {
  val fields = str.split("::")
  assert(fields.size == 4)
  Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
}

val ratings = spark.read.textFile("/../../Movielens/sample_movielens_ratings.txt")
  .map(parseRating)
  .toDF()

ratings.show()
//
val splitData = ratings.withColumn("split_values", split(col("value"), "::"))

val parsedData = splitData.select(
  splitData("split_values")(0).cast(IntegerType).alias("userId"),
  splitData("split_values")(1).cast(IntegerType).alias("movieId"),
  splitData("split_values")(2).cast(FloatType).alias("rating"),
  splitData("split_values")(3).cast(LongType).alias("timestamp")
)

val finalData = parsedData.drop("value", "split_values")

finalData.show()
//
val ratings=finalData

val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
//
 val als = new ALS()
      .setMaxIter(10)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
val model = als.fit(training)
//
model.setColdStartStrategy("drop")

val predictions = model.transform(test)
//
val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
val rmse = evaluator.evaluate(predictions)
println(s"Root-mean-square error = $rmse")

//Generate top 10 movie recommendations for each user
val userRecs = model.recommendForAllUsers(10)
userRecs.show()
//show 2nd column

val row = userRecs.take(1)(0)
val arrayValue = row.getAs[Seq[org.apache.spark.sql.Row]](1)

// Iterate through the array and print its content
arrayValue.foreach { row =>
  val intVal = row.getInt(0)  // Assuming the first column in the row is an integer
  val floatVal = row.getFloat(1)  // Assuming the second column in the row is a float
  println(s"Int Value: $intVal, Float Value: $floatVal")
}


// Generate top 10 user recommendations for each movie
val movieRecs = model.recommendForAllItems(10)
movieRecs.show()

// Generate top 10 movie recommendations for a specified set of users
val users = ratings.select(als.getUserCol).distinct().limit(3)
val userSubsetRecs = model.recommendForUserSubset(users, 10)
userSubsetRecs.show()

// Generate top 10 user recommendations for a specified set of movies
val movies = ratings.select(als.getItemCol).distinct().limit(3)
val movieSubSetRecs = model.recommendForItemSubset(movies, 10)
movieSubSetRecs.show()

