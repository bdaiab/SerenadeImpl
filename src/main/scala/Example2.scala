/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println

// $example on$
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.functions.col

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.io.Source
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example demonstrating ALS.
 * Run with
 * {{{
 * bin/run-example ml.ALSExample
 * }}}
 */
object Example2 {

    // $example on$
    case class Rating(timestamp: Long, sessionId: Int, itemId: Int, rating: Float)
    def parseRating(str: String): Rating = {
        val fields = str.split(",")
        Rating(fields(0).toLong, fields(1).toInt, fields(2).toInt, 1)
    }
    // $example off$

    val index = 9

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("ALSExample")
            .master("local[*]")
            .getOrCreate()
        import spark.implicits._

        // $example on$
        val ratings = spark.read.textFile(s"/Users/Maigo/work/SparkCFR/src/main/resources/train$index.txt")
            .map(parseRating)
            .toDF()
//        val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

        // Build the recommendation model using ALS on the training data
        val als = new ALS()
            .setMaxIter(5)
            .setRegParam(0.01)
            .setUserCol("sessionId")
            .setItemCol("itemId")
            .setRatingCol("rating")
        val model = als.fit(ratings)

        // Evaluate the model by computing the RMSE on the test data
        // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
        model.setColdStartStrategy("drop")
//        val predictions = model.transform(test)

//        val evaluator = new RegressionEvaluator()
//            .setMetricName("rmse")
//            .setLabelCol("rating")
//            .setPredictionCol("prediction")
//        val rmse = evaluator.evaluate(predictions)
//        println(s"Root-mean-square error = $rmse")

        // Generate top 10 movie recommendations for each user
//        val userRecs = model.recommendForAllUsers(10)
        // Generate top 10 user recommendations for each movie
//        val movieRecs = model.recommendForAllItems(10)

        // Generate top 10 movie recommendations for a specified set of users
        val users = spark.read.textFile(s"/Users/Maigo/work/SparkCFR/src/main/resources/test$index.txt").map(l => l.split(",").head).toDF("sessionId")
        val userSubsetRecs = model.recommendForUserSubset(users, 20)
        // Generate top 10 user recommendations for a specified set of movies
//        val movies = ratings.select(als.getItemCol).distinct().limit(3)
//        val movieSubSetRecs = model.recommendForItemSubset(movies, 10)
        // $example off$
//        userRecs.show()
//        movieRecs.show()
        userSubsetRecs.map(r => s"${r.get(0)},${r.get(1)}")
        userSubsetRecs.show()

        val source = Source.fromFile(s"/Users/Maigo/work/SparkCFR/src/main/resources/testSecondHalf$index.txt")
        val sessionIdToFirstHalf = source.getLines().map(l => l.split(",")).map(t => (t.head.toInt, t.tail)).toMap

        userSubsetRecs.printSchema
        val sessionIds: List[Int] = userSubsetRecs.select("sessionId").collectAsList().map(a => a.get(0).asInstanceOf[Int]).toList
        val recs = userSubsetRecs.withColumn("recommendationIds", col("recommendations.itemId")).select("recommendationIds").collectAsList().map(a => a.getSeq(0).mkString(","))


        val predictionWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(s"/Users/Maigo/work/SparkCFR/src/main/resources/prediction$index.txt")))
        sessionIds.zip(recs).foreach(t => predictionWriter.write(s"${t._2};${sessionIdToFirstHalf(t._1).mkString(",")}\n"))

        predictionWriter.flush()
        predictionWriter.close()

        spark.stop()
    }
}
// scalastyle:on println
