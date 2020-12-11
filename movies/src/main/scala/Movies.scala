import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.Encoders.scalaInt
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object Movies {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("assignment-2")
      .getOrCreate()

    val source = "hdfs://localhost:9000/user/shreevari_sp/imdb-movies.csv"
    val moviesDF = spark.read
      .option("header", "true")
      .option("sep", "\t")
      .option("nullValue", "")
      .csv(source)


    val parsedMoviesDF = moviesDF.withColumn("cleaned_budget", regexp_extract(col("budget"), "([0-9]+)", 1).as[Int](scalaInt))

    parsedMoviesDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("movies")

    spark.sql("select year,  max_by(title, cleaned_budget), max(cleaned_budget) from movies group by year").show()

    spark.sql("select language, year, max(reviews_from_users + reviews_from_critics) from movies group by year, language").show()

    spark.sql("select year, max_by(title, votes), max(votes) from movies group by year").show()

    spark.sql("drop table movies")

    spark.close()
  }
}
