import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.Encoders.scalaInt
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object Movies {
  def main(args: Array[String]) {
    hadoop()
    hive()
  }

  def hadoop() {
    val spark = SparkSession.builder()
      .appName("assignment-2 hadoop")
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
  def hive() {
    val spark = SparkSession.builder
      .master("local")
      .appName("assignment-2 hive")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()

    val source = "hdfs://localhost:9000/user/shreevari_sp/imdb-movies.csv"
    val moviesDF = spark.read
      .option("header", "true")
      .option("sep", "\t")
      .option("nullValue", "")
      .csv(source)

    // spark.sql("select * from task1").show()
    // spark.sql("select * from task2").show()
    // spark.sql("select * from task3").show()


    val parsedMoviesDF = moviesDF.withColumn("cleaned_budget", regexp_extract(col("budget"), "([0-9]+)", 1).as[Int](scalaInt))

    // spark.sql("drop table task1")
    // spark.sql("drop table task2")
    // spark.sql("drop table task3")
    
    parsedMoviesDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("movies")

    spark.sql("select year,  max_by(title, cleaned_budget) as title, max(cleaned_budget) as budget from movies group by year")
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("task1")

    spark.sql("select language, year, max(reviews_from_users + reviews_from_critics) as reviews from movies group by year, language")
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("task2")

    spark.sql("select year, max_by(title, votes) as title, max(votes) as votes from movies group by year")
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("task3")

    // spark.sql("drop table movies")

    spark.close()
  }
}
