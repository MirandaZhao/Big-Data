package NAMEPACKAGE

// Note, this assumes input files are saved in workspace folder.

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import java.io._
import org.apache.spark.broadcast._
import org.apache.spark.sql._

object Part1 {
  
   def parseLine(line: String, movieMap: Broadcast[Map[Int, String]]) = {
      // Split by commas
      val fields = line.split("\t")
      // Extract the age and numFriends fields, and convert to integers
      val movieID = fields(1).toInt
      val rating = fields(2).toFloat
      // Create a tuple that is our result.
      (movieMap.value(movieID), rating)
  }
  
   def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("ml-100k/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }    
     return movieNames
  }
   
   
   
  def main(args: Array[String]) {
   
   val log = LogManager.getRootLogger
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Part1") //  with Serializable
    
    // Create a broadcast variable of our ID -> movie name map
    var nameDict = sc.broadcast(loadMovieNames)
    
    // Read in each rating line
    val lines = sc.textFile("ml-100k/u.data").collect()
    
    val rdd = lines.map(l => parseLine(l, nameDict))
	
	val movieTable = spark.createDataFrame(rdd).toDF("movie", "rating")
	movieTable.registerTempTable("movieTable")
	
	val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
	val result=sqlcontext.sql("select * from (select movie, avg(rating) as avg_rating from movieTable group by movie having movie in (select movie from (select movie, count(*) as count from  movieTable group by movie) where count >=100)) order by avg_rating").collect()
    val file = "Part1.csv"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
   
	writer.write("movie,avg rating\n")
    for (x <- result) {
      writer.write(x.toSeq.apply(0).toString + "," + x.toSeq.apply(1).toString + "\n")
   }
   writer.close()
  }
}