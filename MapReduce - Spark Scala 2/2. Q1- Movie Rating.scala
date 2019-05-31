package edu.wm.yzhao

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import java.io._

/** Find the movies with the most ratings. */
object Q1 {
  
  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("../ml-100k/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }    
     return movieNames
  }
 
  /** Our main function where the action happens */
  /** In u.data, define the fields as split by tab; define the 1st column as movieID, the 2nd column as rating **/
   //load data
    def parseLine(line: String) = {
      val fields = line.split("\t")
      val movieID = fields(1).toInt
      val rating = fields(2).toInt
      (movieID, rating)
    }

    //main code  
   def main(args: Array[String]) {
     
      // Set the log level to only print errors
      Logger.getLogger("org").setLevel(Level.ERROR)
      
      // Create a SparkContext using every core of the local machine
      val sc = new SparkContext("local[*]", "Q1")
     
      // Create a broadcast variable of our ID -> movie name map
      var nameDict = sc.broadcast(loadMovieNames)      
      
      // Load each line of the source data into an RDD
      val lines = sc.textFile("../ml-100k/u.data")
    
      // Use our parseLines function to convert to (movie, ratings) tuples
      val rdd = lines.map(parseLine)   
      
      //Define totalsByMovie: 
      //Map to (movieID, 1) tuples to count up all the 1's for each movie
      //Get the (movieID, (Σratings, Σcounts)) 
      val totalsByMovie = rdd.mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))  
      
      //Define the tuple as (movieID, (average ratings, counts))
      //Define the value = averagesByMovie as (average ratings, counts)
      //x._1.= Σratings, x._2 = Σcounts， average ratings = Σratings/Σcounts = x._1./x._2
      val averagesByMovie = totalsByMovie.mapValues(x => (x._1.toDouble / x._2,x._2))
      
      //Filter it by counts for at least 100 ratings
      //_.x._2._2 = counts 
      val filtermovie = averagesByMovie.filter(_.x._2._2>=100)
      
      
      //Import (movieID, (average ratings, counts): (x._1=movieID, (x._2._1=average ratings, x._2._2=counts)
      //Flip (movieID, (average ratings, counts)) to (Average ratings,(movieID, counts)
      val flip1 = filtermovie.map(x =>(x._2._1,(x._1,x._2._2)))
      
      //sort by key:average ratings to still have (Average ratings,(movieID, counts)
      val sortmovie = flip1.sortByKey()
      
      
      //Import (average ratings,(movieID, counts): (x._1=average ratings, (x._2._1=movieID, x._2._2=counts)
      //Fold in the movie names from the broadcast variable as((MovieID-->MovieName),(avg ratings, counts)
      val addname = sortmovie.map(x=>(nameDict.value(x._2._1) ,(x._1,x._2._2)))
      
      //Collect and print results
      val results = addname.collect()
      
      results.foreach(println)
      
      val file = "Q1.txt"    
      val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
      for (x <- results) {
          writer.write(x + "\n")
                      }
      writer.close()          
  }
    
}  
    
    
    