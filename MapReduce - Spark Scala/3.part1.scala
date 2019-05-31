
//team 10: Yuwei yao, Yuchen Zhao, Jiaxin Lu, Tanner Reichard
package edu.wm.jlu

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import java.io._


object part1{
	def parseLine(line: String) = {
		val fields = line.split(",")
		val CustID = fields(0).toInt
    val Amtspent = fields(2).toFloat
    (CustID, Amtspent)
    }
    
   def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "CustSpending")
    val lines = sc.textFile("../DataA1.csv")
    val rdd = lines.map(parseLine)
    val totalAmt=rdd.reduceByKey((x,y) => x+y)
    val sorted=totalAmt.sortByKey()
    
    val formated=sorted.map(x=>(x._1,"%.2f".format(x._2)))
    val results = formated.collect()
    
    val file = "Part1.txt"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (x <- results) {
        writer.write(x + "\n")
                    }
    writer.close()
  }
}

    
    
    
    
    
