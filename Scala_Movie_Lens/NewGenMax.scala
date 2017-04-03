import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io._
import java.io._


object NewGenCount {
  def main(args: Array[String]) {
    if (args.length != 2) {
			      System.err.println("Usage: Mean elapsed time <input path> <output path>");
			      System.exit(-1);
			    }
    val inputFile =args(0)
    val outputFile = args(1)
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Genere Count").setMaster("local[*]"))
    val initalData = sc.textFile(inputFile)
    
   //create RDD to store data only having gener part from whole data 
    val genData = initalData.map(element => element.split("::")(2))
    
   //create RDD to store data only having gener name separately part from gener  
    val splitGenData=genData.flatMap(line=>line.split('|'))
    
    //sort genere count according to value count
    val countRDD=splitGenData.map(word=>word).countByValue().toList.sortWith((x,y)=>x._2>y._2)
    
    //print dat to the file
     val pw = new PrintWriter(new FileOutputStream(new File(outputFile),true))
      for (line<-countRDD)
        {
          pw.println(line)
        }
       pw.close
  }
}