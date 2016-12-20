import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Wiki_sql {
	//val inputlinks = "/Users/jamesziemba/Documents/CS290/Wikipedia/input/links-simple-sorted.txt"
	//val titles = "/Users/jamesziemba/Documents/CS290/Wikipedia/input/titles-sorted.txt"
    //val inputlinks  = "/home/ec2-user/links-simple-sorted.txt"
    //val titles  = "/home/ec2-user/titles-sorted.txt"
    //val outputDir = "/home/ec2-user/"
    //val SPARK_MASTER = "spark://ec2-52-34-150-139.us-west-2.compute.amazonaws.com:7077"
    //val HDFS = "hdfs://ec2-52-34-150-139.us-west-2.compute.amazonaws.com:9000"

def main(args: Array[String]) {
        val conf = new SparkConf()
            .setMaster("local[*]")
            //.setMaster(SPARK_MASTER)
            .setAppName("Wiki_sql")
 
        val sc = new SparkContext(conf)

        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 
        val data = sc
            .textFile("/Users/jamesziemba/Documents/CS290/Wikipedia/input/titles-sorted.txt",8)



		//Tuple with (title, index of title)
		val title_name_tuples = data.zipWithIndex()

		//Matching indexes of titles and links text files
		val title_tuples = title_name_tuples
			.map(a => (a._2.toString,"one"))

		val links = sc
            .textFile("/Users/jamesziemba/Documents/CS290/Wikipedia/input/links-simple-sorted.txt", 8)
            .map(line => line.split(":")(0))
            .map(a => (a,"one"))

        //Only keep titles that don't have outlinks
        val combo = (title_tuples union links)
        	.reduceByKey((a,b) => "zero")
        	//.collect().foreach(println)

        //val schemaString1 = "Index IfOutlink"
        val schema1 = StructType(
        		StructField("Index", StringType, false)::
        		StructField("IfOutlink", StringType, false) :: Nil)

        val rowRDD1 = combo
        	.map(p => Row(p._1.toString,p._2))
        	//.collect().foreach(println)

        val dfNoOutlinks = sqlContext.createDataFrame(rowRDD1, schema1)

        dfNoOutlinks.registerTempTable("no_outlinks")

        //val index_with_no_outlinks = sqlContext.sql("SELECT Index,IfOutlink FROM no_outlinks WHERE IfOutlink = 'one'")

        val titles_indexed = data.zipWithIndex()
        	//.collect().foreach(println)

        val schemaString2 = "Title Index"

        val schema2 = StructType(
        		StructField("Title", StringType, false)::
        		StructField("Index", StringType, false) :: Nil)
        
        val rowRDD2 = titles_indexed
        	.map(p => Row(p._1,p._2.toString))
        	//.collect().foreach(println)

        val dfTitles = sqlContext.createDataFrame(rowRDD2, schema2)

        dfTitles.registerTempTable("titles")

        val titles_with_no_outlinks = sqlContext.sql("SELECT titles.Title FROM no_outlinks,titles WHERE (no_outlinks.IfOutlink = 'one' AND titles.Index = no_outlinks.Index) GROUP BY titles.Title")

        titles_with_no_outlinks.explain(true)
        titles_with_no_outlinks.map(t => (t(0))).collect()
        val answer2 = titles_with_no_outlinks.foreach(println)
        	
        val links2 = sc
            .textFile("/Users/jamesziemba/Documents/CS290/Wikipedia/input/links-simple-sorted.txt", 8)
            .map(line => line.split(":")(1))
            .flatMap(x => x.split(" "))
            .map(a => (a.toString,"one"))

        //then do the same map-reduce as in part 2a to connect to their titles
        val combo2 = (title_tuples union links2)
        	.reduceByKey((a,b) => "zero")
        	//.collect().foreach(println)

        val schemaString3 = "Index IfInlink"
        val schema3 = StructType(
        		StructField("Index", StringType, false)::
        		StructField("IfInlink", StringType, false) :: Nil)
        
        val rowRDD3 = combo2
        	.map(p => Row(p._1.toString,p._2))
        	//.collect().foreach(println)
        
        val dfNoInlinks = sqlContext.createDataFrame(rowRDD3, schema3)

        dfNoInlinks.registerTempTable("no_inlinks")



        val titles_with_no_inlinks = sqlContext.sql("SELECT titles.Title FROM no_inlinks,titles WHERE (no_inlinks.IfInlink = 'one' AND titles.Index = no_inlinks.Index) GROUP BY titles.Title")
        titles_with_no_inlinks.explain(true)
        titles_with_no_inlinks.map(t => (t(0))).collect()
        val answer3 = titles_with_no_inlinks.foreach(println)
		
		}
	}



