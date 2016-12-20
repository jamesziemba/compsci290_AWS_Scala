
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WH_sql {
	//val inputDir = "/Users/jamesziemba/Documents/CS290/WhiteHouse/input/whitehouse-waves-2013.csv"
    //val SPARK_MASTER = "spark://ec2-52-34-150-139.us-west-2.compute.amazonaws.com:7077"
    //val HDFS = "hdfs://ec2-52-34-150-139.us-west-2.compute.amazonaws.com:9000"
    val root_dir = "/Users/jamesziemba/Documents/CS290/WhiteHouse/output"

def main(args: Array[String]) {
        val conf = new SparkConf()
            .setMaster("local[*]")
            //.setMaster(SPARK_MASTER)
            .setAppName("WH_sql")
            .set("spark.eventLog.enabled","true")
            .set("spark.eventLog.dir",root_dir)
 
        val sc = new SparkContext(conf)

        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 
        val data = sc
            .textFile("/Users/jamesziemba/Documents/CS290/WhiteHouse/input/whitehouse-waves-2013.csv",8)

        val schemaString = "NAMELAST NAMEFIRST NAMEMID UIN BDGNBR ACCESS_TYPE TOA POA TOD POD APPT_MADE_DATE APPT_START_DATE APPT_END_DATE APPT_CANCEL_DATE Total_People LAST_UPDATEDBY POST LastEntryDate TERMINAL_SUFFIX visitee_namelast visitee_namefirst MEETING_LOC MEETING_ROOM CALLER_NAME_LAST	CALLER_NAME_FIRST CALLER_ROOM Description Release_Date"

        val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

        val rowRDD = data
        	.map(_.split(","))
        	.map(p => Row(p(0).trim,p(1).trim,p(2).trim,p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11),p(12),p(13),p(14),p(15),p(16),p(17),p(18),p(19),p(20),p(21),p(22),p(23),p(24),p(25),p(26)))
    
        val dfVisitors = sqlContext.createDataFrame(rowRDD, schema)

        dfVisitors.registerTempTable("visitors")

        val visitorNames = sqlContext.sql("SELECT NAMELAST,NAMEFIRST,NAMEMID,COUNT(*) AS counter FROM visitors GROUP BY NAMELAST,NAMEFIRST,NAMEMID ORDER BY counter DESC")

        visitorNames.explain(true)
        visitorNames.map(t => (t(0),t(1),t(2))).collect()
        //val answer1 = visitorNames.take(10).foreach(println)


        val visiteeNames = sqlContext.sql("SELECT visitee_namelast,visitee_namefirst,COUNT(*) AS counter FROM visitors GROUP BY visitee_namelast,visitee_namefirst ORDER BY counter DESC")

        visiteeNames.explain(true)
        visiteeNames.map(t => (t(0),t(1),t(2))).collect()
        //val answer2 = visiteeNames.take(10).foreach(println)

        val comboNames = sqlContext.sql("SELECT NAMELAST,NAMEFIRST,NAMEMID,visitee_namelast,visitee_namefirst,COUNT(*) AS counter FROM visitors GROUP BY NAMELAST,NAMEFIRST,NAMEMID,visitee_namelast,visitee_namefirst ORDER BY counter DESC")

        comboNames.explain(true)
        comboNames.map(t => (t(0),t(1),t(2))).collect()
        //val answer3 = comboNames.take(10).foreach(println)
        val df = sqlContext.read.json("/Users/jamesziemba/Documents/CS290/WhiteHouse/output/local-1448915652454")

        // Displays the content of the DataFrame to stdout
        df.show()
        }



    }











