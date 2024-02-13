import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite
import com.amazonaws.services.timestreamwrite.model.Dimension
import com.amazonaws.services.timestreamwrite.model.Record
import com.amazonaws.services.timestreamwrite.model.WriteRecordsRequest
import com.amazonaws.services.timestreamwrite.model.WriteRecordsResult
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWriteClientBuilder
import collection.mutable._
import com.amazonaws.ClientConfiguration
import scala.util.{Try, Success, Failure}

class TimestreamException(s: String) extends RuntimeException {
  println(s)
}

object GlueApp {
  def main(sysArgs: Array[String]) {
      
    def getTimestreamConnection(): AmazonTimestreamWrite = {
      AmazonTimestreamWriteClientBuilder.standard().build()
    }
    val sparkContext: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sparkContext)
    val sparkSession: SparkSession = glueContext.getSparkSession
    val sparkConf: SparkConf = sparkContext.getConf
    val logger = new GlueLogger
    import sparkSession.implicits._

    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    val jobRunId = args("JOB_RUN_ID")

    val currentTime = System.currentTimeMillis()
    val recordTime = currentTime - 1 * 50

    val amazonTimestreamWrite:AmazonTimestreamWrite = getTimestreamConnection()

    Try {
      val preFlightCheckTimestreamConn = getTimestreamConnection()
    } match {
      case Failure(_) => throw new TimestreamException("[Timestream] Connection issue")
      case Success(_) => logger.info("[Timestream] Preflight check is completed")
    }
    var dimensions:java.util.List[Dimension] = Buffer().asJava
    dimensions.add(new Dimension().withName("Dim1").withValue("Value1"))

    var records:java.util.List[Record] = Buffer().asJava

    records.add(new Record().withDimensions(dimensions).withMeasureName("kilometers").withMeasureValue("15").withMeasureValueType("BIGINT").withTime(String.valueOf(recordTime)))

    val writeRecordsRequest = new WriteRecordsRequest()
      .withDatabaseName("sampleDB")
      .withTableName("test");
    writeRecordsRequest.setRecords(records);
    val writeRecordsResult = amazonTimestreamWrite.writeRecords(writeRecordsRequest)


    Job.commit()
  }
}