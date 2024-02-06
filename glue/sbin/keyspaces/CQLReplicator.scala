/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */
// Target Amazon Keyspaces

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
import org.apache.spark.sql.cassandra._
import scala.collection.mutable.ListBuffer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import java.time.Duration
import java.util.Optional
import org.joda.time.LocalDateTime

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{ObjectMetadata, DeleteObjectsRequest, ObjectListing, S3ObjectSummary}
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.amazonaws.ClientConfiguration
import com.amazonaws.retry.RetryPolicy

import com.datastax.spark.connector.cql._
import com.datastax.spark.connector._
import com.datastax.oss.driver.api.core.NoNodeAvailableException
import com.datastax.oss.driver.api.core.AllNodesFailedException
import com.datastax.oss.driver.api.core.servererrors._

import io.github.resilience4j.retry.{Retry, RetryConfig}
import io.github.resilience4j.core.IntervalFunction

import scala.util.{Try, Success, Failure}
import scala.collection.mutable.StringBuilder
import scala.io.Source

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

import java.util.Base64
import java.nio.charset.StandardCharsets

import net.jpountz.lz4.{LZ4Factory, LZ4Compressor}

class LargeObjectException(s: String) extends RuntimeException {
  println(s)
}

class ProcessTypeException(s: String) extends RuntimeException {
  println(s)
}

class CassandraTypeException(s: String) extends RuntimeException {
  println(s)
}

class StatsS3Exception(s: String) extends RuntimeException {
  println(s)
}

sealed trait Stats

case class DiscoveryStats(tile: Int, primaryKeys: Long, updatedTimestamp: String) extends Stats

case class ReplicationStats(tile: Int, primaryKeys: Long, updatedPrimaryKeys: Long, insertedPrimaryKeys: Long, deletedPrimaryKeys: Long, updatedTimestamp: String) extends Stats

class SupportFunctions() {
  def correctEmptyBinJsonValues(cols: List[String], input: String): String = {
    implicit val formats = DefaultFormats

    if (cols.isEmpty) {
      input
    } else {
      val json = parse(input)

      def replace(json: JValue, cols: Seq[String]): JValue = cols match {
        case Nil => json
        case col :: tail =>
          val updatedJson = (json \ col) match {
            case JString("") => json transformField {
              case JField(`col`, _) => JField(col, JString("0x"))
            }
            case _ => json
          }
          replace(updatedJson, tail)
      }

      val finalJson = replace(json, cols)
      compact(render(finalJson))
    }
  }
}

object GlueApp {
  def main(sysArgs: Array[String]) {

    def readReplicationStatsObject(s3Client: com.amazonaws.services.s3.AmazonS3, bucket: String, key: String): ReplicationStats = {
      Try {
        val s3Object = s3Client.getObject(bucket, key)
        val src = Source.fromInputStream(s3Object.getObjectContent())
        val json = src.getLines.mkString
        src.close()
        json
      } match {
        case Failure(_) => {
          ReplicationStats(0, 0, 0, 0, 0, LocalDateTime.now().toString)
        }
        case Success(json) => {
          implicit val formats = DefaultFormats
          parse(json).extract[ReplicationStats]
        }
      }
    }

    def shuffleDf(df: DataFrame): DataFrame = {
      val encoder = RowEncoder(df.schema)
      df.mapPartitions(new scala.util.Random().shuffle(_))(encoder)
    }

    def shuffleDfV2(df: DataFrame): DataFrame = {
      df.orderBy(rand())
    }

    def customConnectionFactory(sc: SparkContext): (CassandraConnector, CassandraConnector) = {
      val connectorToClusterSrc = CassandraConnector(sc.getConf.set("spark.cassandra.connection.config.profile.path", "KeyspacesConnector.conf"))
      val connectorToClusterTrg = CassandraConnector(sc.getConf.set("spark.cassandra.connection.config.profile.path", "CassandraConnector.conf"))
      (connectorToClusterSrc, connectorToClusterTrg)
    }

    def inferKeys(cc: CassandraConnector, keyType: String, ks: String, tbl: String, columnTs: String): Seq[Map[String, String]] = {
      val meta = cc.openSession.getMetadata.getKeyspace(ks).get.getTable(tbl).get
      keyType match {
        case "partitionKeys" => {
          meta.getPartitionKey.asScala.map(x => Map(x.getName().toString -> x.getType().toString.toLowerCase))
        }
        case "primaryKeys" => {
          meta.getPrimaryKey.asScala.map(x => Map(x.getName().toString -> x.getType().toString.toLowerCase))
        }
        case "primaryKeysWithTS" => {
          meta.getPrimaryKey.asScala.map(x => Map(x.getName().toString -> x.getType().toString.toLowerCase)) :+ Map(s"writetime($columnTs) as ts" -> "bigint")
        }
        case _ => {
          meta.getPrimaryKey.asScala.map(x => Map(x.getName().toString -> x.getType().toString.toLowerCase))
        }
      }
    }

    def getAllColumns(cc: CassandraConnector, ks: String, tbl: String): Seq[scala.collection.immutable.Map[String, String]] = {
      cc.withSessionDo {
        session =>
          session.getMetadata.getKeyspace(ks).get().
            getTable(tbl).get().
            getColumns.entrySet.asScala.
            map(x => Map(x.getKey.toString -> x.getValue.getType.toString)).toSeq
      }
    }

    def parseJSONConfig(i: String): org.json4s.JValue = i match {
      case "None" => JObject(List(("None", JString("None"))))
      case str =>
        try {
          parse(str)
        } catch {
          case _: Throwable => JObject(List(("None", JString("None"))))
        }
    }

    def preFlightCheck(connection: CassandraConnector, keyspace: String, table: String, dir: String): Unit = {
      val logger = new GlueLogger
      Try {
        val c1 = Option(connection.openSession)
        c1.isEmpty match {
          case false => {
            c1.get.getMetadata.getKeyspace(keyspace).isPresent match {
              case true => {
                c1.get.getMetadata.getKeyspace(keyspace).get.getTable(table).isPresent match {
                  case true => {
                    logger.info(s"the $dir table $table exists")
                  }
                  case false => {
                    val err = s"ERROR: the $dir table $table does not exist"
                    logger.error(err)
                    sys.exit(-1)
                  }
                }
              }
              case false => {
                val err = s"ERROR: the $dir keyspace $keyspace does not exist"
                logger.error(err)
                sys.exit(-1)
              }
            }
          }
          case _ => {
            val err = s"ERROR: The job was not able to connecto to the $dir"
            logger.error(err)
            sys.exit(-1)
          }
        }
      } match {
        case Failure(_) => {
          val err = s"ERROR: Detected connectivity issue. Check the reference conf file/Glue connection for the $dir, the job is aborted"
          logger.error(err)
          sys.exit(-1)
        }
        case Success(_) => {
          logger.info(s"Connected to the $dir")
        }
      }
    }

    val sparkContext: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sparkContext)
    val sparkSession: SparkSession = glueContext.getSparkSession
    val sparkConf: SparkConf = sparkContext.getConf
    val logger = new GlueLogger
    import sparkSession.implicits._

    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "TILE", "TOTAL_TILES", "PROCESS_TYPE", "SOURCE_KS", "SOURCE_TBL", "TARGET_KS", "TARGET_TBL", "WRITETIME_COLUMN", "TTL_COLUMN", "S3_LANDING_ZONE", "OFFLOAD_LARGE_OBJECTS", "REPLICATION_POINT_IN_TIME", "SAFE_MODE").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    val jobRunId = args("JOB_RUN_ID")
    val currentTile = args("TILE").toInt
    val totalTiles = args("TOTAL_TILES").toInt
    val safeMode = args("SAFE_MODE")
    // Internal configuration
    val WAIT_TIME = safeMode match {
      case "true" => 25000
      case _ => 0
    }
    val cachingMode = safeMode match {
      case "true" => StorageLevel.DISK_ONLY
      case _ => StorageLevel.MEMORY_AND_DISK_SER
    }
    val MAX_RETRY_ATTEMPTS = 256
    // Unit ms
    val EXP_BACKOFF = 25
    sparkSession.conf.set(s"spark.sql.catalog.ledgerCatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
    sparkSession.conf.set(s"spark.sql.catalog.sourceCluster", "com.datastax.spark.connector.datasource.CassandraCatalog")
    sparkSession.conf.set(s"spark.sql.catalog.ledgerCatalog.spark.cassandra.connection.config.profile.path", "KeyspacesConnector.conf")
    sparkSession.conf.set(s"spark.sql.catalog.sourceCluster.spark.cassandra.connection.config.profile.path", "CassandraConnector.conf")

    val ledgerTable = "ledger"
    val ledgerKeyspaces = "migration"
    val processType = args("PROCESS_TYPE") // discovery or replication
    val interanlLedger = s"ledgerCatalog.$ledgerKeyspaces.$ledgerTable"
    val patternForSingleQuotes = "(.*text.*)|(.*date.*)|(.*timestamp.*)|(.*inet.*)".r

    // Business configuration
    val srcTableName = args("SOURCE_TBL")
    val srcKeyspaceName = args("SOURCE_KS")
    val trgTableName = args("TARGET_TBL")
    val trgKeyspaceName = args("TARGET_KS")
    val customConnections = customConnectionFactory(sparkContext)
    val cassandraConn = customConnections._2
    val keyspacesConn = customConnections._1
    val landingZone = args("S3_LANDING_ZONE")
    val bcktName = landingZone.replaceAll("s3://", "")
    val columnTs = args("WRITETIME_COLUMN")
    val source = s"sourceCluster.$srcKeyspaceName.$srcTableName"
    val ttlColumn = args("TTL_COLUMN")
    val olo = args("OFFLOAD_LARGE_OBJECTS")
    val replicationPointInTime = args("REPLICATION_POINT_IN_TIME").toLong
    val defaultPartitions = scala.math.max(2, (sparkContext.defaultParallelism / 2 - 2))

    //AmazonS3Client to check if a stop request was issued
    val s3ClientConf = new ClientConfiguration().withRetryPolicy(RetryPolicy.builder().withMaxErrorRetry(5).build())
    val s3client = AmazonS3ClientBuilder.standard().withClientConfiguration(s3ClientConf).build()

    // Let's do preflight checks
    logger.info("Preflight check started")
    preFlightCheck(cassandraConn, srcKeyspaceName, srcTableName, "source")
    preFlightCheck(keyspacesConn, trgKeyspaceName, trgTableName, "target")
    logger.info("Preflight check completed")

    val selectStmtWithTTL = ttlColumn match {
      case "None" => ""
      case _ => {
        val tmpMeta = cassandraConn.openSession.getMetadata.getKeyspace(srcKeyspaceName).get.getTable(srcTableName).get
        val lstColumns = tmpMeta.getColumns.asScala.map(x => x._1.toString).toSeq :+ s"ttl($ttlColumn)"
        lstColumns.mkString(",")
      }
    }

    val pkFinal = columnTs match {
      case "None" => inferKeys(cassandraConn, "primaryKeys", srcKeyspaceName, srcTableName, columnTs).flatten.toMap.keys.toSeq
      case _ => inferKeys(cassandraConn, "primaryKeysWithTS", srcKeyspaceName, srcTableName, columnTs).flatten.toMap.keys.toSeq
    }

    val pkFinalWithoutTs = pkFinal.filterNot(_ == s"writetime($columnTs) as ts")
    val pks = pkFinal.filterNot(_ == s"writetime($columnTs) as ts")
    val cond = pks.map(x => col(s"head.$x") === col(s"tail.$x")).reduce(_ && _)
    val columns = inferKeys(cassandraConn, "primaryKeys", srcKeyspaceName, srcTableName, columnTs).flatten.toMap
    val columnsPos = scala.collection.immutable.TreeSet(columns.keys.toArray: _*).zipWithIndex
    val offloadLargeObjTmp = new String(Base64.getDecoder().decode(olo.replaceAll("\\r\\n|\\r|\\n", "")), StandardCharsets.UTF_8)
    logger.info(s"Offload large objects to S3 bucket: $offloadLargeObjTmp")
    val offloadLargeObjects = parseJSONConfig(offloadLargeObjTmp)

    val allColumnsFromSource = getAllColumns(cassandraConn, srcKeyspaceName, srcTableName)
    val blobColumns: List[String] = allColumnsFromSource.flatMap(_.filter(_._2 == "BLOB").keys).toList

    def compressWithLZ4(input: String): String = {
      val lz4Factory = LZ4Factory.fastestInstance()
      val compressor: LZ4Compressor = lz4Factory.fastCompressor()
      val inputBytes = input.getBytes("UTF-8")
      val maxCompressedLength: Int = compressor.maxCompressedLength(inputBytes.length)
      val compressedOutput = new Array[Byte](maxCompressedLength)
      val compressedLength: Int = compressor.compress(inputBytes, 0, inputBytes.length, compressedOutput, 0, maxCompressedLength)
      val compressedBytes = compressedOutput.slice(0, compressedLength)
      Base64.getEncoder.encodeToString(compressedBytes)
    }

    def stopRequested(bucket: String): Boolean = {
      val key = processType match {
        case "discovery" => s"$srcKeyspaceName/$srcTableName/$processType/stopRequested"
        case "replication" => s"$srcKeyspaceName/$srcTableName/$processType/$currentTile/stopRequested"
        case _ => throw new ProcessTypeException("Unrecognizable process type")
      }
      Try {
        val s3Object: S3Object = s3client.getObject(bucket, key)
        s3client.deleteObject(bucket, key)
        logger.info(s"Requested a stop for $key process")
      } match {
        case Failure(_) => false
        case Success(_) => true
      }
    }

    def removeLeadingEndingSlashes(str: String): String = {
      str.
        stripPrefix("/").
        stripSuffix("/")
    }

    def removeS3prefixIfPresent(str: String): String = {
      str.stripPrefix("s3://")
    }

    def offloadToS3(jPayload: org.json4s.JValue, s3ClientOnPartition: com.amazonaws.services.s3.AmazonS3): JValue = {
      val columnName = (offloadLargeObjects \ "column").values.toString
      val bucket = removeS3prefixIfPresent((offloadLargeObjects \ "bucket").values.toString)
      val prefix = removeLeadingEndingSlashes((offloadLargeObjects \ "prefix").values.toString)
      val xrefColumnName = (offloadLargeObjects \ "xref").values.toString
      val key = java.util.UUID.randomUUID.toString
      val largeObject = compressWithLZ4((jPayload \ columnName).values.toString)
      val jsonStatement = jPayload transformField {
        // Replaced s"s3://$bucket/$prefix/$key" by s"$key"
        case JField(`xrefColumnName`, _) => JField(xrefColumnName, org.json4s.JsonAST.JString(s"$key"))
      }
      // Remove orginal payload from the target statement
      val updatedJsonStatement = jsonStatement removeField {
        case JField(`columnName`, _) => true
        case _ => false
      }

      Try {
        s3ClientOnPartition.putObject(bucket, s"$prefix/$key", largeObject)
      } match {
        case Failure(_) => throw new LargeObjectException("Not able to persist the large object to S3")
        case Success(_) => updatedJsonStatement
      }
    }

    def putStats(bucket: String, key: String, objectName: String, stats: Stats): Unit = {
      implicit val formats = DefaultFormats
      val (newContent, message) = stats match {
        case ds: DiscoveryStats =>
          (write(ds), s"Flushing the discovery stats: $key/$objectName")
        case rs: ReplicationStats =>
          val content = readReplicationStatsObject(s3client, bucket, s"$key/$objectName")
          val insertedAggr = content.insertedPrimaryKeys + rs.insertedPrimaryKeys
          val updatedAggr = content.updatedPrimaryKeys + rs.updatedPrimaryKeys
          val deletedAggr = content.deletedPrimaryKeys + rs.deletedPrimaryKeys
          val historicallyInserted = content.primaryKeys + rs.primaryKeys
          (write(ReplicationStats(currentTile,
            historicallyInserted,
            updatedAggr,
            insertedAggr,
            deletedAggr,
            LocalDateTime.now().toString)),
            s"Flushing the replication stats: $key/$objectName")
        case _ => throw new StatsS3Exception("Unknown stats type")
      }
      Try {
        s3client.putObject(bucket, s"$key/$objectName", newContent)
      } match {
        case Failure(_) => throw new StatsS3Exception(s"Can't persist the stats to the S3 bucket $bucket")
        case Success(_) => logger.info(message)
      }
    }

    def getTTLvalue(jvalue: org.json4s.JValue): BigInt = {
      val jvalueByKey = jvalue \ s"ttl($ttlColumn)"
      jvalueByKey match {
        case JInt(values) => values
        case _ => 0
      }
    }

    def backToCQLStatementWithoutTTL(jvalue: org.json4s.JValue): String = {
      val res = jvalue filterField (p => (p._1 != s"ttl($ttlColumn)"))
      val jsonNew = JObject(res)
      compact(render(jsonNew))
    }

    def persistToTarget(df: DataFrame, columns: scala.collection.immutable.Map[String, String], columnsPos: scala.collection.immutable.SortedSet[(String, Int)], tile: Int, op: String): Unit = {
      df.rdd.foreachPartition(
        partition => {
          val supportFunctions = new SupportFunctions()
          val retryConfig = RetryConfig.custom.maxAttempts(MAX_RETRY_ATTEMPTS).
            intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofMillis(EXP_BACKOFF), 1.1)).
            retryExceptions(classOf[WriteFailureException], classOf[WriteTimeoutException], classOf[ServerError], classOf[UnavailableException], classOf[NoNodeAvailableException], classOf[AllNodesFailedException]).build()
          val retry = Retry.of("keyspaces", retryConfig)
          val s3ClientOnPartition: com.amazonaws.services.s3.AmazonS3 = offloadLargeObjects match {
            case JObject(List(("None", JString("None")))) => null
            case _ => AmazonS3ClientBuilder.defaultClient()
          }
          /* This section only for debugging
          val publisher = retry.getEventPublisher
          publisher.onRetry(event => logger.info(s"Operation was retried on event $event"))
          // onError you can offload failed rows to Amazon S3/SQS for the further processing
          publisher.onError(event => logger.info(s"Operation was failed on event $event"))
          */
          partition.foreach(
            row => {
              val whereClause = rowToStatement(row, columns, columnsPos)
              if (!whereClause.isEmpty) {
                if (op == "insert" || op == "update") {
                  cassandraConn.withSessionDo { session => {
                    if (ttlColumn.equals("None")) {
                      val rs = Option(session.execute(s"SELECT json * FROM $srcKeyspaceName.$srcTableName WHERE $whereClause").one())
                      if (!rs.isEmpty) {
                        val jsonRow = supportFunctions.correctEmptyBinJsonValues(blobColumns, rs.get.getString(0).replace("'", "\\\\u0027"))
                        keyspacesConn.withSessionDo {
                          session => {
                            offloadLargeObjects match {
                              case JObject(List(("None", JString("None")))) => Retry.decorateSupplier(retry,
                                () => session.execute(s"INSERT INTO $trgKeyspaceName.$trgTableName JSON '$jsonRow'")).get()
                              case _ => {
                                val json4sRow = parse(jsonRow)
                                val updatedJsonRow = compact(render(offloadToS3(json4sRow, s3ClientOnPartition)))
                                Retry.decorateSupplier(retry,
                                  () => session.execute(s"INSERT INTO $trgKeyspaceName.$trgTableName JSON '$updatedJsonRow'")).get()
                              }
                            }
                          }
                        }
                      }
                    }
                    else {
                      val rs = Option(session.execute(s"SELECT json $selectStmtWithTTL FROM $srcKeyspaceName.$srcTableName WHERE $whereClause").one())
                      if (!rs.isEmpty) {
                        val jsonRow = supportFunctions.correctEmptyBinJsonValues(blobColumns, rs.get.getString(0).replace("'", "\\\\u0027"))
                        val json4sRow = parse(jsonRow)
                        keyspacesConn.withSessionDo {
                          session => {
                            offloadLargeObjects match {
                              case JObject(List(("None", JString("None")))) => {
                                val backToJsonRow = backToCQLStatementWithoutTTL(json4sRow)
                                val ttlVal = getTTLvalue(json4sRow)
                                Retry.decorateSupplier(retry, () => session.execute(s"INSERT INTO $trgKeyspaceName.$trgTableName JSON '$backToJsonRow' USING TTL $ttlVal")).get()
                              }
                              case _ => {
                                val json4sRow = parse(jsonRow)
                                val updatedJsonRow = offloadToS3(json4sRow, s3ClientOnPartition)
                                val backToJsonRow = backToCQLStatementWithoutTTL(updatedJsonRow)
                                val ttlVal = getTTLvalue(json4sRow)
                                Retry.decorateSupplier(retry, () => session.execute(s"INSERT INTO $trgKeyspaceName.$trgTableName JSON '$backToJsonRow' USING TTL $ttlVal")).get()
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                  }
                }
                if (op == "delete") {
                  keyspacesConn.withSessionDo {
                    session => {
                      Retry.decorateSupplier(retry,
                        () => session.execute(s"DELETE FROM $trgKeyspaceName.$trgTableName WHERE $whereClause")).get()
                    }
                  }
                }
              }
            }
          )
        }
      )
    }

    def listWithSingleQuotes(lst: java.util.List[String], colType: String): String = {
      colType match {
        case patternForSingleQuotes(_*) => lst.asScala.toList.map(c => s"'$c'").mkString("[", ",", "]")
        case _ => lst.asScala.toList.map(c => s"$c").mkString("[", ",", "]")
      }
    }

    def rowToStatement(row: Row, columns: scala.collection.immutable.Map[String, String], columnsPos: scala.collection.immutable.SortedSet[(String, Int)]): String = {
      val whereStmt = new StringBuilder
      columnsPos.foreach { el =>
        val colName = el._1
        val position = row.fieldIndex(el._1)
        val colType: String = columns.getOrElse(el._1, "none")
        val v = colType match {
          // inet is string
          case "string" | "text" | "inet" => s"'${row.getString(position)}'"
          case "date" => s"'${row.getDate(position)}'"
          case "timestamp" => s"'${row.getTimestamp(position)}'"
          case "int" => row.getInt(position)
          case "long" | "bigint" => row.getLong(position)
          case "float" => row.getFloat(position)
          case "double" => row.getDouble(position)
          case "short" => row.getShort(position)
          case "decimal" => row.getDecimal(position)
          case "tinyint" => row.getByte(position)
          case "uuid" => row.getString(position)
          case "boolean" => row.getBoolean(position)
          case "blob" => s"0${lit(row.getAs[Array[Byte]](colName)).toString.toLowerCase.replaceAll("'", "")}"
          case colType if colType.startsWith("list") => listWithSingleQuotes(row.getList[String](position), colType)
          case _ => throw new CassandraTypeException(s"Unrecognized data type $colType")
        }
        whereStmt.append(s"$colName=$v")
        el._2 match {
          case i if i < columns.size - 1 => whereStmt.append(" and ")
          case _ =>
        }
      }
      whereStmt.toString
    }

    def dataReplicationProcess() {
      keyspacesConn.withSessionDo {
        session => {
          val ledger = session.execute(s"SELECT location,tile,ver FROM migration.ledger WHERE ks='$srcKeyspaceName' and tbl='$srcTableName' and tile=$currentTile and load_status='' and offload_status='SUCCESS' ALLOW FILTERING").all().asScala
          val ledgerList = Option(ledger)

          if (!ledgerList.isEmpty) {
            val locations = ledgerList.get.map(c => (c.getString(0), c.getInt(1), c.getString(2))).toList.par
            val heads = locations.filter(_._3 == "head").length
            val tails = locations.filter(_._3 == "tail").length

            if (heads > 0 && tails == 0) {

              logger.info(s"Historical data load.Processing locations: $locations")
              locations.foreach(location => {

                val loc = location._1
                val sourcePath = s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/$loc"
                val sourceDf = glueContext.getSourceWithFormat(
                  connectionType = "s3",
                  format = "parquet",
                  options = JsonOptions(s"""{"paths": ["$sourcePath"]}""")
                ).getDynamicFrame().toDF()

                val sourceDfV2 = sourceDf.drop("group").drop("ts")
                val tile = location._2

                persistToTarget(shuffleDfV2(sourceDfV2), columns, columnsPos, tile, "insert")
                session.execute(s"INSERT INTO migration.ledger(ks,tbl,tile,ver,load_status,dt_load, offload_status) VALUES('$srcKeyspaceName','$srcTableName',$tile,'head','SUCCESS', toTimestamp(now()), '')")
                val cnt = sourceDfV2.count()

                val content = ReplicationStats(tile, cnt, 0, 0, 0, LocalDateTime.now().toString)
                putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/replication/$tile", "count.json", content)

              }
              )
            }

            if ((heads > 0 && tails > 0) || (heads == 0 && tails > 0)) {
              var inserted: Long = 0
              var deleted: Long = 0
              var updated: Long = 0

              logger.info("Processing delta...")
              val pathTail = s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$currentTile.tail"
              val dfTail = glueContext.getSourceWithFormat(
                connectionType = "s3",
                format = "parquet",
                options = JsonOptions(s"""{"paths": ["$pathTail"]}""")
              ).getDynamicFrame().toDF().drop("group").persist(cachingMode)

              val pathHead = s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$currentTile.head"
              val dfHead = glueContext.getSourceWithFormat(
                connectionType = "s3",
                format = "parquet",
                options = JsonOptions(s"""{"paths": ["$pathHead"]}""")
              ).getDynamicFrame().toDF().drop("group").persist(cachingMode)

              val newInsertsDF = dfTail.drop("ts").as("tail").join(dfHead.drop("ts").as("head"), cond, "leftanti").persist(cachingMode)
              val newDeletesDF = dfHead.drop("ts").as("head").join(dfTail.drop("ts").as("tail"), cond, "leftanti").persist(cachingMode)

              columnTs match {
                case "None" => {
                  if (!newInsertsDF.isEmpty) {
                    persistToTarget(newInsertsDF, columns, columnsPos, currentTile, "insert")
                    inserted = newInsertsDF.count()
                  }
                }
                case _ => {
                  val newUpdatesDF = dfTail.as("tail").join(dfHead.as("head"), cond, "inner").
                    filter($"tail.ts" > $"head.ts").
                    selectExpr(pks.map(x => s"tail.$x"): _*).persist(cachingMode)
                  if (!(newInsertsDF.isEmpty && newUpdatesDF.isEmpty)) {
                    persistToTarget(newInsertsDF, columns, columnsPos, currentTile, "insert")
                    persistToTarget(newUpdatesDF, columns, columnsPos, currentTile, "update")
                    inserted = newInsertsDF.count()
                    updated = newUpdatesDF.count()
                  }
                  newUpdatesDF.unpersist()
                }
              }

              if (!newDeletesDF.isEmpty) {
                persistToTarget(newDeletesDF, columns, columnsPos, currentTile, "delete")
                deleted = newDeletesDF.count()
              }

              if (!(updated != 0 && inserted != 0 && deleted != 0)) {
                val content = ReplicationStats(currentTile, 0, updated, inserted, deleted, LocalDateTime.now().toString)
                putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/replication/$currentTile", "count.json", content)
              }

              newInsertsDF.unpersist()
              newDeletesDF.unpersist()
              dfTail.unpersist()
              dfHead.unpersist()

              session.execute(s"BEGIN UNLOGGED BATCH " +
                s"INSERT INTO migration.ledger(ks,tbl,tile,ver,load_status,dt_load, offload_status) VALUES('$srcKeyspaceName','$srcTableName',$currentTile,'tail','SUCCESS', toTimestamp(now()), '');" +
                s"INSERT INTO migration.ledger(ks,tbl,tile,ver,load_status,dt_load, offload_status) VALUES('$srcKeyspaceName','$srcTableName',$currentTile,'head','SUCCESS', toTimestamp(now()), '');" +
                s"APPLY BATCH;")

            }
          }
        }
          session.close()
      }
    }

    def keysDiscoveryProcess() {
      val primaryKeysDf = columnTs match {
        case "None" =>
          sparkSession.read.option("inferSchema", "true").
            table(source).
            selectExpr(pkFinal.map(c => c): _*).
            persist(cachingMode)
        case ts if ts != "None" && replicationPointInTime == 0 =>
          sparkSession.read.option("inferSchema", "true").
            table(source).
            selectExpr(pkFinal.map(c => c): _*).
            persist(cachingMode)
        case ts if ts != "None" && replicationPointInTime > 0 =>
          sparkSession.read.option("inferSchema", "true").
            table(source).
            selectExpr(pkFinal.map(c => c): _*).
            filter(($"ts" > replicationPointInTime) && ($"ts".isNotNull)).
            persist(cachingMode)
      }

      val groupedPkDF = primaryKeysDf.withColumn("group", abs(xxhash64(pkFinalWithoutTs.map(c => col(c)): _*)) % totalTiles).
        repartition(col("group")).persist(cachingMode)
      val tiles = (0 to totalTiles - 1).toList.par
      tiles.foreach(tile => {
        keyspacesConn.withSessionDo {
          session => {
            val rsTail = session.execute(s"SELECT * FROM migration.ledger WHERE ks='$srcKeyspaceName' and tbl='$srcTableName' and tile=$tile and ver='tail'").one()
            val rsHead = session.execute(s"SELECT * FROM migration.ledger WHERE ks='$srcKeyspaceName' and tbl='$srcTableName' and tile=$tile and ver='head'").one()

            val tail = Option(rsTail)
            val head = Option(rsHead)
            val tailLoadStatus = tail match {
              case t if !t.isEmpty => rsTail.getString("load_status")
              case _ => ""
            }
            val headLoadStatus = head match {
              case h if !h.isEmpty => rsHead.getString("load_status")
              case _ => ""
            }

            logger.info(s"Processing $tile, head is $head, tail is $tail, head status is $headLoadStatus, tail status is $tailLoadStatus")

            // Swap tail and head
            if ((!tail.isEmpty && tailLoadStatus == "SUCCESS") && (!head.isEmpty && headLoadStatus == "SUCCESS")) {
              logger.info("Swapping the tail and the head")

              val staged = groupedPkDF.where(col("group") === tile).repartition(defaultPartitions, pks.map(c => col(c)): _*)
              val oldTailPath = s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.tail"
              val oldTail = glueContext.getSourceWithFormat(
                connectionType = "s3",
                format = "parquet",
                options = JsonOptions(s"""{"paths": ["$oldTailPath"]}""")
              ).getDynamicFrame().toDF().repartition(defaultPartitions, pks.map(c => col(c)): _*)

              oldTail.write.mode("overwrite").save(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.head")
              staged.write.mode("overwrite").save(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.tail")

              session.execute(
                s"BEGIN UNLOGGED BATCH " +
                  s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location,ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.tail', 'tail','','');" +
                  s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location,ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.head', 'head','','');" +
                  s"APPLY BATCH;"
              )
            }

            // The second round (tail and head)
            if (tail.isEmpty && (!head.isEmpty && headLoadStatus == "SUCCESS")) {
              logger.info("Loading a tail but keeping the head")
              val staged = groupedPkDF.where(col("group") === tile).repartition(defaultPartitions, pks.map(c => col(c)): _*)
              staged.write.mode("overwrite").save(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.tail")
              session.execute(s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location, ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.tail', 'tail','','')")
            }

            // Historical upload, the first round (head)
            if (tail.isEmpty && head.isEmpty) {
              logger.info("Loading a head")
              val staged = groupedPkDF.where(col("group") === tile).repartition(defaultPartitions, pks.map(c => col(c)): _*)
              staged.write.mode("overwrite").save(s"$landingZone/$srcKeyspaceName/$srcTableName/primaryKeys/tile_$tile.head")
              session.execute(s"INSERT INTO migration.ledger(ks,tbl,tile,offload_status,dt_offload,location, ver, load_status, dt_load) VALUES('$srcKeyspaceName','$srcTableName',$tile, 'SUCCESS', toTimestamp(now()), 'tile_$tile.head', 'head','','')")
              val content = DiscoveryStats(tile, staged.count(), LocalDateTime.now().toString)
              putStats(landingZone.replaceAll("s3://", ""), s"$srcKeyspaceName/$srcTableName/stats/discovery/$tile", "count.json", content)
            }
          }
            session.close()
        }
      })
      groupedPkDF.unpersist()
      primaryKeysDf.unpersist()
    }

    Iterator.continually(stopRequested(bcktName)).takeWhile(_ == false).foreach {
      _ => {
        processType match {
          case "discovery" => {
            keysDiscoveryProcess
          }
          case "replication" => {
            dataReplicationProcess
          }
          case _ => {
            logger.info(s"Unrecognizable process type - $processType")
            sys.exit()
          }
        }
        logger.info(s"Cooldown period $WAIT_TIME ms")
        Thread.sleep(WAIT_TIME)
      }
    }
    logger.info(s"Stop was requested for the $processType process...")
    Job.commit()
  }
}