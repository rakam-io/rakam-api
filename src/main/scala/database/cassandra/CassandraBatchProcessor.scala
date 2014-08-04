package database.cassandra

/**
 * Created by buremba on 13/05/14.
 */

import java.nio.ByteBuffer
import org.apache.cassandra.hadoop.cql3.{CqlOutputFormat, CqlPagingInputFormat, CqlConfigHelper}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.vertx.java.core.json.JsonObject
import org.vertx.java.core.json.JsonArray
import java.util.{Date, TimeZone, Calendar, UUID}
import java.{util, lang}
import java.lang.Integer
import java.lang.String
import scala.collection.JavaConversions._
import org.rakam.constant.AggregationType._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.utils.ByteBufferUtil
import com.datastax.driver.core.Cluster
import org.rakam.constant.{Analysis, AggregationType}
import java.io.{ObjectOutputStream, ByteArrayOutputStream}
import org.rakam.model.Event
import org.rakam.model.Actor
import org.rakam.analysis.script.FieldScript
import org.rakam.analysis.rule.aggregation.{AnalysisRule, AggregationRule, MetricAggregationRule, TimeSeriesAggregationRule}
import org.rakam.cache.CacheAdapter
import org.rakam.database.{DatabaseAdapter, BatchProcessor}
import org.rakam.util.Serializer
import java.text.SimpleDateFormat
import org.rakam.ServiceStarter

object CassandraBatchProcessor {
  val sc = new SparkContext("local[2]", "CQLTestApp", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
  val db_session = Cluster.builder.addContactPoint("127.0.0.1").build.connect

  val cHost: String = "localhost"
  val cPort: String = "9160"
  val KeySpace = "analytics"

  /*
    TimeUUID stores timestamp values starting from 1982. We need to convert it to standard unix timestamp
   */
  val uuidEpoch: Calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
  uuidEpoch.clear
  uuidEpoch.set(1582, 9, 15, 0, 0, 0)
  val epochMillis: Int = (uuidEpoch.getTime.getTime / 1000).toInt

  private def createActorJob(): Job = {
    val hadoopActorJob = new Job()
    hadoopActorJob.setInputFormatClass(classOf[CqlPagingInputFormat])
    ConfigHelper.setInputInitialAddress(hadoopActorJob.getConfiguration(), cHost)
    ConfigHelper.setInputRpcPort(hadoopActorJob.getConfiguration(), cPort)
    ConfigHelper.setInputColumnFamily(hadoopActorJob.getConfiguration(), KeySpace, "actor")
    ConfigHelper.setInputPartitioner(hadoopActorJob.getConfiguration(), "Murmur3Partitioner")
    CqlConfigHelper.setInputCQLPageRowSize(hadoopActorJob.getConfiguration(), "3")
    hadoopActorJob
  }

  private def createEventJob(start: Long, end: Long, outputTable: String, outputQuery: String): Job = {
    val hadoopEventJob = new Job()
    hadoopEventJob.setInputFormatClass(classOf[CqlPagingInputFormat])
    ConfigHelper.setInputInitialAddress(hadoopEventJob.getConfiguration(), cHost)
    ConfigHelper.setInputRpcPort(hadoopEventJob.getConfiguration(), cPort)
    ConfigHelper.setInputColumnFamily(hadoopEventJob.getConfiguration(), KeySpace, "event")
    ConfigHelper.setInputPartitioner(hadoopEventJob.getConfiguration(), "Murmur3Partitioner")
    CqlConfigHelper.setInputCQLPageRowSize(hadoopEventJob.getConfiguration(), "3")

    hadoopEventJob.setOutputFormatClass(classOf[CqlOutputFormat])
    CqlConfigHelper.setOutputCql(hadoopEventJob.getConfiguration(), outputQuery)
    ConfigHelper.setOutputColumnFamily(hadoopEventJob.getConfiguration(), KeySpace, outputTable)
    ConfigHelper.setOutputInitialAddress(hadoopEventJob.getConfiguration(), cHost)
    ConfigHelper.setOutputRpcPort(hadoopEventJob.getConfiguration(), cPort)
    ConfigHelper.setOutputPartitioner(hadoopEventJob.getConfiguration(), "Murmur3Partitioner")
    var where = ""
    if (start >= 0) {
      where += " time > minTimeuuid('" + new SimpleDateFormat("yyyy-MM-DD HH:mm+SS00").format(new Date(start)) + "') "
    }
    if (end >= 0)
      where += " time > maxTimeuuid('" + new SimpleDateFormat("yyyy-MM-DD HH:mm+SS00").format(new Date(end)) + "')"
    if (!where.eq(""))
      CqlConfigHelper.setInputWhereClauses(hadoopEventJob.getConfiguration(), where)
    hadoopEventJob
  }

  private def createEventRDD(job: Job): RDD[Event] = {
    CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "3")

    val rawEventRdd = sc.newAPIHadoopRDD(job.getConfiguration(),
      classOf[CqlPagingInputFormat], classOf[java.util.Map[String, ByteBuffer]], classOf[java.util.Map[String, ByteBuffer]])
    rawEventRdd.map({
      case (key, value) => {
        try {
          val time = key.get("time")
          val raw_data = value.get("data")
          val data = if (raw_data != null) new JsonObject(ByteBufferUtil.string(raw_data)) else null
          val raw_actor = value.get("actor_id")
          val actor = if (raw_actor != null) ByteBufferUtil.string(raw_actor) else null
          new Event(new UUID(time.getLong(), time.getLong()), ByteBufferUtil.string(key.get("project")), actor, data)
        } catch {
          case e: Exception => {
            e.printStackTrace()
            null
          }

        }

      }
    })
  }

  private def createActorRDD(): RDD[Actor] = {
    val rawActorRdd = sc.newAPIHadoopRDD(createActorJob().getConfiguration(),
      classOf[CqlPagingInputFormat], classOf[java.util.Map[String, ByteBuffer]], classOf[java.util.Map[String, ByteBuffer]])
    rawActorRdd.map({
      case (key, value) => {
        val prop = value.get("properties")
        if (prop == null)
          new Actor(ByteBufferUtil.string(key.get("project")), ByteBufferUtil.string(key.get("id")))
        else
          new Actor(ByteBufferUtil.string(key.get("project")), ByteBufferUtil.string(key.get("id")), ByteBufferUtil.string(prop))

      }
    })
  }

  private def joinActorProperties(eventRdd: RDD[Event]): RDD[Event] = {
    val actorRdd = createActorRDD().map(a => (a.id, a.data))
    actorRdd.count()
    eventRdd.collect()
    eventRdd.map(e => (e.actor, e)).join(actorRdd).map(
      r => {
        val event = r._2._1
        val actor_props = r._2._2
        if (actor_props != null)
          actor_props.toMap.foreach(item => {
            val key = item._1
            item._2 match {
              case value if value.isInstanceOf[lang.String] => event.data.putString("_user." + key, value.asInstanceOf[lang.String])
              case value if value.isInstanceOf[lang.Number] => event.data.putNumber("_user." + key, value.asInstanceOf[lang.Number])
              case _ => throw new IllegalStateException("couldn't find the type of actor property.")
            }

          })
        event
      }
    )
  }

  private def getLongFromEvent = (select: FieldScript) => {
    (event: Event) => {
      try {
        lang.Long.parseLong(select.extract(event.data, null))
      } catch {
        case e: NumberFormatException => 0
      }
    }
  }

  private def containsKeyFromEvent = (select: FieldScript) => {
    (event: Event) => select.contains(event.data)
  }

  private def getStringFromEvent(select: FieldScript) = {
    (event: Event) => select.extract(event.data, null)
  }


  def processRule(rule: AnalysisRule) {
    this.processRule(rule, -1, -1);
  }

  def processRule(rule: AnalysisRule, start_time: Long, end_time: Long) {
    if (rule.analysisType() == Analysis.ANALYSIS_METRIC || rule.analysisType() == Analysis.ANALYSIS_TIMESERIES) {
      val aggRule = rule.asInstanceOf[AggregationRule]
      val ruleSelect = aggRule.select
      val ruleGroupBy = aggRule.groupBy
      val ruleFilter = aggRule.filters

      var outputQuery: String = null
      var outputTable: String = null
      if (rule.analysisType() == Analysis.ANALYSIS_METRIC || rule.analysisType() == Analysis.ANALYSIS_TIMESERIES) {
        val mrule = rule.asInstanceOf[AggregationRule]
        if (mrule.`type` != UNIQUE_X && mrule.`type` != UNIQUE_X) {
          outputQuery = "UPDATE " + KeySpace + ".aggregated_counter SET value = value + ?"
          outputTable = "aggregated_counter"
        }
      }
      if (outputTable == null) {
        outputQuery = "UPDATE " + KeySpace + ".aggregated_set SET value = ?"
        outputTable = "aggregated_set"
      }

      val eventJob = createEventJob(start_time, start_time, outputTable, outputQuery)
      var eventRdd = createEventRDD(eventJob)

      if ((ruleFilter != null && ruleFilter.requiresUser()) ||
        ruleSelect != null && ruleSelect.fieldKey.startsWith("_user.") ||
        (ruleGroupBy != null && ruleGroupBy.fieldKey.startsWith("_user.")))
        eventRdd = this.joinActorProperties(eventRdd)


      if (aggRule.filters != null)
        eventRdd = eventRdd.filter(e => ruleFilter.test(e.data, null))

      if (aggRule.isInstanceOf[MetricAggregationRule] && aggRule.groupBy == null) {
        val result: Any = aggRule.`type` match {
          case COUNT => eventRdd.count()
          case COUNT_X => eventRdd.filter(containsKeyFromEvent(ruleSelect)).count()
          case SUM_X => eventRdd.map(getLongFromEvent(ruleSelect)).reduce(_ + _)
          case MINIMUM_X => eventRdd.map(getLongFromEvent(ruleSelect)).reduce(Math.min(_, _))
          case MAXIMUM_X => eventRdd.map(getLongFromEvent(ruleSelect)).reduce(Math.max(_, _))
          //case COUNT_UNIQUE_X => eventRdd.map(getStringFromEvent(ruleSelect)).distinct().count()
          case AVERAGE_X => eventRdd.map(getLongFromEvent(ruleSelect)).mean()
          case UNIQUE_X => eventRdd.groupBy(getStringFromEvent(ruleGroupBy)).map(e => (e._1, e._2.map(getStringFromEvent(ruleSelect)).distinct)).collect()
          case _ => throw new IllegalStateException("unknown aggregation pre-aggregation aggRule type")
        }

        val serialized = result match {
          case result if result.isInstanceOf[Long] => ByteBufferUtil.bytes(result.asInstanceOf[Long])
          case result if result.isInstanceOf[Double] => ByteBufferUtil.bytes(result.asInstanceOf[Double])
          case result if result.isInstanceOf[RDD[Any]] => {
            val list: java.util.List[Object] = result.asInstanceOf[RDD[Object]].collect().toList
            ByteBufferUtil.bytes(new JsonArray(list).encode())
          }
          case _ => throw new IllegalStateException("couldn't serialize MetricAggregationRule result")
        }

        val session = Cluster.builder.addContactPoint("127.0.0.1").build.connect
        val query = session.prepare("UPDATE " + KeySpace + ".aggregated SET data = ? where project = ? and id = ? and timecabin = 0")
          .bind(serialized, aggRule.project, aggRule.id)
        session.execute(query)
      } else {
        val groupByFunc = aggRule match {
          case rule if rule.isInstanceOf[MetricAggregationRule] => getStringFromEvent(rule.groupBy)
          case rule if rule.isInstanceOf[TimeSeriesAggregationRule] => {
            // first 1000 is to convert second to millisecond
            // the other 1000s are to convert UUID timestamp from microsecond to second
            val interval = rule.asInstanceOf[TimeSeriesAggregationRule].interval.period
            // it's better to use Int as group by key instead of Long because of performance issues.
            // since we do not need millisecond part of timestamp convert it to second
            // and the output format can be used as an Int
            if (rule.groupBy == null)
              (event: Event) => {
                val a = ((((event.id.timestamp() / 10000000) + epochMillis).toInt / interval) * interval)
                a
              }
            else {
              val ruleGroupByFunc = getStringFromEvent(rule.groupBy)
              (event: Event) => (((((event.id.timestamp() / 10000000) + epochMillis).toInt / interval) * interval), ruleGroupByFunc(event))
            }
          }
          case _ => throw new IllegalStateException("couldn't recognize pre-aggregation aggRule")
        }

        val result: RDD[Any] = aggRule.`type` match {
          case COUNT => eventRdd.groupBy(groupByFunc).map(x => (x._1, x._2.length))
          case COUNT_X => eventRdd.filter(containsKeyFromEvent(ruleSelect)).groupBy(groupByFunc).map(x => (x._1, x._2.length))
          case SUM_X => eventRdd.groupBy(groupByFunc).map(x => (x._1, x._2.map(getLongFromEvent(ruleSelect)).reduce(_ + _)))
          case MINIMUM_X => eventRdd.groupBy(groupByFunc).map(x => (x._1, x._2.map(getLongFromEvent(ruleSelect)).reduce(Math.min(_, _))))
          case MAXIMUM_X => {
            eventRdd.groupBy(groupByFunc).map(x => (x._1, x._2.map(getLongFromEvent(ruleSelect)).reduce(Math.min(_, _))))
          }
          // it seems distinct for Seq which contains null values has a bug in < Scala 2.11.
          // since we use stable Scala 2.10 branch this code may return IllegalArgumentException
          // so the filter above will remain until we switch to 2.11 branch
          // see: https://issues.scala-lang.org/browse/SI-6908
          case AVERAGE_X => {
            eventRdd.groupBy(groupByFunc).map(x => (x._1, x._2.map(getLongFromEvent(ruleSelect)).sum / x._2.length))
          }
          case UNIQUE_X => {
            eventRdd.filter(containsKeyFromEvent(ruleSelect)).groupBy(groupByFunc).map(x => (x._1, x._2.map(getStringFromEvent(ruleSelect)).distinct)) // distinct.length for counting
          }
          case _ => {
            throw new IllegalStateException("unknown aggregation type")
          }
        }

        //result.foreach(x => println(x))

        val ruleId = aggRule.id
        val output = result.map {
          case (timestamp: Integer, count: Number) => {
            val outKey: java.util.Map[String, ByteBuffer] = new java.util.HashMap()
            outKey.put("id", ByteBufferUtil.bytes(ruleId + ":" + timestamp))

            val outVal = new java.util.LinkedList[ByteBuffer]
            outVal.add(ByteBufferUtil.bytes(count.longValue()))

            (outKey, outVal)
          }
          case (timestampGroupBy: (Integer, String), items: ArrayBuffer[String]) => {
            val outKey: java.util.Map[lang.String, ByteBuffer] = new java.util.HashMap()
            outKey.put("id", ByteBufferUtil.bytes(ruleId + ":" + timestampGroupBy._1 + ":" + timestampGroupBy._2))

            val outVal = new java.util.LinkedList[ByteBuffer]

            val data: java.util.Set[String] = items.toSet[String]
            val byteOut = new ByteArrayOutputStream()
            val out = new ObjectOutputStream(byteOut)
            out.writeObject(data)
            byteOut.close()
            outVal.add(ByteBuffer.wrap(byteOut.toByteArray))
            (outKey, outVal)
          }
          case (timestamp: Integer, items: ArrayBuffer[String]) => {
            val outKey: java.util.Map[lang.String, ByteBuffer] = new java.util.HashMap()
            outKey.put("id", ByteBufferUtil.bytes(ruleId + ":" + timestamp))

            val outVal = new java.util.LinkedList[ByteBuffer]
            outVal.add(Serializer.serialize(items.toList))
            (outKey, outVal)
          }
          case (timestampGroupBy: (Integer, String), items: ArrayBuffer[String]) => {
            val outKey: java.util.Map[lang.String, ByteBuffer] = new java.util.HashMap()
            outKey.put("id", ByteBufferUtil.bytes(ruleId + ":" + timestampGroupBy._1 + ":" + timestampGroupBy._2))

            Serializer.serialize(items)

            val outVal = new java.util.LinkedList[ByteBuffer]
            outVal.add(Serializer.serialize(items.toList))
            (outKey, outVal)
          }
          case (timestampGroupBy: (Integer, String), items: Number) => {
            val outKey: java.util.Map[String, ByteBuffer] = new java.util.HashMap()
            outKey.put("id", ByteBufferUtil.bytes(ruleId + ":" + timestampGroupBy._1 + ":" + timestampGroupBy._2))

            val outVal = new java.util.LinkedList[ByteBuffer]
            outVal.add(ByteBufferUtil.bytes(items.longValue()))
            (outKey, outVal)
          }
          case _ => throw new IllegalStateException("rdd type couldn't identified")
        }

        output.saveAsNewAPIHadoopFile(
          KeySpace,
          classOf[java.util.Map[String, ByteBuffer]],
          classOf[java.util.List[ByteBuffer]],
          classOf[CqlOutputFormat],
          eventJob.getConfiguration()
        )
      }
    }
  }


  db_session.execute("use analytics")
  val counter_sql = db_session.prepare("select value from aggregated_counter where id = ?")
  val set_sql = db_session.prepare("select value from aggregated_set where id = ?")

  private def getCounter(id: String): Long = {
    val row = db_session.execute(counter_sql.bind(id)).one()
    return if (row == null) 0 else row.getLong("value")
  }

  private def getSet(id: String): util.Set[lang.String] = {
    val row = db_session.execute(counter_sql.bind(id)).one()
    return if (row == null) null else row.getSet("value", classOf[lang.String])


  }

}
