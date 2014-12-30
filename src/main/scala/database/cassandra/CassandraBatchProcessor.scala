package database.cassandra

/**
 * Created by buremba on 13/05/14.
 */

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.lang.{Long => JLong, Number => JNumber, String => JString}
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone, UUID}

import com.datastax.driver.core.Cluster
import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.hadoop.cql3.{CqlConfigHelper, CqlOutputFormat, CqlPagingInputFormat}
import org.apache.cassandra.utils.ByteBufferUtil
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.rakam.analysis.query.{FieldScript, FilterScript}
import org.rakam.analysis.rule.aggregation.{AnalysisRule, AggregationRule, MetricAggregationRule, TimeSeriesAggregationRule}
import org.rakam.constant.AggregationType._
import org.rakam.constant.Analysis
import org.rakam.model.{Actor, Event}
import org.rakam.util.{ConversionUtil, Serializer}
import org.rakam.util.json.JsonObject

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object CassandraBatchProcessor {
  val sc = new SparkContext("local[2]", "CQLTestApp", System.getenv("SPARK_HOME"))

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

  private def createEventJob(start: Option[Long] = None, end: Option[Long] = None, outputTable: Option[String] = None, outputQuery: Option[String] = None): Job = {
    val hadoopEventJob = new Job()
    hadoopEventJob.setInputFormatClass(classOf[CqlPagingInputFormat])
    ConfigHelper.setInputInitialAddress(hadoopEventJob.getConfiguration(), cHost)
    ConfigHelper.setInputRpcPort(hadoopEventJob.getConfiguration(), cPort)
    ConfigHelper.setInputColumnFamily(hadoopEventJob.getConfiguration(), KeySpace, "event")
    ConfigHelper.setInputPartitioner(hadoopEventJob.getConfiguration(), "Murmur3Partitioner")
    CqlConfigHelper.setInputCQLPageRowSize(hadoopEventJob.getConfiguration(), "3")

    hadoopEventJob.setOutputFormatClass(classOf[CqlOutputFormat])
    if(outputTable.isDefined) {
      CqlConfigHelper.setOutputCql(hadoopEventJob.getConfiguration(), outputQuery.get)
      ConfigHelper.setOutputColumnFamily(hadoopEventJob.getConfiguration(), KeySpace, outputTable.get)
      ConfigHelper.setOutputInitialAddress(hadoopEventJob.getConfiguration(), cHost)
      ConfigHelper.setOutputRpcPort(hadoopEventJob.getConfiguration(), cPort)
      ConfigHelper.setOutputPartitioner(hadoopEventJob.getConfiguration(), "Murmur3Partitioner")
    }
    var where = ""
    if (start.isDefined) {
      where += " time > minTimeuuid('" + new SimpleDateFormat("yyyy-MM-DD HH:mm+SS00").format(new Date(start.get)) + "') "
    }
    if (end.isDefined)
      where += " time > maxTimeuuid('" + new SimpleDateFormat("yyyy-MM-DD HH:mm+SS00").format(new Date(end.get)) + "')"
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

  private def createActorRDD(job : Job): RDD[Actor] = {
    val rawActorRdd = sc.newAPIHadoopRDD(job.getConfiguration(),
      classOf[CqlPagingInputFormat], classOf[java.util.Map[String, ByteBuffer]], classOf[java.util.Map[String, ByteBuffer]])
    rawActorRdd.map({
      case (key, value) => {
        val prop = value.get("properties")
        if (prop == null)
          new Actor(ByteBufferUtil.string(key.get("tracker")), ByteBufferUtil.string(key.get("id")))
        else
          new Actor(ByteBufferUtil.string(key.get("tracker")), ByteBufferUtil.string(key.get("id")), ByteBufferUtil.string(prop))

      }
    })
  }

  private def joinActorProperties(eventRdd: RDD[Event]): RDD[Event] = {
    val actorRdd = createActorRDD(createActorJob()).map(a => (a.id, a.data))
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
              case value if value.isInstanceOf[JString] => event.data.putString("_user." + key, value.asInstanceOf[JString])
              case value if value.isInstanceOf[JNumber] => event.data.putNumber("_user." + key, value.asInstanceOf[JNumber])
              case _ => throw new IllegalStateException("couldn't find the type of actor property.")
            }

          })
        event
      }
    )
  }

  private def getLongFromEvent = (select: FieldScript[String]) => {
    (event: Event) => {
      ConversionUtil.toLong(select.extract(event.data, null), 0L).asInstanceOf[Long]
    }
  }

  private def containsKeyFromEvent = (select: FieldScript[String]) => {
    (event: Event) => select.contains(event.data, null)
  }

  private def getStringFromEvent(select: FieldScript[String]) = (event: Event) => select.extract(event.data, null)

  def processRule(rule: AnalysisRule) {
    this.processRule(rule, -1, -1)
  }

  def filterEvents(filter : FilterScript, limit : Int, sortBy : String) = {
    val eventJob = createEventJob()
    var eventRdd = createEventRDD(eventJob)
    eventRdd = eventRdd.filter(e => filter.test(e.data))
    if(sortBy != null)
      eventRdd.top(limit)(Ordering.by(e => e.data.getValue(sortBy)))
    else
      eventRdd.take(limit)
  }

  def filterActors(filter : FilterScript, limit : Int, sortBy : String = null) = {
    val actorJob = createActorJob()
    var actorRdd = createActorRDD(actorJob)
    actorRdd = actorRdd.filter(e => filter.test(e.data))
    if(sortBy != null)
      actorRdd.top(limit)(Ordering.by(e => e.data.getValue(sortBy)))
    else
      actorRdd.take(limit)
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

      val eventJob = createEventJob(Some(start_time), Some(start_time), Some(outputTable), Some(outputQuery))
      var eventRdd = createEventRDD(eventJob)
      val dbSession = Cluster.builder.addContactPoint("127.0.0.1").build.connect
      val set = dbSession.prepare("UPDATE " + KeySpace + ".aggregated_set SET value = ? where project = ? and id = ?")
      val counter = dbSession.prepare("UPDATE " + KeySpace + ".aggregated_counter SET value = ? where project = ? and id = ?")

      if ((ruleFilter != null && ruleFilter.requiresUser()) ||
        ruleSelect != null && ruleSelect.requiresUser() ||
        (ruleGroupBy != null && ruleGroupBy.requiresUser()))
        eventRdd = this.joinActorProperties(eventRdd)


      if (aggRule.filters != null)
        eventRdd = eventRdd.filter(e => ruleFilter.test(e.data))

      if (aggRule.isInstanceOf[MetricAggregationRule] && aggRule.groupBy == null) {

        aggRule.`type` match {
          case COUNT => {
            val count: JLong = eventRdd.count()
            dbSession.execute(counter.bind(count, rule.project, rule.id))
          }
          case COUNT_X => {
            val count: JLong = eventRdd.filter(containsKeyFromEvent(ruleSelect)).count()
            dbSession.execute(counter.bind(count, rule.project, rule.id))
          }
          case SUM_X => {
            val reduce: JLong = eventRdd.map(getLongFromEvent(ruleSelect)).reduce(_ + _)
            dbSession.execute(counter.bind(reduce, rule.project, rule.id))
          }
          case MINIMUM_X => {
            val reduce: JLong = eventRdd.map(getLongFromEvent(ruleSelect)).reduce(Math.min(_, _))
            dbSession.execute(counter.bind(reduce, rule.project, rule.id))
          }
          case MAXIMUM_X => {
            val reduce: JLong = eventRdd.map(getLongFromEvent(ruleSelect)).reduce(Math.max(_, _))
            dbSession.execute(counter.bind(reduce, rule.project, rule.id))
          }
          case AVERAGE_X => {
            val sum : JLong = eventRdd.map(getLongFromEvent(ruleSelect)).reduce(_ + _)
            val count : JLong= eventRdd.filter(containsKeyFromEvent(ruleSelect)).count()
            dbSession.execute(counter.bind(sum, rule.project, rule.id+":sum"))
            dbSession.execute(counter.bind(count, rule.project, rule.id+":count"))
          }
          case UNIQUE_X => {
            val collect: Array[String] = eventRdd.map(getStringFromEvent(ruleSelect)).distinct().collect
            dbSession.execute(set.bind(collect, rule.project, rule.id))
          }
        }

      } else {
        val groupByFunc = aggRule match {
          case rule if rule.isInstanceOf[MetricAggregationRule] => getStringFromEvent(rule.groupBy)
          case rule if rule.isInstanceOf[TimeSeriesAggregationRule] => {
            // first 1000 is to convert second to millisecond
            // the other 1000s are to convert UUID timestamp from microsecond to second
            val interval = rule.asInstanceOf[TimeSeriesAggregationRule].interval
            // it's better to use Int as group by key instead of Long because of performance issues.
            // since we do not need millisecond part of timestamp convert it to second
            // and the output format can be used as an Int
            if (rule.groupBy == null)
              (event: Event) => {
                interval.span(((event.id.timestamp() / 10000000) + epochMillis).toInt).current
              }
            else {
              val ruleGroupByFunc = getStringFromEvent(rule.groupBy)
              (event: Event) => (interval.span(((event.id.timestamp() / 10000000) + epochMillis).toInt).current(), ruleGroupByFunc(event))
            }
          }
          case _ => throw new IllegalStateException("couldn't recognize pre-aggregation aggRule")
        }

        val result: RDD[Any] = aggRule.`type` match {
          case COUNT => eventRdd.groupBy(groupByFunc).map(x => (x._1, x._2.size))
          case COUNT_X => eventRdd.filter(containsKeyFromEvent(ruleSelect)).groupBy(groupByFunc).map(x => (x._1, x._2.size))
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
            eventRdd.groupBy(groupByFunc).map(x => (x._1, x._2.map(getLongFromEvent(ruleSelect)).sum / x._2.size))
          }
          case UNIQUE_X => {
            eventRdd.filter(containsKeyFromEvent(ruleSelect)).groupBy(groupByFunc).map(x => (x._1, x._2.map(getStringFromEvent(ruleSelect)).stream.distinct)) // distinct.length for counting
          }
          case _ => {
            throw new IllegalStateException("unknown aggregation type")
          }
        }

        val ruleId = rule.id
        val output = result.map {
          case (timestamp: Integer, count: Number) => {
            val outKey: java.util.Map[String, ByteBuffer] = new java.util.HashMap()
            outKey.put("id", ByteBufferUtil.bytes(ruleId + ":" + timestamp))

            val outVal = new java.util.LinkedList[ByteBuffer]
            outVal.add(ByteBufferUtil.bytes(count.longValue()))

            (outKey, outVal)
          }
          case (timestampGroupBy: (Integer, String), items: ArrayBuffer[String]) => {
            val outKey: java.util.Map[JString, ByteBuffer] = new java.util.HashMap()
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
            val outKey: java.util.Map[JString, ByteBuffer] = new java.util.HashMap()
            outKey.put("id", ByteBufferUtil.bytes(ruleId + ":" + timestamp))

            val outVal = new java.util.LinkedList[ByteBuffer]
            outVal.add(Serializer.serialize(items.toList))
            (outKey, outVal)
          }
          case (timestampGroupBy: (Integer, String), items: ArrayBuffer[String]) => {
            val outKey: java.util.Map[JString, ByteBuffer] = new java.util.HashMap()
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

}
