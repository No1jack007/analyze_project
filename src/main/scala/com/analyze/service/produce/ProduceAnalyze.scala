package com.analyze.service.produce

import java.text.SimpleDateFormat
import java.util.UUID

import cn.hutool.core.date
import com.analyze.bean.Holiday
import com.analyze.util.{CheckUtil, DatabasePool, DateUtil}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object ProduceAnalyze {

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def main(args: Array[String]): Unit = {

    var master = "local[*]"
    var path = "D:\\0-program\\test\\analyze\\sys_veh_produce"
    var departId = ""
    var databaseConf = "D:\\0-program\\work\\idea\\analyze_project\\src\\main\\resources\\db.properties"

    if (args.length > 0) {
      master = args(0)
      path = args(1)
      databaseConf = args(2)
      departId = args(3)
    }

    val holiday = DateUtil.getHoliday(databaseConf)

    val conf = new SparkConf().setMaster(master).setAppName("produceAnalyze")
    val sc = new SparkContext(conf)

    val produceData = sc.textFile(path)
    val produceData1 = produceData.map(dealData(_))
    val produceData2 = produceData1.filter(filterData(_))
    val produceData3 = produceData2.map(getData(_))
    val produceData4 = produceData3.map(reduceData(_))
    val produceData5 = produceData4.reduceByKey(_ ++= _)
    val produceData6 = produceData5.map(x => {
      val result = new ListBuffer[Int]()
      var fifteen = 0
      var sixTeen = 0
      var thirtyOne = 0
      var fortySix = 0
      var sixty = 0
      x._2.foreach(y => {
        val day = DateUtil.countWorkDay(x._1, y(2), holiday)
        if (day <= 15) {
          fifteen = fifteen + 1
        } else if (day > 15 && day <= 30) {
          sixTeen = sixTeen + 1
        } else if (day > 30 && day <= 45) {
          thirtyOne = thirtyOne + 1
        } else if (day > 45 && day < 60) {
          fortySix = fortySix + 1
        } else if (day > 60) {
          sixty = sixty + 1
        }
      })
      result.append(fifteen)
      result.append(sixTeen)
      result.append(thirtyOne)
      result.append(fortySix)
      result.append(sixty)
      (x._1, result)
    })
    val produceData7 = produceData6.repartition(10)

    val result = produceData7

    val cleanDate = date.DateUtil.now

    result.foreachPartition(partition => {

      @transient val dbp = DatabasePool.getInstance(databaseConf)

      val con = dbp.getConnection
      val cleanSql = "delete from analyze_report_produce where create_time < '" + cleanDate + "'"
      val ps = con.prepareStatement(cleanSql)
      ps.execute()
      ps.close()

      partition.foreach(x => {

        val id = UUID.randomUUID.toString
        val fifteen = x._2(0)
        val sixTeen = x._2(1)
        val thirtyOne = x._2(2)
        val fortySix = x._2(3)
        val sixty = x._2(4)
        val notOnSchedule = sixTeen + thirtyOne + fortySix + sixty

        val sql = "insert into analyze_report_produce values('" + id + "','" + date.DateUtil.now + "','" + x._1 + "','" + fifteen + "','" + notOnSchedule + "','" + fifteen + "','" + sixTeen + "','" + thirtyOne + "','" + fortySix + "','" + sixty + "','" + departId + "')"
        println(sql)
        val ps = con.prepareStatement(sql)
        ps.execute()
        ps.close()
      })
      con.close()
    })

    sc.stop()
  }

  def dealData(x: String): (Array[String]) = {
    x.replaceAll("\"", "").split("\t")
  }

  def filterData(x: Array[String]): (Boolean) = {
    if ("1".equals(x(12)) && CheckUtil.checkTime_1(x(13)) && CheckUtil.checkTime_1(x(4))) {
      true
    } else {
      false
    }
  }

  def getData(x: Array[String]): (Array[String]) = {
    val result: Array[String] = new Array[String](4)
    result(0) = x(0)
    result(1) = x(4).substring(0, 10)
    result(2) = x(13).substring(0, 10)
    result(3) = x(15)
    result
  }

  def reduceData(x: Array[String]): (String, ListBuffer[Array[String]]) = {
    val result = new ListBuffer[Array[String]]()
    result.append(x)
    (x(1), result)
  }

  def count(start: String, end: String, holiday: Holiday): Int = {
    var result = 0
    try {
      val startDate = sdf.parse(start)
      val endDate = sdf.parse(end)
      while (startDate.compareTo(endDate) < 0) {
        if (startDate.getDay != 0 && startDate.getDay != 6) {
          result = result + 1
          if (holiday.getHolidays.indexOf(start) > -1) {
            result = result - 1
          }
        }
        else if (holiday.getWorkdays.indexOf(start) > -1) {
          result = result + 1
        }
        startDate.setDate(startDate.getDay + 1)
      }
      return result;
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    return result;

  }

}
