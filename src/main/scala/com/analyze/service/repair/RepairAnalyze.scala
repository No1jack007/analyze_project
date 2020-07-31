package com.analyze.service.repair

import java.util.UUID

import cn.hutool.core.date
import com.analyze.util.{CheckUtil, DatabasePool, DateUtil}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object RepairAnalyze {

  def main(args: Array[String]): Unit = {

    var master = "local[*]"
    var path = "D:\\0-program\\test\\analyze\\after_sale_repair_record"
    var departId = ""
    var databaseConf = "D:\\0-program\\work\\idea\\analyze_project\\src\\main\\resources\\db.properties"

    if (args.length > 0) {
      master = args(0)
      path = args(1)
      databaseConf = args(2)
      departId = args(3)
    }

    val holiday = DateUtil.getHoliday(databaseConf)

    val conf = new SparkConf().setMaster(master).setAppName("repairAnalyze")
    val sc = new SparkContext(conf)

    val repairData = sc.textFile(path)
    val repairData1 = repairData.map(dealData(_))
    val repairData2 = repairData1.filter(filterData(_))
    val repairData3 = repairData2.map(getData(_))
    val repairData4 = repairData3.map(reduceData(_))
    val repairData5 = repairData4.reduceByKey(_ ++= _)
    val repairData6 = repairData5.map(x => {
      val result = new ListBuffer[Int]()
      var v1 = 0
      var v2 = 0
      var v3 = 0
      var v4 = 0
      var v5 = 0
      x._2.foreach(y => {
        val day = DateUtil.countWorkDay(x._1, y(2), holiday)
        if (day <= 30) {
          v1 = v1 + 1
        } else if (day >= 31 && day <= 60) {
          v2 = v2 + 1
        } else if (day >= 61 && day <= 90) {
          v3 = v3 + 1
        } else if (day >= 91 && day <= 120) {
          v4 = v4 + 1
        } else if (day > 120) {
          v5 = v5 + 1
        }
      })
      result.append(v1)
      result.append(v2)
      result.append(v3)
      result.append(v4)
      result.append(v5)
      (x._1, result)
    })
    val repairData7 = repairData6.repartition(10)

    val result = repairData7

    val cleanDate = date.DateUtil.now;

    result.foreachPartition(partition => {
      @transient val dbp = DatabasePool.getInstance(databaseConf)

      val con = dbp.getConnection
      val cleanSql = "delete from analyze_report_repair where create_time < '" + cleanDate + "'"
      val ps = con.prepareStatement(cleanSql)
      ps.execute()
      ps.close()

      partition.foreach(x => {

        val id = UUID.randomUUID.toString
        val v1 = x._2(0)
        val v2 = x._2(1)
        val v3 = x._2(2)
        val v4 = x._2(3)
        val v5 = x._2(4)
        val notOnSchedule = v2 + v3 + v4 + v5

        val sql = "insert into analyze_report_repair values('" + id + "','" + date.DateUtil.now + "','" + x._1 + "','" + v1 + "','" + notOnSchedule + "','" + v1 + "','" + v2 + "','" + v3 + "','" + v4 + "','" + v5 + "','" + departId + "')"
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
    if ("1".equals(x(10)) && CheckUtil.checkTime_1(x(11)) && CheckUtil.checkDate_1(x(3))) {
      true
    } else {
      false
    }
  }

  def getData(x: Array[String]): (Array[String]) = {
    val result: Array[String] = new Array[String](4)
    result(0) = x(2)
    result(1) = x(3)
    result(2) = x(11).substring(0, 10)
    result(3) = x(13)
    result
  }

  def reduceData(x: Array[String]): (String, ListBuffer[Array[String]]) = {
    val result = new ListBuffer[Array[String]]()
    result.append(x)
    (x(1), result)
  }

}
