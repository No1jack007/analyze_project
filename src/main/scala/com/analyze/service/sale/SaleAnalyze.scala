package com.analyze.service.sale

import java.util.UUID

import cn.hutool.core.date
import com.analyze.util.{CheckUtil, DatabasePool, DateUtil}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object SaleAnalyze {

  def main(args: Array[String]): Unit = {

    var master = "local[*]"
    var path = "D:\\0-program\\test\\analyze\\sys_veh_sale"
    var departId = ""
    var databaseConf = "D:\\0-program\\work\\idea\\analyze_project\\src\\main\\resources\\db.properties"

    if (args.length > 0) {
      master = args(0)
      path = args(1)
      databaseConf = args(2)
      departId = args(3)
    }

    val holiday = DateUtil.getHoliday(databaseConf)

    val conf = new SparkConf().setMaster(master).setAppName("saleAnalyze")
    val sc = new SparkContext(conf)

    val saleData = sc.textFile(path)
    val saleData1 = saleData.map(dealData(_))
    val saleData2 = saleData1.filter(filterData(_))
    val saleData3 = saleData2.map(getData(_))
    val saleData4 = saleData3.map(reduceData(_))
    val saleData5 = saleData4.reduceByKey(_ ++= _)
    val saleData6 = saleData5.map(x => {
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
    val saleData7 = saleData6.repartition(10)

    val result = saleData7

    val cleanDate = date.DateUtil.now;

    result.foreachPartition(partition => {
      @transient val dbp = DatabasePool.getInstance(databaseConf)

      val con = dbp.getConnection
      val cleanSql = "delete from analyze_report_sale where create_time < '" + cleanDate + "'"
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

        val sql = "insert into analyze_report_sale values('" + id + "','" + date.DateUtil.now + "','" + x._1 + "','" + v1 + "','" + notOnSchedule + "','" + v1 + "','" + v2 + "','" + v3 + "','" + v4 + "','" + v5 + "','" + departId + "')"
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
    if ("1".equals(x(11)) && CheckUtil.checkTime_1(x(12)) && CheckUtil.checkDate_1(x(4))) {
      true
    } else {
      false
    }
  }

  def getData(x: Array[String]): (Array[String]) = {
    val result: Array[String] = new Array[String](4)
    result(0) = x(0)
    result(1) = x(4)
    result(2) = x(12).substring(0, 10)
    result(3) = x(14)
    result
  }

  def reduceData(x: Array[String]): (String, ListBuffer[Array[String]]) = {
    val result = new ListBuffer[Array[String]]()
    result.append(x)
    (x(1), result)
  }

}
