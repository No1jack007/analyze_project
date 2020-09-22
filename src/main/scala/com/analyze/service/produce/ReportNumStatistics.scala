package com.analyze.service.produce

import java.sql.{PreparedStatement, ResultSet}
import java.util.UUID

import cn.hutool.core.date
import com.analyze.bean.Holiday
import com.analyze.service.produce.ProduceAnalyze.sdf
import com.analyze.service.produce.ProduceSaleAnalyze.{dealData, filterDataProduce}
import com.analyze.util.{CheckUtil, DatabasePool, DateUtil}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object ReportNumStatistics {

  def main(args: Array[String]): Unit = {

    var master = "local[*]"
    var pathProduce = "D:\\0-program\\test\\analyze\\sys_veh_produce"
    var pathSale = "D:\\0-program\\test\\analyze\\sys_veh_sale"
    var departId = ""
    var databaseConf = "D:\\0-program\\work\\idea\\analyze_project\\src\\main\\resources\\db.properties"

    if (args.length > 0) {
      master = args(0)
      pathProduce = args(1)
      pathSale = args(2)
      databaseConf = args(3)
      departId = args(4)
    }

    val holiday = DateUtil.getHoliday(databaseConf)

    var reminderTime = 5;

    val dbp = DatabasePool.getInstance(databaseConf)
    val con = dbp.getConnection
    val ps = con.prepareStatement("select reminder_time from mail_config")
    val mailConfig = ps.executeQuery
    while (mailConfig.next) {
      reminderTime = Integer.parseInt(mailConfig.getString("reminder_time"))
    }
    ps.execute()
    ps.close()

    val cleanDate = date.DateUtil.now

    val conf = new SparkConf().setMaster(master).setAppName("reportNumStatistics")
    val sc = new SparkContext(conf)

    val produceData = sc.textFile(pathProduce)
    val produceData1 = produceData.map(x => dealData(x))
    val allNumProduce = produceData1.count()
    val produceData2 = produceData1.map(x => getDataProduce(x))
    val produceData3 = produceData2.filter(x => filterDataProduce(x, cleanDate.substring(0, 10), reminderTime, holiday))

    val saleData = sc.textFile(pathSale)
    val saleData1 = saleData.map(x => dealData(x))
    val allNumSale = saleData1.count()
    val saleData2 = saleData1.map(x => getDataSale(x))
    val saleData3 = saleData2.filter(x => filterDataSale(x, cleanDate.substring(0, 10), reminderTime, holiday))

    val result = produceData3.union(saleData3)
    val result2 = result.repartition(10)

    result2.foreachPartition(partition => {
      val dbp = DatabasePool.getInstance(databaseConf)

      val con = dbp.getConnection
      val cleanSql = "delete from reminded_vehicle where create_time < '" + cleanDate + "'"
      val ps = con.prepareStatement(cleanSql)
      ps.execute()
      ps.close()

      partition.foreach(x => {
        val id = UUID.randomUUID.toString
        val vin = x(0)
        val dataType = x(3)
        val sql = "insert into reminded_vehicle values('" + id + "','" + date.DateUtil.now + "','" + vin + "','" + dataType + "','" + departId + "')"
        println(sql)
        val ps = con.prepareStatement(sql)
        ps.execute()
        ps.close()
      })
      con.close()
    })

    val ps1 = con.prepareStatement("delete from government_report_num where type='2'")
    ps1.execute()
    ps1.close()
    val ps2 = con.prepareStatement("insert into government_report_num values('" + UUID.randomUUID.toString + "','" + date.DateUtil.now + "',' ',' '," + allNumProduce + "," + allNumSale + ",'2')")
    ps2.execute()
    ps2.close()

    sc.stop()
  }

  def dealData(x: String): (Array[String]) = {
    x.replaceAll("\"", "").split("\t")
  }

  def getDataProduce(x: Array[String]): (Array[String]) = {
    val result: Array[String] = new Array[String](5)
    result(0) = x(0)
    result(1) = x(4).substring(0, 10)
    result(2) = x(15)
    result(3) = "1"
    result(4) = x(12)
    result
  }


  def filterDataProduce(x: Array[String], now: String, reminderTime: Int, holiday: Holiday): (Boolean) = {
    if ("0".equals(x(4)) && CheckUtil.checkDate_1(x(1))) {
      val day = DateUtil.countWorkDay(x(1), now, holiday)
      if (day > reminderTime) {
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  def getDataSale(x: Array[String]): (Array[String]) = {
    val result: Array[String] = new Array[String](5)
    result(0) = x(0)
    result(1) = x(1).substring(0, 10)
    result(2) = x(14)
    result(3) = "2"
    result(4) = x(11)
    result
  }

  def filterDataSale(x: Array[String], now: String, reminderTime: Int, holiday: Holiday): (Boolean) = {
    if ("0".equals(x(4)) && CheckUtil.checkDate_1(x(1))) {
      val day = DateUtil.countWorkDay(x(1), now, holiday)
      if (day > reminderTime) {
        true
      } else {
        false
      }
    } else {
      false
    }
  }


}
