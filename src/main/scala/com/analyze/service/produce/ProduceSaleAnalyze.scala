package com.analyze.service.produce

import java.util.UUID

import cn.hutool.core.date
import com.analyze.service.produce.ProduceAnalyze.dealData
import com.analyze.util.{CheckUtil, DatabasePool}
import org.apache.spark.{SparkConf, SparkContext}
import java.text.NumberFormat

import scala.collection.mutable.ListBuffer

object ProduceSaleAnalyze {

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

    val conf = new SparkConf().setMaster(master).setAppName("produceSaleAnalyze")
    val sc = new SparkContext(conf)

    val mapYearProduce: scala.collection.mutable.Map[String, Int] = scala.collection.mutable.HashMap()
    val broadCastYearProduce = sc.broadcast(mapYearProduce)
    val mapMonthProduce: scala.collection.mutable.Map[String, Int] = scala.collection.mutable.HashMap()
    val broadCastMonthProduce = sc.broadcast(mapMonthProduce)

    val produceData = sc.textFile(pathProduce)
    val produceData1 = produceData.map(x => dealData(x))
    val produceData2 = produceData1.filter(x => filterDataProduce(x))
    val produceData3 = produceData2.map(x => getData(x))
    val produceData4 = produceData3.map(x => getReduceData(x))
    val produceData5 = produceData4.reduceByKey((x, y) => reduceData(x, y))

    val resultProduce = produceData5.repartition(1)

    resultProduce.foreachPartition(partion => {
      partion.foreach(x => {
        val year = x._1.substring(0, 4)
        val num = broadCastYearProduce.value.getOrElse(year, 0)
        broadCastYearProduce.value += (year -> (num + x._2))
        broadCastMonthProduce.value += (x._1 -> x._2)
      })
    })

    val mapYearSale: scala.collection.mutable.Map[String, Int] = scala.collection.mutable.HashMap()
    val broadCastYearSale = sc.broadcast(mapYearSale)
    val mapMonthSale: scala.collection.mutable.Map[String, Int] = scala.collection.mutable.HashMap()
    val broadCastMonthSale = sc.broadcast(mapMonthSale)

    val saleData = sc.textFile(pathSale)
    val saleData1 = saleData.map(x => dealData(x))
    val saleData2 = saleData1.filter(x => filterDataSalae(x))
    val saleData3 = saleData2.map(x => getData(x))
    val saleData4 = saleData3.map(x => getReduceData(x))
    val saleData5 = saleData4.reduceByKey((x, y) => reduceData(x, y))

    val resultSale = saleData5.repartition(1)

    resultSale.foreachPartition(partion => {
      partion.foreach(x => {
        val year = x._1.substring(0, 4)
        val num = broadCastYearSale.value.getOrElse(year, 0)
        broadCastYearSale.value += (year -> (num + x._2))
        broadCastMonthSale.value += (x._1 -> x._2)
      })
    })

    val yearProduceMap = broadCastYearProduce.value
    val monthProduceMap = broadCastMonthProduce.value

    println("produce  year\t" + yearProduceMap)
    println("produce  month\t" + monthProduceMap)

    val yearSaleMap = broadCastYearSale.value
    val monthSaleMap = broadCastMonthSale.value

    println("sale  year\t" + yearSaleMap)
    println("sale  month\t" + monthSaleMap)

    val cleanDate = date.DateUtil.now
    val insertData: scala.collection.mutable.Map[String, Array[String]] = scala.collection.mutable.HashMap()
    for ((key, value) <- monthProduceMap) {
      val year = key.substring(0, 4)
      val allYearNum: Int = yearProduceMap.getOrElse(year, 0);

      var data: Array[String] = new Array[String](9)
      val id = UUID.randomUUID.toString
      data(0) = id
      data(1) = cleanDate
      data(2) = key
      data(3) = value.toString
      val numberFormat = NumberFormat.getInstance
      numberFormat.setMaximumFractionDigits(2)
      val produceProportion = numberFormat.format(value.asInstanceOf[Float] / allYearNum.asInstanceOf[Float] * 100)
      data(4) = produceProportion
      val lastYear = Integer.parseInt(year) - 1
      if (monthProduceMap.contains(lastYear + key.substring(4, 7))) {
        val last: Int = monthProduceMap.getOrElse(lastYear + key.substring(4, 7), 0)
        val now = value
        val margin = now - last
        val produceGrowthRate = numberFormat.format(margin.asInstanceOf[Float] / last.asInstanceOf[Float] * 100)
        data(5) = produceGrowthRate
      } else {
        data(5) = "0"
      }
      data(6) = "0"
      data(7) = "0"
      data(8) = "0"
      insertData.put(key, data)
    }

    for ((key, value) <- monthSaleMap) {
      val year = key.substring(0, 4)
      val allYearNum: Int = yearSaleMap.getOrElse(year, 0);

      val numberFormat = NumberFormat.getInstance
      numberFormat.setMaximumFractionDigits(2)
      val saleProportion = numberFormat.format(value.asInstanceOf[Float] / allYearNum.asInstanceOf[Float] * 100)

      val lastYear = Integer.parseInt(year) - 1
      var saleGrowthRate = "0"
      if (monthSaleMap.contains(lastYear + key.substring(4, 7))) {
        val last: Int = monthSaleMap.getOrElse(lastYear + key.substring(4, 7), 0)
        val now = value
        val margin = now - last
        saleGrowthRate = numberFormat.format(margin.asInstanceOf[Float] / last.asInstanceOf[Float] * 100)
      }

      if (insertData.contains(key)) {
        val data = insertData.getOrElse(key, null)
        data(6) = value.toString
        data(7) = saleProportion
        data(8) = saleGrowthRate
      } else {
        val data: Array[String] = new Array[String](9)
        val id = UUID.randomUUID.toString
        data(0) = id
        data(1) = cleanDate
        data(2) = key
        data(3) = "0"
        data(4) = "0"
        data(5) = "0"
        data(6) = value.toString
        data(7) = saleProportion
        data(8) = saleGrowthRate
        insertData.put(key, data)
      }
    }

    @transient val dbp = DatabasePool.getInstance(databaseConf)
    val con = dbp.getConnection
    val cleanSql = "delete from analyze_produce_sale where create_time < '" + cleanDate + "'"
    val ps = con.prepareStatement(cleanSql)
    ps.execute()
    ps.close()


    for ((key, value) <- insertData) {
      val sql = "insert into analyze_produce_sale values('" + value(0) + "','" + value(1) + "','" + value(2) + "'," + value(3) + "," + value(4) + "," + value(5) + "," + value(6) + "," + value(7) + "," + value(8) + ",'" + departId + "')"
      println(sql)
      val ps = con.prepareStatement(sql)
      ps.execute()
      ps.close()
    }

    con.close()

    sc.stop()
  }

  def dealData(x: String): (Array[String]) = {
    x.split("\t")
  }

  def filterDataProduce(x: Array[String]): (Boolean) = {
    if (CheckUtil.checkTime_1(x(4))) {
      true
    } else {
      false
    }
  }

  def filterDataSalae(x: Array[String]): (Boolean) = {
    if (CheckUtil.checkDate_1(x(4))) {
      true
    } else {
      false
    }
  }

  def getData(x: Array[String]): (String) = {
    x(4).substring(0, 7)
  }

  def getReduceData(x: String): (String, Int) = {
    (x, 1)
  }

  def reduceData(x: Int, y: Int): (Int) = {
    x + y
  }

}
