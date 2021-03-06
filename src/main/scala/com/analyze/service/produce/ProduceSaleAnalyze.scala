package com.analyze.service.produce

import java.util.{Calendar, Date, UUID}

import cn.hutool.core.date
import com.analyze.service.produce.ProduceAnalyze.dealData
import com.analyze.util.{CheckUtil, DatabasePool}
import org.apache.spark.{SparkConf, SparkContext}
import java.text.{DecimalFormat, NumberFormat}

import scala.collection.mutable.Map
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

    val month = Array("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
    val now=Calendar.getInstance().get(Calendar.YEAR)

    val conf = new SparkConf().setMaster(master).setAppName("produceSaleAnalyze")
    val sc = new SparkContext(conf)

    val produceData = sc.textFile(pathProduce)
    val produceData1 = produceData.map(x => dealData(x))
    val produceData2 = produceData1.filter(x => filterDataProduce(x))
    val produceData3 = produceData2.map(x => getData(x))
    val produceData4 = produceData3.map(x => getReduceData(x))
    val produceData5 = produceData4.reduceByKey((x, y) => reduceData(x, y))
    val produceData6 = produceData5.map(x => {
      (x._1.substring(0, 4), x._2)
    })
    val produceData7 = produceData6.reduceByKey((x, y) => x + y)
    val produceData8 = produceData5.map(x => {
      (x._1.substring(0, 4), x)
    })
    val produceData9 = produceData8.leftOuterJoin(produceData7)
    val produceData10 = produceData9.map(x => {
      (x._2._1._1, x._2._1._2, x._2._2)
    })
    val produceData11 = produceData10.map(x => {
      val allYearNum: Double = Integer.parseInt(x._3.get.toString)
      val now: Double = x._2
      val v1 = now / allYearNum * 100
      val produceProportion = Double.box(v1).formatted("%.2f")
      (x._1, x._2, produceProportion, "produce")
    })
    val produceData12 = produceData11.collect().toList
    val produceData13 = produceData12.map(x => {
      Integer.parseInt(x._1.substring(0, 4))
    })
    val produceData14 = produceData13.sorted
    val startProduce = produceData14.head
    val endProduce = now
    val dataProduce: Map[String, Map[String, String]] = Map[String, Map[String, String]]()
    produceData12.foreach(x => {
      val row = Map[String, String]()
      row.put("produce_num", x._2.toString)
      row.put("produce_proportion", x._3)
      dataProduce.put(x._1, row)
    })
    //    println(dataProduce)
    val dataProduceInsert: Map[String, Map[String, String]] = Map[String, Map[String, String]]()
    for (i <- startProduce to endProduce) {
      month.foreach(x => {
        val date = i + "-" + x
        val lastDate = (i - 1) + "-" + x
        if (dataProduce.contains(date) && dataProduce.contains(lastDate)) {
          val row: Map[String, String] = dataProduce.getOrElse(date, Map[String, String]())
          val rowLast: Map[String, String] = dataProduce.getOrElse(lastDate, Map[String, String]())
          val now: Double = Integer.parseInt(row.get("produce_num").get)
          val last: Double = Integer.parseInt(rowLast.get("produce_num").get)
          val v1 = (now - last) / last * 100
          val produceGrowthRate = Double.box(v1).formatted("%.2f")
          row.put("produce_growth_rate", produceGrowthRate)
          dataProduceInsert.put(date, row)
        } else if (dataProduce.contains(date) && !dataProduce.contains(lastDate)) {
          val row: Map[String, String] = dataProduce.getOrElse(date, Map[String, String]())
          row.put("produce_growth_rate", "--")
          dataProduceInsert.put(date, row)
        } else if (!dataProduce.contains(date) && dataProduce.contains(lastDate)) {
          val row = Map[String, String]()
          row.put("produce_num", "0")
          row.put("produce_proportion", "0")
          row.put("produce_growth_rate", "-100")
          dataProduceInsert.put(date, row)
        } else {
          val row = Map[String, String]()
          row.put("produce_num", "0")
          row.put("produce_proportion", "0")
          row.put("produce_growth_rate", "0")
          dataProduceInsert.put(date, row)
        }
      })
    }
    //    println(dataProduceInsert)

    val saleData = sc.textFile(pathSale)
    val saleData1 = saleData.map(x => dealData(x))
    val saleData2 = saleData1.filter(x => filterDataSale(x))
    val saleData3 = saleData2.map(x => getData(x))
    val saleData4 = saleData3.map(x => getReduceData(x))
    val saleData5 = saleData4.reduceByKey((x, y) => reduceData(x, y))
    val saleData6 = saleData5.map(x => {
      (x._1.substring(0, 4), x._2)
    })
    val saleData7 = saleData6.reduceByKey((x, y) => x + y)
    val saleData8 = saleData5.map(x => {
      (x._1.substring(0, 4), x)
    })
    val saleData9 = saleData8.leftOuterJoin(saleData7)
    val saleData10 = saleData9.map(x => {
      (x._2._1._1, x._2._1._2, x._2._2)
    })
    val saleData11 = saleData10.map(x => {
      val allYearNum: Double = Integer.parseInt(x._3.get.toString)
      val now: Double = x._2
      val v3 = now / allYearNum * 100
      val saleProportion = Double.box(v3).formatted("%.2f")
      (x._1, x._2, saleProportion, "sale")
    })
    val saleData12 = saleData11.collect().toList
    val saleData13 = saleData12.map(x => {
      Integer.parseInt(x._1.substring(0, 4))
    })
    val saleData14 = saleData13.sorted
    val startSale = saleData14.head
    val endSale = now
    val dataSale = Map[String, Map[String, String]]()
    saleData12.foreach(x => {
      val row = Map[String, String]()
      row.put("sale_num", x._2.toString)
      row.put("sale_proportion", x._3)
      dataSale.put(x._1, row)
    })
    //    println(dataSale)
    val dataSaleInsert: Map[String, Map[String, String]] = Map[String, Map[String, String]]()
    for (i <- startSale to endSale) {
      month.foreach(x => {
        val date = i + "-" + x
        val lastDate = (i - 1) + "-" + x
        if (dataSale.contains(date) && dataSale.contains(lastDate)) {
          val row = dataSale.getOrElse(date, Map[String, String]())
          val rowLast: Map[String, String] = dataSale.getOrElse(lastDate, Map[String, String]())
          val now: Double = Integer.parseInt(row.get("sale_num").get)
          val last: Double = Integer.parseInt(rowLast.get("sale_num").get)
          val v1 = (now - last) / last * 100
          val produceGrowthRate = Double.box(v1).formatted("%.2f")
          row.put("sale_growth_rate", produceGrowthRate)
          dataSaleInsert.put(date, row)
        } else if (dataSale.contains(date) && !dataSale.contains(lastDate)) {
          val row: Map[String, String] = dataSale.getOrElse(date, Map[String, String]())
          row.put("sale_growth_rate", "--")
          dataSaleInsert.put(date, row)
        } else if (!dataSale.contains(date) && dataSale.contains(lastDate)) {
          val row = Map[String, String]()
          row.put("sale_num", "0")
          row.put("sale_proportion", "0")
          row.put("sale_growth_rate", "-100")
          dataSaleInsert.put(date, row)
        } else {
          val row = Map[String, String]()
          row.put("sale_num", "0")
          row.put("sale_proportion", "0")
          row.put("sale_growth_rate", "0")
          dataSaleInsert.put(date, row)
        }
      })
    }
    //    println(dataSaleInsert)

    val dataInsert: Map[String, Map[String, String]] = Map[String, Map[String, String]]()

    dataProduceInsert.keys.foreach(x => {
      dataInsert.put(x, dataProduceInsert.get(x).get)
    })
    dataSaleInsert.keys.foreach(x => {
      if (dataInsert.contains(x)) {
        dataInsert.get(x).get.put("sale_num", dataSaleInsert.get(x).get.get("sale_num").get)
        dataInsert.get(x).get.put("sale_proportion", dataSaleInsert.get(x).get.get("sale_proportion").get)
        dataInsert.get(x).get.put("sale_growth_rate", dataSaleInsert.get(x).get.get("sale_growth_rate").get)
      } else {
        dataInsert.put(x, dataSaleInsert.get(x).get)
      }
    })

    val cleanDate = date.DateUtil.now
    @transient val dbp = DatabasePool.getInstance(databaseConf)
    val con = dbp.getConnection
    val cleanSql = "delete from analyze_produce_sale where create_time < '" + cleanDate + "'"
    val ps = con.prepareStatement(cleanSql)
    ps.execute()
    ps.close()
    dataInsert.keys.foreach(x => {
      val date = x
      val data=dataInsert.get(x).get
      val data1 = data.getOrElse("produce_num","0")
      val data2 = data.getOrElse("produce_proportion","0")
      val data3 = data.getOrElse("produce_growth_rate","0")
      val data4 = data.getOrElse("sale_num","0")
      val data5 = data.getOrElse("sale_proportion","0")
      val data6 = data.getOrElse("sale_growth_rate","0")
      val sql = "insert into analyze_produce_sale values('" + UUID.randomUUID.toString + "','" + cleanDate + "','" +date+ "','" + data1 + "','" + data2 + "','" + data3 + "','" + data4 + "','" + data5 + "','" + data6 + "','" + departId + "')"
      println(sql)
      val ps = con.prepareStatement(sql)
      ps.execute()
      ps.close()
    })
    con.close()

    sc.stop()

  }

  def dealData(x: String): (Array[String]) = {
    x.replaceAll("\"", "").split("\t")
  }

  def filterDataProduce(x: Array[String]): (Boolean) = {
    if (CheckUtil.checkTime_1(x(4))) {
      true
    } else {
      false
    }
  }

  def filterDataSale(x: Array[String]): (Boolean) = {
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
