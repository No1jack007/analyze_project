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
    produceData5.foreach(x => {
      val year = x._1.substring(0, 4)
      val num = broadCastYearProduce.value.getOrElse(year, 0)
      broadCastYearProduce.value += (year -> (num + x._2))
      broadCastMonthProduce.value += (x._1 -> x._2)
      println("broadCastMonthProduce\t" + broadCastMonthProduce.value)
    })
    val produceData6 = produceData5.map(x => {
      (x._1.substring(0, 4), x._2)
    })
    val produceData7 = produceData6.reduceByKey((x, y) => x + y)
    val produceData8 = produceData5.map(x => {
      (x._1.substring(0, 4), x)
    })
    val produceData9 = produceData8.join(produceData7)
    val produceData10 = produceData9.map(x => {
      (x._1, x._2._1._1, x._2._1._2, x._2._2)
    })
    val produceData11 = produceData10.map(x => {
      val year = x._1
      val allYearNum: Int = x._4;
      val numberFormat = NumberFormat.getInstance
      numberFormat.setMaximumFractionDigits(2)
      val produceProportion = numberFormat.format(x._3.asInstanceOf[Float] / allYearNum.asInstanceOf[Float] * 100)
      val lastYear = Integer.parseInt(year) - 1
      var produceGrowthRate = "0";
      if (broadCastMonthProduce.value.contains(lastYear + x._2.substring(4, 7))) {
        val last: Int = broadCastMonthProduce.value.getOrElse(lastYear + x._2.substring(4, 7), 0)
        val now = x._3
        val margin = now - last
        produceGrowthRate = numberFormat.format(margin.asInstanceOf[Float] / last.asInstanceOf[Float] * 100)
      }
      (x._2, x._3, produceProportion, produceGrowthRate)
    })
    val produceData12 = produceData11.map(x => {
      (x._1, Map("produce_num"->x._2,"produce_proportion"->x._3,"produce_growth_rate"->x._4))
    })

    val mapYearSale: scala.collection.mutable.Map[String, Int] = scala.collection.mutable.HashMap()
    val broadCastYearSale = sc.broadcast(mapYearSale)
    val mapMonthSale: scala.collection.mutable.Map[String, Int] = scala.collection.mutable.HashMap()
    val broadCastMonthSale = sc.broadcast(mapMonthSale)

    val saleData = sc.textFile(pathSale)
    val saleData1 = saleData.map(x => dealData(x))
    val saleData2 = saleData1.filter(x => filterDataSale(x))
    val saleData3 = saleData2.map(x => getData(x))
    val saleData4 = saleData3.map(x => getReduceData(x))
    val saleData5 = saleData4.reduceByKey((x, y) => reduceData(x, y))
    saleData5.foreach(x => {
      val year = x._1.substring(0, 4)
      val num = broadCastYearSale.value.getOrElse(year, 0)
      broadCastYearSale.value += (year -> (num + x._2))
      broadCastMonthSale.value += (x._1 -> x._2)
      println("broadCastMonthSale\t" + broadCastMonthSale.value)
    })
    val saleData6 = saleData5.map(x => {
      (x._1.substring(0, 4), x._2)
    })
    val saleData7 = saleData6.reduceByKey((x, y) => x + y)
    val saleData8 = saleData5.map(x => {
      (x._1.substring(0, 4), x)
    })
    val saleData9 = saleData8.join(saleData7)
    val saleData10 = saleData9.map(x => {
      (x._1, x._2._1._1, x._2._1._2, x._2._2)
    })
    val saleData11 = saleData10.map(x => {
      val year = x._1
      val allYearNum: Int = x._4;
      val numberFormat = NumberFormat.getInstance
      numberFormat.setMaximumFractionDigits(2)
      val saleProportion = numberFormat.format(x._3.asInstanceOf[Float] / allYearNum.asInstanceOf[Float] * 100)
      val lastYear = Integer.parseInt(year) - 1
      var saleGrowthRate = "0";
      if (broadCastMonthSale.value.contains(lastYear + x._2.substring(4, 7))) {
        val last: Int = broadCastMonthSale.value.getOrElse(lastYear + x._2.substring(4, 7), 0)
        val now = x._3
        val margin = now - last
        saleGrowthRate = numberFormat.format(margin.asInstanceOf[Float] / last.asInstanceOf[Float] * 100)
      }
      (x._2, x._3, saleProportion, saleGrowthRate)
    })
    val saleData12 = saleData11.map(x => {
      (x._1, Map("sale_num"->x._2,"sale_propotion"->x._3,"sale_growth_rate"->x._4))
    })

    val produceSaleData = produceData12.union(saleData12)
    val produceSaleData1 = produceSaleData.reduceByKey((x, y) => {
      x ++ y
    })

    val result = produceSaleData1.repartition(1)

    val cleanDate = date.DateUtil.now
    result.foreachPartition(partition => {
      @transient val dbp = DatabasePool.getInstance(databaseConf)
      val con = dbp.getConnection
      val cleanSql = "delete from analyze_produce_sale where create_time < '" + cleanDate + "'"
      val ps = con.prepareStatement(cleanSql)
      ps.execute()
      ps.close()
      partition.foreach(x => {
        val data=x._2
        var data1=data.getOrElse("produce_num",0)
        var data2=data.getOrElse("produce_proportion",0)
        var data3=data.getOrElse("produce_growth_rate",0)
        var data4=data.getOrElse("sale_num",0)
        var data5=data.getOrElse("sale_propotion",0)
        var data6=data.getOrElse("sale_growth_rate",0)
        val sql = "insert into analyze_produce_sale values('" +  UUID.randomUUID.toString + "','" + cleanDate + "','" + x._1 + "'," + data1 + "," + data2 + "," + data3 + "," + data4 + "," + data5 + "," + data6 + ",'" + departId + "')"
        println(sql)
        val ps = con.prepareStatement(sql)
        ps.execute()
        ps.close()
      })
      con.close()
    })

    //    val resultProduce = produceData5.repartition(1)
    //
    //    resultProduce.foreachPartition(partition => {
    //      partition.foreach(x => {
    //        val year = x._1.substring(0, 4)
    //        val num = broadCastYearProduce.value.getOrElse(year, 0)
    //        broadCastYearProduce.value += (year -> (num + x._2))
    //        broadCastMonthProduce.value += (x._1 -> x._2)
    //        println("broadCastMonthProduce\t" + broadCastMonthProduce.value)
    //      })
    //    })
    //
    //    val mapYearSale: scala.collection.mutable.Map[String, Int] = scala.collection.mutable.HashMap()
    //    val broadCastYearSale = sc.broadcast(mapYearSale)
    //    val mapMonthSale: scala.collection.mutable.Map[String, Int] = scala.collection.mutable.HashMap()
    //    val broadCastMonthSale = sc.broadcast(mapMonthSale)
    //
    //    val saleData = sc.textFile(pathSale)
    //    val saleData1 = saleData.map(x => dealData(x))
    //    val saleData2 = saleData1.filter(x => filterDataSalae(x))
    //    val saleData3 = saleData2.map(x => getData(x))
    //    val saleData4 = saleData3.map(x => getReduceData(x))
    //    val saleData5 = saleData4.reduceByKey((x, y) => reduceData(x, y))
    //
    //    val resultSale = saleData5.repartition(1)
    //
    //    resultSale.foreachPartition(partition => {
    //      partition.foreach(x => {
    //        val year = x._1.substring(0, 4)
    //        val num = broadCastYearSale.value.getOrElse(year, 0)
    //        broadCastYearSale.value += (year -> (num + x._2))
    //        broadCastMonthSale.value += (x._1 -> x._2)
    //        println("broadCastMonthSale\t" + broadCastMonthSale.value)
    //      })
    //    })
    //
    //    val monthProduceMap = sc.parallelize(broadCastMonthProduce.value.toList)
    //
    //    val monthSaleMap = sc.parallelize(broadCastMonthSale.value.toList)
    //
    //    println("produce  year\t" + broadCastYearProduce.value)
    //    println("produce  month\t" + monthProduceMap)
    //
    //    println("sale  year\t" + broadCastYearSale.value)
    //    println("sale  month\t" + monthSaleMap)
    //
    //    val cleanDate = date.DateUtil.now
    //    val insertData: scala.collection.mutable.Map[String, Array[String]] = scala.collection.mutable.HashMap()
    //    val broadCastInsertData = sc.broadcast(insertData)
    //
    //    broadCastMonthProduce.value.toList.foreach(value => {
    //      println("produce\t" + value._1 + "\t" + value._2)
    //      val year = value._1.substring(0, 4)
    //      val allYearNum: Int = broadCastYearProduce.value.getOrElse(year, 0);
    //
    //      val data: Array[String] = new Array[String](9)
    //      val id = UUID.randomUUID.toString
    //      data(0) = id
    //      data(1) = cleanDate
    //      data(2) = value._1
    //      data(3) = value._2.toString
    //      val numberFormat = NumberFormat.getInstance
    //      numberFormat.setMaximumFractionDigits(2)
    //      val produceProportion = numberFormat.format(value._2.asInstanceOf[Float] / allYearNum.asInstanceOf[Float] * 100)
    //      data(4) = produceProportion.toString
    //      val lastYear = Integer.parseInt(year) - 1
    //      if (broadCastMonthProduce.value.contains(lastYear + value._1.substring(4, 7))) {
    //        val last: Int = broadCastMonthProduce.value.getOrElse(lastYear + value._1.substring(4, 7), 0)
    //        val now = value._2
    //        val margin = now - last
    //        val produceGrowthRate = numberFormat.format(margin.asInstanceOf[Float] / last.asInstanceOf[Float] * 100)
    //        data(5) = produceGrowthRate.toString
    //      } else {
    //        data(5) = "0"
    //      }
    //      data(6) = "0"
    //      data(7) = "0"
    //      data(8) = "0"
    //      broadCastInsertData.value.put(value._1, data)
    //    })
    //
    //    broadCastMonthSale.value.toList.foreach(value => {
    //      println("sale\t" + value._1 + "\t" + value._2)
    //      val year = value._1.substring(0, 4)
    //      val allYearNum: Int = broadCastYearSale.value.getOrElse(year, 0);
    //
    //      val numberFormat = NumberFormat.getInstance
    //      numberFormat.setMaximumFractionDigits(2)
    //      val saleProportion = numberFormat.format(value._2.asInstanceOf[Float] / allYearNum.asInstanceOf[Float] * 100)
    //
    //      val lastYear = Integer.parseInt(year) - 1
    //      var saleGrowthRate = "0"
    //      if (broadCastMonthSale.value.contains(lastYear + value._1.substring(4, 7))) {
    //        val last: Int = broadCastMonthSale.value.getOrElse(lastYear + value._1.substring(4, 7), 0)
    //        val now = value._2
    //        val margin = now - last
    //        saleGrowthRate = numberFormat.format(margin.asInstanceOf[Float] / last.asInstanceOf[Float] * 100)
    //      }
    //
    //      if (broadCastInsertData.value.contains(value._1)) {
    //        val data = broadCastInsertData.value.getOrElse(value._1, null)
    //        data(6) = value._2.toString
    //        data(7) = saleProportion
    //        data(8) = saleGrowthRate
    //        broadCastInsertData.value.put(value._1, data)
    //      } else {
    //        val data: Array[String] = new Array[String](9)
    //        val id = UUID.randomUUID.toString
    //        data(0) = id
    //        data(1) = cleanDate
    //        data(2) = value._1
    //        data(3) = "0"
    //        data(4) = "0"
    //        data(5) = "0"
    //        data(6) = value._2.toString
    //        data(7) = saleProportion
    //        data(8) = saleGrowthRate
    //        broadCastInsertData.value.put(value._1, data)
    //      }
    //    })
    //
    //    val insertDataAll = sc.parallelize(broadCastInsertData.value.toList)
    //
    //    insertDataAll.foreachPartition(partition => {
    //      @transient val dbp = DatabasePool.getInstance(databaseConf)
    //      val con = dbp.getConnection
    //      val cleanSql = "delete from analyze_produce_sale where create_time < '" + cleanDate + "'"
    //      val ps = con.prepareStatement(cleanSql)
    //      ps.execute()
    //      ps.close()
    //      partition.foreach(value => {
    //        val sql = "insert into analyze_produce_sale values('" + value._2(0) + "','" + value._2(1) + "','" + value._2(2) + "'," + value._2(3) + "," + value._2(4) + "," + value._2(5) + "," + value._2(6) + "," + value._2(7) + "," + value._2(8) + ",'" + departId + "')"
    //        println(sql)
    //        val ps = con.prepareStatement(sql)
    //        ps.execute()
    //        ps.close()
    //      })
    //      con.close()
    //    })

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
