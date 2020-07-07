package com.analyze.service.sale

import com.analyze.service.sale.SaleAnalyze.{dealData, filterData, getData}
import com.analyze.util.{CheckUtil, DatabasePool, ProvinceCityUtil}
import org.apache.spark.{SparkConf, SparkContext}
import java.util.{Calendar, UUID}

import cn.hutool.core.date
import cn.hutool.core.util.StrUtil

object VehicleAgeAnalyze {
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

    val now = Calendar.getInstance
    val nowYear = String.valueOf(now.get(Calendar.YEAR))

    val licenseProvince = ProvinceCityUtil.getLicenceProvince(databaseConf)

    val conf = new SparkConf().setMaster(master).setAppName("vehicleAgeAnalyze")
    val sc = new SparkContext(conf)

    val saleData = sc.textFile(path)
    val saleData1 = saleData.map(dealData(_))
    val saleData2 = saleData1.filter(filterData(_))
    val saleData3 = saleData2.map(getData(_))
    val saleData4 = saleData3.map(x => {
      x(3) = licenseProvince.get(x(0).substring(0, 1))
      x
    })
    val saleData5 = saleData4.map(x => {
      val age = Integer.parseInt(nowYear) - Integer.parseInt(x(2))
      if (age < 1) {
        x(4) = "1"
      } else if (age > 8) {
        x(4) = "8"
      } else {
        x(4) = age.toString
      }
      x
    })
    val saleData6 = saleData5.map(x => {
      (x(3) + "-" + x(1) + "-" + x(4), 1)
    })
    val saleData7 = saleData6.reduceByKey((x, y) => {
      x + y
    })

    val result = saleData7.repartition(1)

    val cleanDate = date.DateUtil.now

    result.foreachPartition(partition => {
      val dbp = DatabasePool.getInstance(databaseConf)

      val con = dbp.getConnection
      val cleanSql = "delete from analyze_vehicle_age where create_time < '" + cleanDate + "'"
      val ps = con.prepareStatement(cleanSql)
      ps.execute()
      ps.close()

      partition.foreach(x => {
        val id = UUID.randomUUID.toString
        val data = x._1.split("-")
        val array = data(0)
        val veh_type_name = data(1)
        val age = data(2)
        val num = x._2
        val sql = "insert into analyze_vehicle_age values('" + id + "','" + cleanDate + "','" + array + "','" + veh_type_name + "'," + age + "," + num + ",'" + departId + "')"
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
    if (StrUtil.isNotBlank(x(2)) && CheckUtil.checkDate_1(x(4)) && CheckUtil.checkLicense(x(2))) {
      true
    } else {
      false
    }
  }

  def getData(x: Array[String]): (Array[String]) = {
    val result: Array[String] = new Array[String](5)
    result(0) = x(2)
    result(1) = x(3)
    result(2) = x(4).substring(0, 4)
    result
  }

}
