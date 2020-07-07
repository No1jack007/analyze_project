package com.analyze.service.sale

import java.util.UUID

import cn.hutool.core.date
import cn.hutool.core.util.StrUtil
import com.analyze.service.sale.VehicleAgeAnalyze.{dealData, filterData, getData}
import com.analyze.util.{CheckUtil, DatabasePool, IDCardUtil}
import org.apache.spark.{SparkConf, SparkContext}

object UserPortraitAnalyze {

  def main(args: Array[String]): Unit = {

    var master = "local[*]"
    //    var path = "D:\\0-program\\test\\analyze\\sys_veh_sale"
    var path = "D:\\0-program\\test\\idCard.txt"
    var departId = ""
    var databaseConf = "D:\\0-program\\work\\idea\\analyze_project\\src\\main\\resources\\db.properties"

    if (args.length > 0) {
      master = args(0)
      path = args(1)
      databaseConf = args(2)
      departId = args(3)
    }

    val idCodeProvince = IDCardUtil.getProvince(databaseConf)

    val conf = new SparkConf().setMaster(master).setAppName("userPortraitAnalyze")
    val sc = new SparkContext(conf)

    val saleData = sc.textFile(path)
    val saleData1 = saleData.map(dealData(_))
    val saleData2 = saleData1.filter(filterData(_,idCodeProvince))
    val saleData3 = saleData2.map(getData(_))
    val saleData4 = saleData3.map(x => {
      val array = idCodeProvince.get(x.substring(0, 2))
//      println(x.substring(0, 2)+"\t"+array)
      val sex = IDCardUtil.parseGender(x)
      val age = IDCardUtil.parseAge(x)
      (array, sex, age)
    })
    val saleData5 = saleData4.map(x => {
      (x._1 + "-" + x._2, 1)
    })
    val saleData6 = saleData5.reduceByKey((x, y) => {
      x + y
    })
    val saleData7 = saleData4.map(x => {
      val age = x._3
      var ageType = "0"
      if (age >= 18 && age <= 30) {
        ageType = "1"
      } else if (age >= 31 && age <= 40) {
        ageType = "2"
      } else if (age >= 41 && age <= 50) {
        ageType = "3"
      } else if (age >= 51 && age <= 60) {
        ageType = "4"
      } else {
        ageType = "0"
      }
      (x._1, x._2, ageType)
    })
    val saleData8 = saleData7.map(x => {
      val array = Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
      if ("男".equals(x._2)) {
        if ("1".equals(x._3)) {
          array(0) = 1
        } else if ("2".equals(x._3)) {
          array(1) = 1
        } else if ("3".equals(x._3)) {
          array(2) = 1
        } else if ("4".equals(x._3)) {
          array(3) = 1
        } else if ("0".equals(x._3)) {
          array(4) = 1
        }
      } else if ("女".equals(x._2)) {
        if ("1".equals(x._3)) {
          array(6) = 1
        } else if ("2".equals(x._3)) {
          array(7) = 1
        } else if ("3".equals(x._3)) {
          array(8) = 1
        } else if ("4".equals(x._3)) {
          array(9) = 1
        } else if ("0".equals(x._3)) {
          array(10) = 1
        }
      }
      (x._1 + "-" + x._2, array)
    })
    val saleData9 = saleData8.reduceByKey((x, y) => {
      x(0) = x(0) + y(0)
      x(1) = x(1) + y(1)
      x(2) = x(2) + y(2)
      x(3) = x(3) + y(3)
      x(4) = x(4) + y(4)
      x(5) = x(5) + y(5)
      x(6) = x(6) + y(6)
      x(7) = x(7) + y(7)
      x(8) = x(8) + y(8)
      x(9) = x(9) + y(9)
      x(10) = x(10) + y(10)
      x(11) = x(11) + y(11)
      (x)
    })
    val saleData10 = saleData9.join(saleData6)
    val saleData11 = saleData10.map(x => {
      val key = x._1.split("-")
      if ("男".equals(key(1))) {
        x._2._1(5) = x._2._2
      } else if ("女".equals(key(1))) {
        x._2._1(11) = x._2._2
      }
      (key(0), x._2._1)
    })
    val saleData12 = saleData11.reduceByKey((x, y) => {
      x(0) = x(0) + y(0)
      x(1) = x(1) + y(1)
      x(2) = x(2) + y(2)
      x(3) = x(3) + y(3)
      x(4) = x(4) + y(4)
      x(5) = x(5) + y(5)
      x(6) = x(6) + y(6)
      x(7) = x(7) + y(7)
      x(8) = x(8) + y(8)
      x(9) = x(9) + y(9)
      x(10) = x(10) + y(10)
      x(11) = x(11) + y(11)
      (x)
    })
    val saleData13 = saleData12.map(x => {
      x._2(12) = x._2(5) + x._2(11)
      x
    })

    val result = saleData13.repartition(1)

    val cleanDate = date.DateUtil.now

    result.foreachPartition(partition => {
      val dbp = DatabasePool.getInstance(databaseConf)

      val con = dbp.getConnection
      val cleanSql = "delete from analyze_user_portrait where create_time < '" + cleanDate + "'"
      val ps = con.prepareStatement(cleanSql)
      ps.execute()
      ps.close()

      partition.foreach(x => {
        val id = UUID.randomUUID.toString
        val sql = "insert into analyze_user_portrait values('" + id + "','" + cleanDate + "','" + x._1 + "'," + x._2(0) + "," + x._2(1) + "," + x._2(2) + "," + x._2(3) + "," + x._2(4) + "," + x._2(5) + "," + x._2(6) + "," + x._2(7) + "," + x._2(8) + "," + x._2(9) + "," + x._2(10) + "," + x._2(11) + "," + x._2(12) + ",'" + departId + "')"
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

  def filterData(x: Array[String],idCodeProvince: java.util.Map[String,String]): (Boolean) = {
    if (StrUtil.isNotBlank(x(6)) && CheckUtil.checkIdCard(x(6))) {
      val array = idCodeProvince.get(x(6).substring(0, 2))
      if(array!=null){
        true
      }else{
        false
      }
    } else {
      false
    }
  }

  def getData(x: Array[String]): (String) = {
    x(6)
  }


}
