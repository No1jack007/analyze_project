package com.analyze.vehicle

import com.analyze.util.DateUtil
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object VehicleAnalyze {

  def main(args: Array[String]): Unit = {

    var master = "local[*]";
    var path = "D:\\0-program\\test\\";
    var fileName = "sys_veh_produce.txt";
    var departId="";

    if (args.length > 0) {
      master = args(0);
      path = args(1);
      fileName = args(2);
      departId=args(3);
    }

    val conf = new SparkConf().setMaster(master).setAppName("vehicleAnalyze");
    val sc = new SparkContext(conf);

    val vehicleData = sc.textFile(path + fileName);
    val vehicleData1 = vehicleData.map(dealData(_));
    val vehicleData2 = vehicleData1.filter(filterData(_));
    val vehicleData3 = vehicleData2.map(getData(_));
    val vehicleData4 = vehicleData3.map(reduceData(_));
    val vehicleData5 = vehicleData4.reduceByKey(_ ++= _);
    val vehicleData6 = vehicleData5.map(x => {
      val result = new ListBuffer[Int]()
      x._2.foreach(y => {
        result.append(DateUtil.getDutyDays(x._1, y(2)))
      });
      (x._1, result);
    })

    val result = vehicleData6;
    result.foreach(x => {
      print(x._1);
      x._2.foreach(x => {
        print("\t" + x);
      })
      println()
    })
    sc.stop();
  }

  def dealData(x: String): (Array[String]) = {
    x.replaceAll("\"", "").split("\t");
  }

  def filterData(x: Array[String]): (Boolean) = {
    if ("1".equals(x(12)) && !"".equals(x(13))) {
      true;
    } else {
      false;
    }
  }

  def getData(x: Array[String]): (Array[String]) = {
    var result: Array[String] = new Array[String](4);
    result(0) = x(0);
    result(1) = x(4).substring(0, 10);
    result(2) = x(13).substring(0, 10);
    result(3) = x(15);
    result;
  }

  def reduceData(x: Array[String]): (String, ListBuffer[Array[String]]) = {
    val result = new ListBuffer[Array[String]]()
    result.append(x);
    (x(1), result);
  }

}
