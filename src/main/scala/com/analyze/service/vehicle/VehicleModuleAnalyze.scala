package com.analyze.service.vehicle

import org.apache.spark.{SparkConf, SparkContext}

object VehicleModuleAnalyze {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("vehicleModuleAnalyze");
    val sc = new SparkContext(conf);

    val path = "D:\\0-program\\test\\";

    val vehicleModelData = sc.textFile(path + "sys_veh_model.txt");
    val vehicleModelData1 = vehicleModelData.map(x => (x.replaceAll("\"", "").split("\t")))
    val vehicleModelData2 = vehicleModelData1.map(x => (x(4), x))
    //    val broadcastVehicleModel = sc.broadcast(vehicleModelData2)

    vehicleModelData2.foreach(line => (
      println(line._1)
      ))

    val vehicleData = sc.textFile(path + "sys_veh.txt", 4);
    val vehicleData1 = vehicleData.map(x => (x.replaceAll("\"", "").split("\t")));
    val vehicleData2 = vehicleData1.map(x => (x(4), x(0)))

    val vehicleModelJoin = vehicleData2.leftOuterJoin(vehicleModelData2);

    val result = vehicleModelJoin;
    result.foreachPartition(partition => {
      partition.foreach(line => {
        //        line.foreach(cell => print(cell + "\t"))
        println(line._1 + "\t" + line._2._1 + "\t" + line._2._2.get(6))
        println()
      })
    })

    sc.stop()
  }

}
