package com.analyze.service.produce

import org.apache.spark.{SparkConf, SparkContext}

object ProduceSaleAnalyze {

  def main(args: Array[String]): Unit = {
    var master = "local[*]"
    var pathProduce = "D:\\0-program\\test\\analyze\\sys_veh_produce"
    var pathSale = "D:\\0-program\\test\\analyze\\sys_veh_produce"
    var departId = ""
    var databaseConf = "D:\\0-program\\work\\idea\\analyze_project\\src\\main\\resources\\db.properties"

    if (args.length > 0) {
      master = args(0)
      pathProduce = args(1)
      pathSale=args(2)
      databaseConf = args(3)
      departId = args(4)
    }

    val conf = new SparkConf().setMaster(master).setAppName("produceSaleAnalyze")
    val sc = new SparkContext(conf)

    val vehicleData = sc.textFile(pathProduce)
    val saleData = sc.textFile(pathSale)



  }

}
