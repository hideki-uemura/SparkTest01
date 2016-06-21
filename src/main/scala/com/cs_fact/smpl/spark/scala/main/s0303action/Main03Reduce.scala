package com.cs_fact.smpl.spark.scala.main.s0303action

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{LoggerFactory,Logger}

/**
  * 起動部分
  */
object Main0302Reduce {
  def main(args:Array[String]): Unit ={
    val logger = LoggerFactory.getLogger(classOf[Main0302Reduce)
    val DATA = "./src/main/resources/data2/data01.txt"
    val KEY = "./src/main/resources/data2/data02.txt"
  }


}
class Main0302Reduce extends Object with Serializable{
  def testReduce(d:String,k:String,l:Logger,sc2:SparkContext): Unit = {
//    val logger = l
    val logger = LoggerFactory.getLogger(classOf[Main0302Reduce])
    val sc = new SparkContext(new SparkConf setAppName "app" setMaster "local[*]")
//    logger.info(String.valueOf(sc.parallelize(Array("1","2","3","4","5","6","7","8")).reduce((x,y ) =>  y)))
    logger.info(String.valueOf(sc.parallelize(Array(8,1,3)).reduce((x,y ) =>  x)))
    logger.info(String.valueOf(sc.parallelize(Array(8,1,3)).reduce((x,y ) =>  y)))
    logger.info(String.valueOf(sc.parallelize(Array(8,1,3)).reduce((x,y ) =>  x-y)))
    def test2(x:Int, y:Int): Int ={
      logger.info(s"X=> ${x} ： y=> $y")
      x + 1
    }

    def rdd = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))
//    def v = rdd.reduce(test2)
    def v = rdd.reduce(test2)
    logger.info(String.valueOf(v))


//    logger.info(String.valueOf(sc.parallelize(Array("1","2","3","4","5","6","7","8")).reduce((x,y ) =>  x + y)))
  }
}
