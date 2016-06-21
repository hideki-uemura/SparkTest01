package com.cs_fact.smpl.spark.scala.main.s0303action

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{LoggerFactory,Logger}

/**
  * 起動部分
  */
object Main03Reduce {
  def main(args:Array[String]): Unit ={
    val logger = LoggerFactory.getLogger(classOf[Main03Reduce])
    val DATA = "./src/main/resources/data2/data01.txt"
    val KEY = "./src/main/resources/data2/data02.txt"
//    val logger = LoggerFactory.getLogger(classOf[Main03Reduce])
    logger.info("spark scala開始")
    val sc = null
//    val sc = new SparkContext(new SparkConf setAppName "app" setMaster "local[*]")
    //    new MainS302().testDistinct(DATA,KEY,logger,sc)
    //    new MainS302().testUnion(DATA,KEY,logger,sc)
    //    new MainS302()testInterSection(DATA,KEY,logger,sc)
    new Main03Reduce().testReduce(DATA,KEY,logger,sc)
  }


}
class Main03Reduce extends Object with Serializable{
//  class Main03Reduce {

  /**
    * メインから指定された情報を削除したRDDを生成する
    * @param d
    * @param k
    * @param l
    * @param sc
    */
  def testReduce(d:String,k:String,l:Logger,sc2:SparkContext): Unit = {
//    val logger = l
    val logger = LoggerFactory.getLogger(classOf[Main03Reduce])
    val sc = new SparkContext(new SparkConf setAppName "app" setMaster "local[*]")
//    logger.info(String.valueOf(sc.parallelize(Array("1","2","3","4","5","6","7","8")).reduce((x,y ) =>  y)))
    logger.info(String.valueOf(sc.parallelize(Array(8,1,3)).reduce((x,y ) =>  x)))
    logger.info(String.valueOf(sc.parallelize(Array(8,1,3)).reduce((x,y ) =>  y)))
    logger.info(String.valueOf(sc.parallelize(Array(8,1,3)).reduce((x,y ) =>  x-y)))
//    var count = 0
//    def test1(x:Int, y:Int): Int ={
//     count += 1
//      logger.info(s"回数 => $count : X=> ${x} ： y=> $y")
//      x + y
//    }
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
