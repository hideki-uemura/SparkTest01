package com.cs_fact.smpl.spark.scala.main.s302

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import org.slf4j.Logger

/**
  * 起動部分
  */
object MainS302 {
  def main(args:Array[String]): Unit ={
    val DATA = "./src/main/resources/data2/data01.txt"
    val KEY = "./src/main/resources/data2/data02.txt"
    val logger = LoggerFactory.getLogger(classOf[MainS302])
    logger.info("spark scala開始")
    val sc = new SparkContext(new SparkConf setAppName "app" setMaster "local[*]")
//    new MainS302().testDistinct(DATA,KEY,logger,sc)
//    new MainS302().testUnion(DATA,KEY,logger,sc)
//    new MainS302()testInterSection(DATA,KEY,logger,sc)
    new MainS302()testSubtract(DATA,KEY,logger,sc)

  }


}

/**
  * 実装部分
  */
class MainS302{



  /**
    * メインから指定された情報を削除したRDDを生成する
    * @param d
    * @param k
    * @param l
    * @param sc
    */
  def testSubtract(d:String,k:String,l:Logger,sc:SparkContext): Unit = {
    val dataPath = d
    val keyPath = k
    val logger = l
    sc.textFile(dataPath).subtract(sc.parallelize(Array("aaa","ddd"))).foreach(logger info _)
  }

  /**
    * 両方にあるものを取得する
    * @param d
    * @param k
    * @param l
    * @param sc
    */
  def testInterSection(d:String,k:String,l:Logger,sc:SparkContext): Unit = {
    val dataPath = d
    val keyPath = k
    val logger = l
    //ユニオンして全部出す
    logger.info("両方に含まれているものを出す")
    sc.textFile(dataPath).intersection(sc.textFile(keyPath)).foreach(logger info _)

    logger.info("両方に含まれているものを出す(parで生成")
    sc.textFile(dataPath).intersection(sc.parallelize(Array("aaa"))).foreach(logger info _)

  }

  /**
    * Unionする
    *
    * @param d
    * @param k
    * @param l
    * @param sc
    */
  def testUnion(d:String,k:String,l:Logger,sc:SparkContext): Unit ={
    val dataPath = d
    val keyPath = k
    val logger = l
    //ユニオンして全部出す
    logger.info("ユニオンして全部出す")
    sc.textFile(dataPath).union(sc.textFile(keyPath)).filter(_.indexOf("aaa") != -1).foreach(logger info _)

    //ユニオンしてFilterして出す
    logger.info("ユニオンしてFilter(aaaを含まないもの)出す")
    sc.textFile(dataPath).union(sc.textFile(keyPath)).filter(_.indexOf("aaa") == -1).foreach(logger info _)


  }


  /**
    * distinctして除外
    *
    * @param d
    * @param k
    * @param l
    * @param sc
    */
  def testDistinct(d:String,k:String,l:Logger,sc:SparkContext): Unit ={
    val data = d
    val logger = l
    sc.textFile(data).distinct().foreach(logger.info _)
  }

}
