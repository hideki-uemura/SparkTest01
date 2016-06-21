package com.cs_fact.smpl.spark.scala.main.s301

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import org.slf4j.Logger

/**
  * Created by uemura on 2016/06/20.
  */
object MainS3MethodMapFlatMapFilterForeach {


  def main(args:Array[String]): Unit ={
    val DATA = "./src/main/resources/exists/data.txt"
    val KEY = "./src/main/resources/exists/key.txt"
    val logger = LoggerFactory.getLogger(classOf[MainS3MethodMapFlatMapFilterForeach])
    logger.info("spark scala開始")
    val sc = new SparkContext(new SparkConf setAppName "app" setMaster "local[*]")


//    new MainS3Method().start2(DATA,KEY,logger,sc)
//    new MainS3Method().testMap(DATA,logger,sc)
//    new MainS3Method().testFlatMap(DATA,logger,sc)
      new MainS3MethodMapFlatMapFilterForeach().testFlatMap2(DATA,KEY,logger,sc)

  }


}



class MainS3MethodMapFlatMapFilterForeach {
  /**
    * 単純なフラットmapをキーでFilterする
    *
    * @param d
    * @param l
    * @param sc
    */
  def testFlatMap2 (d:String,k:String,l:Logger,sc:SparkContext): Unit = {
    val data = d
    val key = k
    val logger = l
    val dataRdd = sc.textFile(data)
    val keyData = sc textFile key first()
    //単語単位に分割したためすべてaaaしかでない
    logger.info("フラットにしてフィルター")
    dataRdd.flatMap(_.split(" ")).filter(_.indexOf(keyData) != -1).foreach(logger.info(_))
    logger.info("フラットにせずフィルター")
    dataRdd.map(_ + "XXXXXXXX").filter(_.indexOf(keyData) != -1).foreach(logger.info(_))
  }

  /**
    * 単純なフラットmap
    *
    * @param d
    * @param l
    * @param sc
    */
  def testFlatMap (d:String,l:Logger,sc:SparkContext): Unit = {
    val data = d
    val logger = l
    val dataRdd = sc.textFile(data)
    dataRdd.flatMap{_.split(" ")}.foreach(logger.info(_))
  }
  /**
    * Mapメソッド使って変換を加えた所
    *
    * @param d
    * @param l
    * @param sc
    */
  def testMap (d:String,l:Logger,sc:SparkContext): Unit = {
    val data = d
    val logger = l
    val dataRdd = sc.textFile(data)
    dataRdd.map{data => s"【$data $data】"}.foreach(joinStr=>logger.info(joinStr))
  }
  /**
    * test01
    */
  def start (): Unit = {
    val DATA = "./src/main/resources/exists/data.txt"
    val KEY = "./src/main/resources/exists/key.txt"
    val logger = LoggerFactory.getLogger(classOf[MainS3MethodMapFlatMapFilterForeach])
    logger.info("spark scala開始")
    val sc = new SparkContext(new SparkConf setAppName "app" setMaster "local[*]")

    val dataRdd = sc.textFile(DATA)
    val keyRdd = sc.textFile(KEY)
    val key = keyRdd.take(1).toString
    dataRdd.sortBy({line => }).map({ line => line + s"XXXXXX" }).foreach(
      newLine => logger.info(newLine)
    )
  }

  /**
    * テスト02
    *
    * @param data
    * @param key
    * @param sc
    */
    def start2 (d:String,k:String ,l:Logger,sc:SparkContext): Unit ={
      val data = d
      val key = k
      val logger = l
      val dataRdd = sc.textFile(data)
      val keyRdd = sc.textFile(key)
//      keyRdd.persist()
      val count = keyRdd.count()
    logger.info(count + "")
    val keyRddInKey = keyRdd.first()
      logger.info(keyRddInKey)
      val newDataRdd = dataRdd.map {line => line + s"${keyRddInKey}".toString}
      newDataRdd.foreach {line => logger.info(line)}
  }


}
