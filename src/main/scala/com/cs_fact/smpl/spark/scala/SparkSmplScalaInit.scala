package com.cs_fact.smpl.spark.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by uemura on 2016/06/20.
  */
object SparkSmplScalaInit {

  def main(args:Array[String]): Unit ={
    val logger = LoggerFactory.getLogger(classOf[SparkSmplScalaInit])
    logger.info("scala:Spark開始します")
    val ctx = new SparkContext(new SparkConf setAppName "SparkSmplScalaInit" setMaster "local[*]" )
    val rdd = ctx parallelize(Array(1,2,3,4))
    val cnt = rdd.count()
    logger.info(s"件数＝＞$cnt")
    logger.info("scala:Spark終了します")
  }
}


class SparkSmplScalaInit{

}
