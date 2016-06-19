package com.cs_fact.smpl.spark.scala

import org.slf4j.LoggerFactory

/**
  * Created by uemura on 2016/06/20.
  */
object ExecuteMain {
  def key = "ScalaFilter01"

  def main(args:Array[String]): Unit ={
    val logger = LoggerFactory.getLogger(classOf[ExecuteMain])
    if (key == "ScalaFilter01"){
      logger.info(s"$key 実行します")
      new ScalaFilter01 start()
      logger.info(s"$key 完了しました")
    }
  }
}
class  ExecuteMain{}
