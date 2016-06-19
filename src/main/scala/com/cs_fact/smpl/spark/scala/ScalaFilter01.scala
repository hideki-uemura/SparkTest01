package com.cs_fact.smpl.spark.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by uemura on 2016/06/20.
  */
class ScalaFilter01 {

  def start(): Unit ={
    def logger = LoggerFactory.getLogger(classOf[ScalaFilter01])
    val sc = new SparkContext(new SparkConf setAppName "ScalaFilter01" setMaster "local[*]")
    val rdd1 = sc parallelize(Array(1,2,3,4,5))
    val outLineRdd = rdd1 filter(v =>  v % 2 == 0)
    outLineRdd collect() foreach (line => logger info(line toString))
    outLineRdd saveAsTextFile "a"
  }
}

