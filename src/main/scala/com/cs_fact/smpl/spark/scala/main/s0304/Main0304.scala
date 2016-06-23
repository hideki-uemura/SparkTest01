package com.cs_fact.smpl.spark.scala.main.s0304

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by uemura on 2016/06/21.
  */
object Main0304 {
  def main(args:Array[String]): Unit ={
        new Main0304 start
  }
}
class  Main0304 {
  def start(): Unit = {
    val sc = createContext("Main0304","local[*]")

  }


  def createContext(appName:String,master:String):SparkContext = {
    new SparkContext( new SparkConf setAppName appName setMaster master)
  }


  def createRdd(sc:SparkContext):RDD




}