package com.cs_fact.smpl.spark.groovy.main.s03

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

/**
 * Created by uemura on 2016/06/20.
 */
class RddFilter {
    static final FILE = "./src/main/resources/inputFileSimpleWords.txt"
    static void main(String[] args){
 //def start(){
        def logger = LoggerFactory.getLogger(RddFilter.class)
        logger.info("開始しますRDD Filter")
        def conf = new SparkConf().setAppName("s03_RddFilter").setMaster("local[*]")
        def sc = new JavaSparkContext(conf)
        JavaRDD<String> linesRdd = sc.textFile(FILE)
        JavaRDD<String>  parsistRdd = linesRdd.persist(StorageLevel.DISK_ONLY())
        logger.info("行数:${parsistRdd .count()}")


        logger.info("aaaを除外した場合")
        logger.info("aaaを除外した場合aaaaaS")
        JavaRDD<String>  filterRdd = parsistRdd.filter{line -> line.indexOf("aaa") != -1}
        logger.info("行数:${filterRdd.count()}")
        logger.info("終了しますRDD Filter")


    }
}

