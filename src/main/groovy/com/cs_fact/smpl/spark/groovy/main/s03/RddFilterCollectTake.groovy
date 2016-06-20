package com.cs_fact.smpl.spark.groovy.main.s03

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

/**
 * Created by uemura on 2016/06/20.
 */
class RddFilterCollectTake {
    static final FILEDATA = "./src/main/resources/exists/data.txt"
    static final FILEKEY = "./src/main/resources/exists/key.txt"
    static void main(String[] args){
 //def start(){
        def logger = LoggerFactory.getLogger(RddFilterCollectTake.class)
        logger.info("開始しますRDD RddFilterCollectTake")
        def conf = new SparkConf().setAppName("s03_RddFilter").setMaster("local[*]")
        def sc = new JavaSparkContext(conf)


        def dataRdd = sc.textFile(FILEDATA)
        def keyRdd = sc.textFile(FILEKEY)
//        def keys = keyRdd.take(1).toString().replace("[" ,"").replace("]","")
        def keys = keyRdd.take(1)
        logger.info("KYE=>$keys")
        dataRdd.foreach({line ->  logger.info(line)})
        def dataFilter = dataRdd.filter(
                {
                    line ->
                        logger.info("line=>$line keys=> $keys")
                        line.indexOf(keys) != -1
                }
        )
        logger.info("カウント${dataFilter.count()}")
        logger.info("ここからは内容をFOREACh")
        dataFilter.foreach({line -> println(line)})



    }
}

