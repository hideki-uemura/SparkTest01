package com.cs_fact.smpl.spark.groovy

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.slf4j.LoggerFactory

/**
 * Created by uemura on 2016/06/19.
 */
class SparkSmplGroovyInit {
    static void main(String[] args){
        def logger = LoggerFactory.getLogger(SparkSmplGroovyInit.class)
        logger.info("groovy:開始します(groovy)")
        def context = new JavaSparkContext(new SparkConf().setAppName("SparkSmplGroovyInit").setMaster("local[*]"))
        logger.info(String.valueOf(context.parallelize([1,2,3,5]).count()))
        logger.info("groovy:spark終了します")
}
}
