package com.cs_fact.smpl.spark.groovy.main.s03.main.s0303reduce

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.Function2
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

/**
 * Created by uemura on 2016/06/21.
 */
class ReduceTest {
    static void main(String[] args){
        def logger = LoggerFactory.getLogger(ReduceTest.class)
        logger.info("開始しますRDD Filter")
        def conf = new SparkConf().setAppName("s03_RddFilter").setMaster("local[*]")
        def sc = new JavaSparkContext(conf)
        def v = sc.parallelize(Arrays.asList(1,2,3))
        v.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            Integer call(Integer v1, Integer v2) throws Exception {
                logger.info(v1 + "")
                logger.info(v2 + "")
                v1 + v2
            }
        })
    }
}
