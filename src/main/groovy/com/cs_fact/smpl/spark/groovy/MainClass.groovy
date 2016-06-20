package com.cs_fact.smpl.spark.groovy

import com.cs_fact.smpl.spark.groovy.main.s03.RddFilter
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.slf4j.LoggerFactory

/**
 * Created by uemura on 2016/06/20.
 */
class MainClass {
        static void main(String[] args){
            new RddFilter().start()


    }
}

