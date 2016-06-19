package com.cs_fact.smpl.spark.groovy

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.Function2
import org.apache.spark.api.java.function.PairFunction
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory
import scala.Tuple2

/**
 * Created by uemura on 2016/06/20.
 */
class Spark0102inTextFile {
    static void main(String[] args){
        def logger = LoggerFactory.getLogger(Spark0102inTextFile.class)
        logger.info("開始します")
        def ctx = new JavaSparkContext(new SparkConf().setAppName("Spark0102inTextFile").setMaster("local[*]"))
        def simpleWords = ctx.textFile("src/main/resources/inputFileSimpleWords.txt")
        def rdd = simpleWords.flatMap {
                line -> Arrays.asList(line.split(" "))
        }
        def map = rdd
           .mapToPair(new PairFunction<String,String,Integer>() {
                Tuple2<String,Integer> call(String word) throws Exception {
                    new Tuple2(word,1)
                }
            })
        def redRdd = map
            .reduceByKey(new Function2<Integer, Integer, Integer>() {
                Integer call(Integer v1, Integer v2) throws Exception {
                    v1 + v2
                }
            })
        simpleWords.persist(StorageLevel.DISK_ONLY())
        redRdd.persist(StorageLevel.DISK_ONLY())
        logger.info(String.valueOf(redRdd.count()))
        simpleWords.saveAsTextFile("out")

        logger.info("終了します")

    }
}
