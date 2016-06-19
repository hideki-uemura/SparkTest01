package com.cs_fact.smpl.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by uemura on 2016/06/19.
 */
public class SparkSmpl01JavaInit {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setAppName("public class SparkSmpl01JavaInit").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<Integer> parallelizeRDD = context.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        long count = parallelizeRDD.count();
        System.out.println(count);
    }
}
