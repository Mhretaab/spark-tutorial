package com.spark.tutorial.ch07.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.partial.BoundedDouble;
import org.apache.spark.partial.PartialResult;

import java.util.Arrays;

public class CountApprox {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CountApprox Example").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Integer> intRDD = jsc.parallelize(Arrays.asList(1,4,3,5,7,6,9,10,11,13,16,20));
        PartialResult<BoundedDouble> countAprx = intRDD.countApprox(20);

        System.out.println("Confidence::"+countAprx.getFinalValue().confidence());
        System.out.println("high::"+countAprx.getFinalValue().high());
        System.out.println("Low::"+countAprx.getFinalValue().low());
        System.out.println("Mean::"+countAprx.getFinalValue().mean());
        System.out.println("Final::"+countAprx.getFinalValue().toString());

        //If the countApprox method has a confidence of 0.7,
        //then one can expect 70% of the results to contain the true count.
        PartialResult<BoundedDouble> countAprox = intRDD.countApprox(1, 0.95);
        System.out.println("Confidence::"+countAprox.getFinalValue().confidence());
        System.out.println("high::"+countAprox.getFinalValue().high());
        System.out.println("Low::"+countAprox.getFinalValue().low());
        System.out.println("Mean::"+countAprox.getFinalValue().mean());
        System.out.println("Final::"+countAprox.getFinalValue().toString());
    }
}
