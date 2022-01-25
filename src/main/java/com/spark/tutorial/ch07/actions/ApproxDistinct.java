package com.spark.tutorial.ch07.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ApproxDistinct {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Approx Distinct Counts Example").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Integer> intRDD = jsc.parallelize(Arrays.asList(1,4,3,5,7,6,9,4,3,13,16,20));

        long countApprxDistinct = intRDD.countApproxDistinct(0.95);
        System.out.println("The approximate distinct element count is ::"+countApprxDistinct);

        List<Tuple2<String, Integer>> list = new ArrayList<>();

        list.add(new Tuple2<>("a", 1));
        list.add(new Tuple2<>("b", 2));
        list.add(new Tuple2<>("c", 3));
        list.add(new Tuple2<>("a", 4));
        list.add(new Tuple2<>("c", 3));
        list.add(new Tuple2<>("b", 11));
        list.add(new Tuple2<>("c", 8));

        JavaPairRDD<String, Integer> pairRDD = jsc.parallelizePairs(list);

        JavaPairRDD<String, Long> approxCountOfKey = pairRDD.countApproxDistinctByKey(0.80);
        approxCountOfKey.foreach(tuple -> System.out.println("The approximate distinct vlaues for Key :"+tuple._1()+" is ::"+tuple._2()));

    }
}
