package com.spark.tutorial.ch07.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.partial.BoundedDouble;
import org.apache.spark.partial.PartialResult;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CountByKeyApprox {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CountByKeyApprox Example").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> list = new ArrayList<>();

        list.add(new Tuple2<>("a", 1));
        list.add(new Tuple2<>("b", 2));
        list.add(new Tuple2<>("c", 3));
        list.add(new Tuple2<>("a", 4));
        list.add(new Tuple2<>("c", 9));
        list.add(new Tuple2<>("b", 11));
        list.add(new Tuple2<>("c", 8));

        JavaPairRDD<String, Integer> pairRDD = jsc.parallelizePairs(list);

        // countByKeyApprox(long timeout);
        PartialResult<Map<String, BoundedDouble>> countByKeyApprx = pairRDD.countByKeyApprox(1);

        for (Map.Entry<String, BoundedDouble> entrySet : countByKeyApprx.getFinalValue().entrySet()) {
            System.out.println("Confidence for " + entrySet.getKey() + " ::" + entrySet.getValue().confidence());
            System.out.println("high for " + entrySet.getKey() + " ::" + entrySet.getValue().high());
            System.out.println("Low for " + entrySet.getKey() + " ::" + entrySet.getValue().low());
            System.out.println("Mean for " + entrySet.getKey() + " ::" + entrySet.getValue().mean());
            System.out.println("Final val for " + entrySet.getKey() + " ::" + entrySet.getValue().toString());
        }

        //countByKeyApprox(long timeout, double confidence)
        PartialResult<Map<String, BoundedDouble>> countByKeyApprox = pairRDD.countByKeyApprox(1,0.75);
        for( Map.Entry<String, BoundedDouble> entrySet:  countByKeyApprox.getFinalValue().entrySet()){
            System.out.println("Confidence for "+entrySet.getKey()+" ::"+entrySet.getValue().confidence());
            System.out.println("high for "+entrySet.getKey()+" ::"+entrySet.getValue().high());
            System.out.println("Low for "+entrySet.getKey()+" ::"+entrySet.getValue().low());
            System.out.println("Mean for "+entrySet.getKey()+" ::"+entrySet.getValue().mean());
            System.out.println("Final val for "+entrySet.getKey()+" ::"+entrySet.getValue().toString());

        }

    }
}
