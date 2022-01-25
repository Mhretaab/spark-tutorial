package com.spark.tutorial.ch07.actions;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MiscellaneousActions {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        SparkConf conf = new SparkConf().setAppName("Asynchronous Actions Example").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<Integer> intRDD = sparkContext.parallelize(Arrays.asList(1,4,3,5,7,6,9,10,11,13,16,20));
        JavaRDD<Integer> intRDD1 = sparkContext.parallelize(Arrays.asList(1,4,3,5,7,6,9,10,11,13,16,20),4);
        JavaRDD<Integer> intRDD2 = sparkContext.parallelize(Arrays.asList(31,34,33,35,37,36,39,310,311,313,316,320),4);

        List<Tuple2<String, Integer>> list = new ArrayList<>();

        list.add(new Tuple2<>("a", 1));
        list.add(new Tuple2<>("b", 2));
        list.add(new Tuple2<>("c", 3));
        list.add(new Tuple2<>("a", 4));
        list.add(new Tuple2<>("c", 9));
        list.add(new Tuple2<>("b", 11));
        list.add(new Tuple2<>("c", 8));

        JavaPairRDD<String, Integer> pairRDD = sparkContext.parallelizePairs(list);

        List<Integer> lookupVal = pairRDD.lookup("a");
        for(Integer val:lookupVal){
            System.out.println("The lookup val is ::"+val);
        }

        int numberOfPartitions = intRDD2.getNumPartitions();
        System.out.println("The no of partitions in the RDD are ::"+numberOfPartitions);

        List<Partition> partitions = intRDD1.partitions();
        for(Partition part :partitions){
            System.out.println(part.toString());
            System.out.println(part.index());


        }

        List<Integer>[] collectPart = intRDD1.collectPartitions(new int[]{1,2});
        System.out.println("The length of collectPart is "+collectPart.length);

        for(List<Integer> i:collectPart){
            for(Integer it:i){
                System.out.println(" The val of collect is "+it);
            }

        }

        System.out.println(" The no of partitions are ::"+ intRDD1.getNumPartitions());

    }
}
