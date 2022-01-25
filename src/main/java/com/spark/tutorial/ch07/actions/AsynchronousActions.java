package com.spark.tutorial.ch07.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class AsynchronousActions {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        SparkConf conf = new SparkConf().setAppName("Asynchronous Actions Example").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<Integer> intRDD = sparkContext.parallelize(Arrays.asList(1,4,3,5,7,6,9,10,11,13,16,20));
        JavaRDD<Integer> intRDD1 = sparkContext.parallelize(Arrays.asList(1,4,3,5,7,6,9,10,11,13,16,20),4);
        JavaRDD<Integer> intRDD2 = sparkContext.parallelize(Arrays.asList(31,34,33,35,37,36,39,310,311,313,316,320),4);

        JavaFutureAction<Long> intCount = intRDD1.countAsync();
        System.out.println(" The async count for "+intCount);

        JavaFutureAction<List<Integer>> intCol = intRDD2.collectAsync();
        for(Integer val:intCol.get()){
            System.out.println("The collect val is "+val);
        }

        JavaFutureAction<List<Integer>> takeAsync = intRDD.takeAsync(3);
        for( Integer val:takeAsync.get()) {
            System.out.println(" The async value of take is :: "+val);
        }

        intRDD.foreachAsync(t -> System.out.println("The val is :"+t));
        intRDD2.foreachAsync(t -> System.out.println("the val2 is :"+t));
    }
}
