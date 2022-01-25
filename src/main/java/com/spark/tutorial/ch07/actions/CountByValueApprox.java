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

public class CountByValueApprox {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CountByValueApprox Example").setMaster("local");
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

        //  countByValueApprox(long timeout);
        PartialResult<Map<Tuple2<String, Integer>, BoundedDouble>> countByValueApprx = pairRDD.countByValueApprox(1);
        for(Map.Entry<Tuple2<String, Integer>, BoundedDouble> entrySet : countByValueApprx.getFinalValue().entrySet()){
            System.out.println("Confidence for key "+entrySet.getKey()._1() +" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().confidence());
            System.out.println("high for key "+entrySet.getKey()._1()+" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().high());
            System.out.println("Low for key "+entrySet.getKey()._1()+" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().low());
            System.out.println("Mean for key "+entrySet.getKey()._1()+" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().mean());
            System.out.println("Final val for key "+entrySet.getKey()._1()+" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().toString());

        }

        //  countByValueApprox(long timeout, double confidence)
        PartialResult<Map<Tuple2<String, Integer>, BoundedDouble>> countByValueApprox = pairRDD.countByValueApprox(1,0.85);
        for(Map.Entry<Tuple2<String, Integer>, BoundedDouble> entrySet : countByValueApprox.getFinalValue().entrySet()){
            System.out.println("Confidence for key "+entrySet.getKey()._1() +" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().confidence());
            System.out.println("high for key "+entrySet.getKey()._1()+" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().high());
            System.out.println("Low for key "+entrySet.getKey()._1()+" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().low());
            System.out.println("Mean for key "+entrySet.getKey()._1()+" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().mean());
            System.out.println("Final val for key "+entrySet.getKey()._1()+" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().toString());

        }
    }
}
