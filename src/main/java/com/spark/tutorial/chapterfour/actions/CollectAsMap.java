package com.spark.tutorial.chapterfour.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CollectAsMap {
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("CollectAsMap Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		List<Tuple2<String, Integer>> list= new ArrayList<>();
		list.add(new Tuple2<>("a", 1));
		list.add(new Tuple2<>("b", 2));
		list.add(new Tuple2<>("c", 3));
		list.add(new Tuple2<>("a", 4));
		JavaPairRDD<String, Integer> pairRDD = javaSparkContext.parallelizePairs(list);

		//Since all the partition data of RDD is dumped at
		//the driver all the effort should be made to minimize the size
		Map<String, Integer> result = pairRDD.collectAsMap();

		System.out.println(result);
	}
}
