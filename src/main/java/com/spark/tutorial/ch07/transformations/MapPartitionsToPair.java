package com.spark.tutorial.ch07.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MapPartitionsToPair {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Map Partition to pair Example");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		List<Integer> intList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> intRDD = jsc.parallelize(intList, 2);

		intRDD.mapPartitionsToPair(iterator -> {

			List<Tuple2<String, Integer>> list = new ArrayList<>();

			while (iterator.hasNext()) {
				Integer element = iterator.next();
				list.add(
						element % 2 == 0 ? new Tuple2<>("EVEN", element) : new Tuple2<>("ODD", element)
				);
			}
			return list.iterator();
		});
	}
}
