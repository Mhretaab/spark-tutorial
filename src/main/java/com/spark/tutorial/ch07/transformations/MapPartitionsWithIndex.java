package com.spark.tutorial.ch07.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MapPartitionsWithIndex {
	public static void main(String[] args) {
		//similar to mapPartitions
		//partition index is available
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Map Partition with index Example");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		List<Integer> intList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> intRDD = jsc.parallelize(intList, 2);

		JavaRDD<String> elementsWithPartitionRDD = intRDD.mapPartitionsWithIndex(
				(Function2<Integer, Iterator<Integer>, Iterator<String>>) (index, itr) -> {
					List<String> list = new ArrayList<>();

					while (itr.hasNext()) {
						list.add(String.format("Element %s belongs to partition %d", itr.next(), index));
					}
					return list.iterator();
				}, false);

		System.out.println(elementsWithPartitionRDD.collect());
	}
}
