package com.spark.tutorial.chapterfour;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FlatMapToPairTransformation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("FlatMapToPair Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaRDD<String> stringRDD = javaSparkContext.parallelize(Arrays.asList("Hello World", "Hello Spark"));

		JavaPairRDD<String, Integer> wordLengthRDD = stringRDD.flatMapToPair(
				txt -> Arrays.asList(txt.split(" ")).stream().map(word -> new Tuple2<>(word, word.length()))
						.iterator());

		List<Tuple2<String, Integer>> result = wordLengthRDD.collect();

		System.out.println(result);
	}
}
