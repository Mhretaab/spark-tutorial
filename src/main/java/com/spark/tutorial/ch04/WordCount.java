package com.spark.tutorial.ch04;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Word Count").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaRDD<String> inputData = javaSparkContext.textFile("/home/mberhe/Projects/tutorials/spark-tutorial/data/word_count_input.txt");

		JavaPairRDD<String, Integer> flattenPairs = inputData.flatMapToPair(text -> Arrays.asList(text.split(" ")).stream()
				.map(word-> new Tuple2<>(word, 1)).iterator());

		JavaPairRDD<String, Integer> wordCountRDD = flattenPairs.reduceByKey((v1, v2) -> v1 + v2);

		wordCountRDD.saveAsTextFile("/home/mberhe/Projects/tutorials/spark-tutorial/data/word_count_output");
	}
}
