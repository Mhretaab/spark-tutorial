package com.spark.tutorial.ch05.datasources;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class LocalFileSystem {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Interaction with Local File System").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		//DAG of Spark operations starts to execute
		//only when an action is performed. Here, reading the file from
		//the local filesystem is also a part of DAG
		//block size: 32MB
		JavaRDD<String> inputData = javaSparkContext.textFile("/home/mberhe/Projects/tutorials/spark-tutorial/data/word_count_input.txt");
		//javaSparkContext.textFile("absolutepathOfTheFile1,absolutepathOfTheFile2");
		//javaSparkContext.textFile("absolutepathOfTheDir/*");
		//javaSparkContext.textFile("absolutepathOfTheDir1/*,absolutepathOfTheDir2/*");

		JavaPairRDD<String, Integer> flattenPairs = inputData.flatMapToPair(text -> Arrays.asList(text.split(" ")).stream()
				.map(word-> new Tuple2<>(word, 1)).iterator());

		JavaPairRDD<String, Integer> wordCountRDD = flattenPairs.reduceByKey((v1, v2) -> v1 + v2);

		wordCountRDD.saveAsTextFile("/home/mberhe/Projects/tutorials/spark-tutorial/data/local_file_system_1");

		//returns a pair RDD with key as the filename and value as the
		//content of the file
		JavaPairRDD<String, String> localFile = javaSparkContext.wholeTextFiles(
				"/home/mberhe/Projects/tutorials/spark-tutorial/data/word_count_input.txt");
		//javaSparkContext.wholeTextFiles ("absolutepathOfTheFile1,absolutepathOfTheFile2");
		//javaSparkContext.wholeTextFiles ("absolutepathOfTheDir/*");
		//javaSparkContext.wholeTextFiles ("absolutepathOfTheDir1/*,absolutepathOfTheDir2/*");
		localFile.saveAsTextFile("/home/mberhe/Projects/tutorials/spark-tutorial/data/local_file_system_2");
	}
}
