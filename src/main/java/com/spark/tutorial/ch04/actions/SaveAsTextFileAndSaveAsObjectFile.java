package com.spark.tutorial.ch04.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SaveAsTextFileAndSaveAsObjectFile {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SaveAsTextFile And SaveAsObjectFile Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaRDD<Integer> rdd = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5), 3);

		//RDD can be saved to some local filesystem, HDFS, or any other
		//Hadoop-supported filesystem
		rdd.saveAsTextFile("/home/mberhe/Projects/tutorials/spark-tutorial/data/rdd_to_file");
		JavaRDD<String> textRDD = javaSparkContext.textFile("/home/mberhe/Projects/tutorials/spark-tutorial/data/rdd_to_file");
		textRDD.foreach(x -> System.out.println("The elements read from TextFileDir are :" + x));

		//In order to store and retrieve the data and metadata information about its data
		//type of RDD
		rdd.saveAsObjectFile("/home/mberhe/Projects/tutorials/spark-tutorial/data/rdd_to_object");
		JavaRDD<Integer> objectRDD= javaSparkContext.objectFile("/home/mberhe/Projects/tutorials/spark-tutorial/data/rdd_to_object");
		objectRDD.foreach(x -> System.out.println("The elements read from TextFileDir are :" + x));
	}
}
