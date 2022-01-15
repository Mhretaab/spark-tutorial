package com.spark.tutorial.ch05.datasources;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class AmazonS3 {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Interaction with Amazon S3").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		//AWS Access key ID
		//AWS Secret access key
		//export AWS_ACCESS_KEY_ID=accessKeyIDValue
		//export AWS_SECRET_ACCESS_KEY=securityAccesKeyValue

		//Using S3N Scheme
		//javaSparkContext.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "accessKeyIDValue");
		//javaSparkContext.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "securityAccesKeyValue");
		//javaSparkContext.textFile("s3n://"+"bucket-name"+"/"+"file-path");
		//javaSparkContext.textFile("s3n://"+"bucket-name"+"/"+"file-path-1", "s3n://"+"bucket-name"+"/"+"file-path-2");
		//javaSparkContext.textFile("s3n://"+"bucket-name"+"/"+"path-of-dir/*");
		//s3WordCount.saveAsTextFile("s3n://"+"buxket-name"+"/"+"outputDirPath");

		//Using S3A: latest file system scheme
		//removed the upper limit constraint of S3N and provides various
		//performance improvements
		javaSparkContext.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", "accessKeyIDValue");
		javaSparkContext.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", "securityAccesKeyValue");
		JavaRDD<String> s3File = javaSparkContext.textFile("s3a://"+"bucket-name"+"/"+"file-path");
		//javaSparkContext.textFile("s3a://"+"bucket-name"+"/"+"file-path-1", "s3a://"+"bucket-name"+"/"+"file-path-2");
		//javaSparkContext.textFile("s3a://"+"bucket-name"+"/"+"path-of-dir/*");

		JavaPairRDD<String, Integer> s3WordCount = s3File.flatMapToPair(text -> Arrays.asList(text.split(" ")).stream()
				.map(word-> new Tuple2<>(word, 1)).iterator());

		JavaPairRDD<String, Integer> wordCountRDD = s3WordCount.reduceByKey((v1, v2) -> v1 + v2);

		//To write the results back to Amazon S3
		s3WordCount.saveAsTextFile("s3a://"+"buxket-name"+"/"+"outputDirPath");

	}
}
