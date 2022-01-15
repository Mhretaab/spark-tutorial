package com.spark.tutorial.ch05.dataformats;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CSVData {
	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession
				.builder()
				.appName("CSV Data Basics")
				.master("local")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();

		JavaRDD<Movie> moviesRDD = sparkSession.read().textFile("/home/mberhe/Projects/tutorials/spark-tutorial/data/movies.csv").javaRDD().filter(str -> !(null == str))
				.filter(str -> !(str.length() == 0))
				.filter(str -> !str.contains("movieId"))
				.map(str -> Movie.parseRating(str));

		moviesRDD.foreach(movie -> System.out.println(movie));

		//use the Spark CSV library by Databricks for CSV datatypes
		Dataset<Row> csv_read = sparkSession.read().format("com.databricks.spark.csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.load("/home/mberhe/Projects/tutorials/spark-tutorial/data/movies.csv");

		csv_read.printSchema();
		csv_read.show();

		//inferred schema can be overridden by specifying the schema of
		//StructType having StructField as elements representing the CSV fields
		StructType customSchema = new StructType(new StructField[] {
				new StructField( "movieId", DataTypes.LongType, true, Metadata.empty()),
				new StructField("title", DataTypes.StringType, true, Metadata.empty()),
				new StructField("genres", DataTypes.StringType, true, Metadata.empty())
		});

		Dataset<Row> csv_custom_read = sparkSession.read().format("com.databricks.spark.csv")
				.option("header", "true")
				.schema(customSchema)
				.load("/home/mberhe/Projects/tutorials/spark-tutorial/data/movies.csv");
		csv_custom_read.printSchema();
		csv_custom_read.show();

		//Save to disk
		csv_custom_read.write()
				.format("com.databricks.spark.csv")
				.option("header", "true")
				.option("delimiter", ",")
				//.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
				.save("/home/mberhe/Projects/tutorials/spark-tutorial/data/newMovies.csv");
	}
}
