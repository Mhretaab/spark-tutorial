package com.spark.tutorial.ch05.dataformats;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JSONData {
	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession
				.builder()
				.appName("JSON Data Basics")
				.master("local")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();

		RDD<String> textFile = sparkSession.sparkContext()
				.textFile("/home/mberhe/Projects/tutorials/spark-tutorial/data/pep_json.json", 2);
		JavaRDD<PersonDetails> mapParser = textFile.toJavaRDD()
				.map(v1 -> new ObjectMapper().readValue(v1, PersonDetails.class));
		mapParser.foreach(personDetails -> System.out.println(personDetails));

		//handling JSON using Spark SQL
		Dataset<Row> json_rec = sparkSession.read()
				.json("/home/mberhe/Projects/tutorials/spark-tutorial/data/pep_json.json");
		json_rec.printSchema();
		json_rec.show();

		//with custom schema
		StructType schema = new StructType(new StructField[] {
				DataTypes.createStructField("cid", DataTypes.IntegerType, true),
				DataTypes.createStructField("county", DataTypes.StringType, true),
				DataTypes.createStructField("firstName", DataTypes.StringType, true),
				DataTypes.createStructField("sex", DataTypes.StringType, true),
				DataTypes.createStructField("year", DataTypes.StringType, true),
				DataTypes.createStructField("dateOfBirth", DataTypes.TimestampType, true) });

		Dataset <Row> person_mod = sparkSession.read().schema(schema).json("/home/mberhe/Projects/tutorials/spark-tutorial/data/pep_json.json");
		person_mod .printSchema();
		person_mod .show();

		//Saving RDD in JSON format
		person_mod.write().format("json").mode("overwrite").save("/home/mberhe/Projects/tutorials/spark-tutorial/data/pep_out.json");
	}
}
