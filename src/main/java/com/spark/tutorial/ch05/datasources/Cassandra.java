package com.spark.tutorial.ch05.datasources;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Cassandra {
	public static void main(String[] args) {
		//highly scalable with no single point of failure
		//availability and partition tolerance categories of the CAP theorem with tunable consistency
		//very high-speed writes
		SparkConf conf = new SparkConf().setAppName("Interaction with Cassandra NoSQL DB").setMaster("local");
		conf.set("spark.cassandra.connection.host", "127.0.0.1");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		//Mapping Cassandra table row to POJO
		JavaRDD<Employee> cassandraRDD = CassandraJavaUtil.javaFunctions(javaSparkContext)
				.cassandraTable("my_keyspace", "emp", CassandraJavaUtil.mapRowTo(Employee.class));

		cassandraRDD.collect().forEach(System.out::println);

		//load only one column from the table
		JavaRDD<String> selectEmpDept = CassandraJavaUtil.javaFunctions(javaSparkContext)
				.cassandraTable("my_keyspace", "emp", CassandraJavaUtil.mapColumnTo(String.class))
				.select("emp_dept");

		selectEmpDept.collect().forEach(System.out::println);

		//To write the results back to Cassandra
		CassandraJavaUtil.javaFunctions(cassandraRDD)
				.writerBuilder("my_keyspace", "emp1", CassandraJavaUtil.mapToRow(Employee.class)).saveToCassandra();

		//A dataset is an RDD with schema
		//Using the dataset API, an RDD can be viewed as a tabular structure

		/*SQLContext sqlContext = new SQLContext(javaSparkContext);

		Map<String,String> map =new HashMap<>();
		map.put("table" , "emp");
		map.put("keyspace", "my_keyspace");

		Dataset<Row> df = sqlContext.read().format("org.apache.spark.sql.cassandra")
		  .options(map)
		  .load();

		df.show();*/
	}
}
