package com.spark.tutorial.ch07.sharedvariables;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

public class BroadcastVariable {
    public static void main(String[] args) {
        //	SparkConf conf = new SparkConf().setMaster("local").setAppName("BroadCasting");
        //	JavaSparkContext jsc = new JavaSparkContext(conf);
        // 	Broadcast<String> broadcastVar = jsc.broadcast("Hello Spark");

        SparkSession sparkSession = SparkSession.builder()
                .master("local").appName("My App").getOrCreate();

        //lifecycle is managed by BroadcastManager and ContextCleaner
        //ContextCleaner is a Spark service which is responsible for cleaning up RDDs
        Broadcast<String> broadcastVar= sparkSession.sparkContext().broadcast("Hello Spark",  scala.reflect.ClassTag$.MODULE$.apply(String.class));
        System.out.println(broadcastVar.getValue());

        broadcastVar.unpersist();
        // broadcastVar.unpersist(true);
        broadcastVar.destroy();
        //broadcastVar.destroy(true);
    }
}
