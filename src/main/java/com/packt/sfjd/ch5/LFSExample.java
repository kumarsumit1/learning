package com.packt.sfjd.ch5;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class LFSExample {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\softwares\\Winutils");
		SparkConf conf =new SparkConf().setMaster("local").setAppName("Local File System Example");
		JavaSparkContext jsc=new JavaSparkContext(conf);
		
		JavaRDD<String> localFile = jsc.textFile("C:\\Users\\sgulati\\Documents\\Result\\Missing_Alarms1.txt");
		localFile.flatMap(x -> Arrays.asList(x.split(" ")).iterator()).mapToPair(x -> new Tuple2<String, Integer>((String) x, 1))
		.reduceByKey((x, y) -> x + y);
//		JavaPairRDD<String, Integer> wordCountLocalFile =localFile.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
//			.mapToPair(x -> new Tuple2<String, Integer>((String) x, 1))
//			.reduceByKey((x, y) -> x + y);
		
		//System.out.println(wordCountLocalFile.collect());
		
		
	}
}
