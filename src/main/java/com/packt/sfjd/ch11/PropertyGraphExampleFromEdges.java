package com.packt.sfjd.ch11;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;

import scala.Function1;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction1;

public class PropertyGraphExampleFromEdges {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\softwares\\Winutils");
		SparkConf conf = new SparkConf().setMaster("local").setAppName("graph");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		ClassTag<Long> longTag = scala.reflect.ClassTag$.MODULE$.apply(Long.class);
		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);


		List<Edge<String>> edges = new ArrayList<>();

		edges.add( new Edge<String>(0, 1, "Friend"));
		edges.add(new Edge<String>(1, 2, "Collegue"));
		edges.add( new Edge<String>(0, 2, "Friend"));

		JavaRDD<Edge<String>> edgeRDD = javaSparkContext.parallelize(edges);
		
		
		Graph<Long, String> graph = Graph.fromEdges(edgeRDD.rdd(), 1l,StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), longTag, stringTag);
		
		
		graph.vertices().toJavaRDD().collect().forEach(System.out::println);
		
		
		
//	graph.aggregateMessages(sendMsg, mergeMsg, tripletFields, evidence$11)	
		
	}
}
