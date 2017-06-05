package com.packt.sfjd.ch11;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;

import scala.Predef.$eq$colon$eq;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class PropertyGraphExample {
	public static void main(String[] args) {

		System.setProperty("hadoop.home.dir", "C:\\softwares\\Winutils");
		SparkConf conf = new SparkConf().setMaster("local").setAppName("graph");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		
		
		
	 //$eq$colon$eq<Object, Object> scala$Predef$$singleton_$eq$colon$eq =  scala.Predef$.MODULE$.scala$Predef$$singleton_$eq$colon$eq;
$eq$colon$eq<String, String> tpEquals = scala.Predef.$eq$colon$eq$.MODULE$.tpEquals();
		List<Tuple2<Object, String>> vertices = new ArrayList<>();

		vertices.add(new Tuple2<Object, String>(1l, "James"));
		vertices.add(new Tuple2<Object, String>(2l, "Charlie"));
		vertices.add(new Tuple2<Object, String>(3l, "John"));

		List<Edge<String>> edges = new ArrayList<>();

		edges.add(new Edge<String>(1, 2, "Manager"));
		edges.add(new Edge<String>(2, 3, "Collegue"));
		edges.add(new Edge<String>(1, 3, "Manager"));

		JavaRDD<Tuple2<Object, String>> verticesRDD = javaSparkContext.parallelize(vertices);
		JavaRDD<Edge<String>> edgesRDD = javaSparkContext.parallelize(edges);

		Graph<String, String> graph = Graph.apply(verticesRDD.rdd(), edgesRDD.rdd(), "", StorageLevel.MEMORY_ONLY(),
				StorageLevel.MEMORY_ONLY(), stringTag, stringTag);
		

		/* graph.edges().toJavaRDD().collect().forEach(System.out::println);

		// graph.triplets().toJavaRDD().collect().forEach(System.out::println);

		System.out.println("-------------------------------");
		Graph<String, String> subgraph = graph.subgraph(new AbsFunc1(), new AbsFunc2());
		subgraph.edges().toJavaRDD().collect().forEach(System.out::println);*/
		
		Graph<String, String> mapVertices = graph.mapVertices(new AbsFunc3(), stringTag, tpEquals);
		mapVertices.vertices().toJavaRDD().collect().forEach(System.out::println);
		
	}

}
