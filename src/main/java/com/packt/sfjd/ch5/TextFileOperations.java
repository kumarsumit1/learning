package com.packt.sfjd.ch5;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class TextFileOperations {

	public static void main(String[] args) {
		
		 SparkConf conf = new SparkConf().setMaster("local").setAppName("ActionExamples").set("spark.hadoop.validateOutputSpecs", "false");
			JavaSparkContext sparkContext = new JavaSparkContext(conf);
			
		JavaRDD<Person> people = sparkContext.textFile("examples/src/main/resources/people.txt").map(
				  new Function<String, Person>() {
				    public Person call(String line) throws Exception {
				      String[] parts = line.split(",");

				      Person person = new Person();
				      person.setName(parts[0]);
				      person.setAge(Integer.parseInt(parts[1].trim()));

				      return person;
				    }
				  });
				  

	}


	
}
