package com.packt.sfjd.ch5;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class TextFileOperations {

	public static void main(String[] args) {
		 System.setProperty("hadoop.home.dir", "E:\\sumitK\\Hadoop");
		 SparkConf conf = new SparkConf().setMaster("local").setAppName("ActionExamples").set("spark.hadoop.validateOutputSpecs", "false");
			JavaSparkContext sparkContext = new JavaSparkContext(conf);
			
		 JavaRDD<String> textFile = sparkContext.textFile("C:/Users/sumit.kumar/git/learning/src/main/resources/people.tsv");
				
				
				JavaRDD<Person> people =textFile.map(
				  new Function<String, Person>() {
				    public Person call(String line) throws Exception {
				      String[] parts = line.split("~");
				      Person person = new Person();
				      person.setName(parts[0]);
				      person.setAge(Integer.parseInt(parts[1].trim()));
				      return person;
				    }
				  });
				  
				  
				people.foreach(new VoidFunction<Person>() {					
					@Override
					public void call(Person t) throws Exception {
						System.out.println(t);						
					}
				});
			
			
				
				JavaRDD<Person> peoplePart = textFile.mapPartitions(new FlatMapFunction<Iterator<String>, Person>() {
					@Override
					public Iterator<Person> call(Iterator<String> t)throws Exception {
						ArrayList<Person> personList=new ArrayList<Person>();
						while (t.hasNext()){
							String[] parts = t.next().split("~");
						      Person person = new Person();
						      person.setName(parts[0]);
						      person.setAge(Integer.parseInt(parts[1].trim()));	
						      personList.add(person);
						}
						return  personList.iterator();
					}
				});
				  
				peoplePart.foreach(new VoidFunction<Person>() {
					@Override
					public void call(Person t) throws Exception {
						System.out.println(t);						
					}
				});
			
				
				people.saveAsTextFile("C:/Users/sumit.kumar/git/learning/src/main/resources/peopleSimple");
				people.repartition(1).saveAsTextFile("C:/Users/sumit.kumar/git/learning/src/main/resources/peopleRepart");
				people.coalesce(1).saveAsTextFile("C:/Users/sumit.kumar/git/learning/src/main/resources/peopleCoalesce");
	}


	
}
