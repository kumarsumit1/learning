package com.packt.sfjd.ch5;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonFileOperations {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "E:\\sumitK\\Hadoop");
		Logger rootLogger = LogManager.getRootLogger();
		rootLogger.setLevel(Level.WARN); 
		      SparkSession spark = SparkSession
		      .builder()
		      .master("local")
			  .config("spark.sql.warehouse.dir","file:///E:/sumitK/Hadoop/warehouse")
		      .appName("JavaALSExample")
		      .getOrCreate();
		      
		   RDD<String> textFile = spark.sparkContext().textFile("C:/Users/sumit.kumar/git/learning/src/main/resources/pep_json.json",2); 
		   
		   JavaRDD<PersonDetails> mapParser = textFile.toJavaRDD().map(new Function<String, PersonDetails>() {

			@Override
			public PersonDetails call(String v1) throws Exception {
				
				return new ObjectMapper().readValue(v1, PersonDetails.class);
			}
		});
		   
		  mapParser.foreach(new VoidFunction<PersonDetails>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(PersonDetails t) throws Exception {
				System.out.println(t);
				
			}
		}); 
		   Dataset<Row> anotherPeople = spark.read().json(textFile);
		   
		 //  anotherPeople.
		   
		   anotherPeople.printSchema();
		   anotherPeople.show();
		      
		      
		      Dataset<Row> json_rec = spark.read().json("C:/Users/sumit.kumar/git/learning/src/main/resources/pep_json.json");
		      
		      
		      json_rec.printSchema();
		      
		      json_rec.show();
		      
		      StructType schema = new StructType( new StructField[] {
		    	            DataTypes.createStructField("cid", DataTypes.IntegerType, true),
		    	            DataTypes.createStructField("county", DataTypes.StringType, true),
		    	            DataTypes.createStructField("firstName", DataTypes.StringType, true),
		    	            DataTypes.createStructField("sex", DataTypes.StringType, true),
		    	            DataTypes.createStructField("year", DataTypes.StringType, true),
		    	            DataTypes.createStructField("dateOfBirth", DataTypes.TimestampType, true) });
		      
		    /*  StructType pep = new StructType(new StructField[] {
						new StructField("Count", DataTypes.StringType, true, Metadata.empty()),
						new StructField("County", DataTypes.StringType, true, Metadata.empty()),
						new StructField("First Name", DataTypes.StringType, true, Metadata.empty()),
						new StructField("Sex", DataTypes.StringType, true, Metadata.empty()),
						new StructField("Year", DataTypes.StringType, true, Metadata.empty()),
					    new StructField("timestamp", DataTypes.TimestampType, true, Metadata.empty()) });*/
		      
		     Dataset<Row> person_mod = spark.read().schema(schema).json(textFile);
		     
		     person_mod.printSchema();
		     person_mod.show();
		     
		     person_mod.write().format("json").mode("overwrite").save("C:/Users/sumit.kumar/git/learning/src/main/resources/pep_out.json");

	}

}
