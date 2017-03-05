package com.packt.sfjd.ch5;



import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
//29,"City of Lost Children, The (Cité des enfants perdus, La) (1995)",Adventure|Drama|Fantasy|Mystery|Sci-Fi
//40,"Cry, the Beloved Country (1995)",Drama
public class CSVFileOperations {

	public static void main(String[] args) {
		  System.setProperty("hadoop.home.dir", "E:\\sumitK\\Hadoop");
			
	      SparkSession spark = SparkSession
	      .builder()
	      .master("local")
		  .config("spark.sql.warehouse.dir","file:///E:/sumitK/Hadoop/warehouse")
	      .appName("JavaALSExample")
	      .getOrCreate();
	      Logger rootLogger = LogManager.getRootLogger();
			rootLogger.setLevel(Level.WARN); 

	    JavaRDD<Movies> moviesRDD = spark
	      .read().textFile("C:/Users/sumit.kumar/git/learning/src/main/resources/movies.csv").javaRDD().filter( str-> !(null==str))
	      .filter(str-> !(str.length()==0))
	      .filter(str-> !str.contains("movieId"))	      
	      .map(new Function<String, Movies>() {
			private static final long serialVersionUID = 1L;
			public Movies call(String str) {		        	
	          return Movies.parseRating(str);
	        }
	      });
	    
	    moviesRDD.foreach(new VoidFunction<Movies>() {
			
			@Override
			public void call(Movies t) throws Exception {
				System.out.println(t);
				
			}
		});
	    
	    String formatClass = "com.databricks.spark.csv";
		       Dataset<Row> csv_read = spark.read().format(formatClass)
		        		      .option("header", "true")
		        		      .option("inferSchema", "true")
		        		      .load("C:/Users/sumit.kumar/git/learning/src/main/resources/movies.csv");
		       
		       csv_read.printSchema();
		       
		       csv_read.show();
	}

}
