package com.packt.sfjd.ch10.features;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import static org.apache.spark.sql.functions.col;

public class VectorAssemblerTransformers {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\hadoop");
		SparkSession spark = SparkSession.builder().master("local").appName("JavaVectorAssemblerExample").getOrCreate();

		Dataset<Row> emp_ds = spark.read().format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true")
				.load("src/main/resources/employee.txt");
        
		Dataset<Row> featSet = emp_ds.select(col("empId"),col("salary"),col("deptno"));
		
		String[] featuresCols = featSet.columns();

		VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("rawFeatures");

		Dataset<Row> output = vectorAssembler.transform(featSet);

		output.show(false);

		spark.stop();
	}

}
