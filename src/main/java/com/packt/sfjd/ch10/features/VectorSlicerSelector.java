package com.packt.sfjd.ch10.features;

import static org.apache.spark.sql.functions.col;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.attribute.Attribute;
import org.apache.spark.ml.attribute.AttributeGroup;
import org.apache.spark.ml.attribute.NumericAttribute;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorSlicer;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class VectorSlicerSelector {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\hadoop");
		 SparkSession spark = SparkSession
			      .builder().master("local")
			      .appName("JavaVectorSlicerExample")
			      .getOrCreate();
			    
			    Dataset<Row> emp_ds = spark.read().format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true")
						.load("src/main/resources/employee.txt");
		        
				Dataset<Row> featSet = emp_ds.select(col("empId"),col("salary"),col("deptno"));
				
				String[] featuresCols = featSet.columns();

				Dataset<Row> vecAssembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("rawFeatures").transform(featSet);

			    VectorSlicer vectorSlicer = new VectorSlicer().setInputCol("rawFeatures").setOutputCol("features");

			    vectorSlicer.setIndices(new int[]{1, 2});

			    Dataset<Row> output = vectorSlicer.transform(vecAssembler);
			    output.show(false);

			    spark.stop();
	}
}
