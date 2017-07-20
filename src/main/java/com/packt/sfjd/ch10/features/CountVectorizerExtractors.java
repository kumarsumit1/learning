package com.packt.sfjd.ch10.features;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CountVectorizerExtractors {
	public static void main(String[] args) {
		 System.setProperty("hadoop.home.dir", "D:\\hadoop");
		 SparkSession spark = SparkSession
			      .builder().master("local")
			      .appName("JavaCountVectorizerExample")
			      .getOrCreate();


			    List<Row> data = Arrays.asList(
			      RowFactory.create(Arrays.asList("w1", "w2", "w3")),
			      RowFactory.create(Arrays.asList("w1", "w2", "w4", "w5", "w2"))
			    );
			    
			    StructType schema = new StructType(new StructField [] {
			      new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
			    });
			    Dataset<Row> df = spark.createDataFrame(data, schema);

			      CountVectorizerModel cvModel = new CountVectorizer()
			      .setInputCol("text")
			      .setOutputCol("feature")
			      .setVocabSize(5)
			      .setMinDF(2)
			      .fit(df);

			    System.out.println("The words in the Vocabulary are :: "+ Arrays.toString(cvModel.vocabulary()));

			    cvModel.transform(df).show(false);

			   
			    // CVM model having an a-priori dictionary			    
			    CountVectorizerModel cvMod = new CountVectorizerModel(new String[]{"w1", "w2", "w3"})
			    		  .setInputCol("text")
			    		  .setOutputCol("feature");
			    System.out.println("The words in the Vocabulary are :: "+ Arrays.toString(cvMod.vocabulary()));
			    
			    cvMod.transform(df).show(false);
			    
			    spark.stop();
	}
}
