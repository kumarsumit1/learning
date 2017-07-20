package com.packt.sfjd.ch10.features;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Word2VecExtractors {
	public static void main(String[] args) {
		 System.setProperty("hadoop.home.dir", "D:\\hadoop");
		
		SparkSession spark = SparkSession
			      .builder().master("local")
			      .appName("JavaWord2VecExample")
			      .getOrCreate();

			    List<Row> data = Arrays.asList(
			      RowFactory.create(Arrays.asList("Learning Apache Spark".split(" "))),
			      RowFactory.create(Arrays.asList("Spark has API for Java, Scala, and Python".split(" "))),
			      RowFactory.create(Arrays.asList("API in above laguage are very richly developed".split(" ")))
			    );
			    StructType schema = new StructType(new StructField[]{
			      new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
			    });
			    Dataset<Row> documentDF = spark.createDataFrame(data, schema);

			    Word2Vec word2Vec = new Word2Vec()
			      .setInputCol("text")
			      .setOutputCol("result")
			      .setVectorSize(10)
			      .setMinCount(0);

			    Word2VecModel model = word2Vec.fit(documentDF);
			    Dataset<Row> result = model.transform(documentDF);

		        result.show(false);

			    spark.stop();
	}

}
