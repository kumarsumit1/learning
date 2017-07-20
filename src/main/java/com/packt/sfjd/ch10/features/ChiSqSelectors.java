package com.packt.sfjd.ch10.features;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.ChiSqSelector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ChiSqSelectors {
	public static void main(String[] args) {
		 System.setProperty("hadoop.home.dir", "D:\\hadoop");
		SparkSession spark = SparkSession
			      .builder().master("local")
			      .appName("JavaChiSqSelectorExample")
			      .getOrCreate();

			    List<Row> data = Arrays.asList(
			      RowFactory.create(1, Vectors.dense(0.1, 0.0, 1.0, 20.0), 0.0),
			      RowFactory.create(2, Vectors.dense(0.2, 1.0, 13.0, 40.0), 1.0),
			      RowFactory.create(3, Vectors.dense(0.3, 2.0, 21.0, 1.1), 0.0)
			    );
			    StructType schema = new StructType(new StructField[]{
			      new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
			      new StructField("rawfeatures", new VectorUDT(), false, Metadata.empty()),
			      new StructField("label", DataTypes.DoubleType, false, Metadata.empty())
			    });

			    Dataset<Row> df = spark.createDataFrame(data, schema);

			    ChiSqSelector selector = new ChiSqSelector()
			      .setNumTopFeatures(1)
			      .setFeaturesCol("rawfeatures")
			      .setLabelCol("label")
			      .setOutputCol("features");

			    Dataset<Row> result = selector.fit(df).transform(df);
			    result.show();


			    spark.stop();
	}
}
