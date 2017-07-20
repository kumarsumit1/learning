package com.packt.sfjd.ch10.features;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;

public class RFormulaSelector {
	public static void main(String[] args) {
		 System.setProperty("hadoop.home.dir", "D:\\hadoop");
		SparkSession spark = SparkSession
			      .builder().master("local")
			      .appName("JavaRFormulaExample")
			      .getOrCreate();

			    // $example on$
			    StructType schema = createStructType(new StructField[]{
			      createStructField("id", IntegerType, false),
			      createStructField("country", StringType, false),
			      createStructField("hour", IntegerType, false),
			      createStructField("clicked", DoubleType, false)
			    });

			    List<Row> data = Arrays.asList(
			      RowFactory.create(7, "US", 18, 1.0),
			      RowFactory.create(8, "CA", 12, 0.0),
			      RowFactory.create(9, "NZ", 15, 0.0)
			    );

			    Dataset<Row> dataset = spark.createDataFrame(data, schema);
			    RFormula formula = new RFormula()
			      .setFormula("clicked ~ country + hour")
			      .setFeaturesCol("features")
			      .setLabelCol("label");
			    Dataset<Row> output = formula.fit(dataset).transform(dataset);
			    output.select("features", "label").show();
			    // $example off$
			    spark.stop();
	}
}
