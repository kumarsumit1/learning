package com.packt.sfjd.ch10.datatypes;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Matrices;
import org.apache.spark.ml.linalg.Matrix;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.mllib.util.MLUtils;

public class MLDataTypes {

	public static void main(String[] args) {
		
		// Create a dense vector (1.0, 0.0, 3.0).
		Vector dv = Vectors.dense(1.0, 0.0, 3.0);
		// Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
		Vector sv = Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0});
		
		
		// Create a labeled point with a positive label and a dense feature vector.
		LabeledPoint pos = new LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0));

		// Create a labeled point with a negative label and a sparse feature vector.
		LabeledPoint neg = new LabeledPoint(0.0, Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0}));
		
		//JavaRDD<LabeledPoint> examples = 
			//	  MLUtils.loadLibSVMFile(jsc.sc(), "data/mllib/sample_libsvm_data.txt").toJavaRDD();
		
		
		// Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
		Matrix dm = Matrices.dense(3, 2, new double[] {1.0, 3.0, 5.0, 2.0, 4.0, 6.0});

		// Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
		Matrix sm = Matrices.sparse(3, 2, new int[] {0, 1, 3}, new int[] {0, 2, 1}, new double[] {9, 6, 8});
		
	}
	
	
}