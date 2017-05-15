package com.packt.sfjd.ch8;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CaclUDAF extends UserDefinedAggregateFunction  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	@Override
	public StructType inputSchema() {
		// TODO Auto-generated method stub
		//return new StructType(new StructField[] { new StructField("counter", DataTypes.StringType, true, Metadata.empty())});
		return new StructType(new StructField[] { new StructField("counter", DataTypes.DoubleType, true, Metadata.empty())});
	}
	
	@Override
	public DataType dataType() {
		// TODO Auto-generated method stub
		return DataTypes.DoubleType;
	}
	
	@Override
	public boolean deterministic() {
		// TODO Auto-generated method stub
		return false;
	}

	
	
	@Override
	public StructType bufferSchema() {
		
		return new StructType() .add("sumVal", DataTypes.DoubleType) .add("countVal", DataTypes.DoubleType);
	}

	@Override
	public void initialize(MutableAggregationBuffer bufferAgg) {
		bufferAgg.update(0, 0.0);
		bufferAgg.update(1, 0.0);
		
	}

	@Override
	public void update(MutableAggregationBuffer bufferAgg, Row row) {
		bufferAgg.update(0, bufferAgg.getDouble(0)+row.getDouble(0));
		bufferAgg.update(1, bufferAgg.getDouble(1)+2.0);
	}
	
	
	@Override
	public void merge(MutableAggregationBuffer bufferAgg, Row row) {
		bufferAgg.update(0, bufferAgg.getDouble(0)+row.getDouble(0));
		bufferAgg.update(1, bufferAgg.getDouble(1)+row.getDouble(1));
		
	}

	
	@Override
	public Object evaluate(Row row) {
		
		return row.getDouble(0)/row.getDouble(1);
	}

	
	

	
	

}
