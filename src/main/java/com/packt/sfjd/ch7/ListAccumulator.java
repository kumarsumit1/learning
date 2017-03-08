package com.packt.sfjd.ch7;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.util.AccumulatorV2;

public class ListAccumulator extends AccumulatorV2<String, List<String>> {

	private static final long serialVersionUID = 1L;
	private List<String> accList=null;
	
	public ListAccumulator() {
		accList = new ArrayList<String>();
		
	}

	public ListAccumulator(List<String> value) {
		if (value.size() != 0) {
			accList = new ArrayList<String>(value);
		}
	}

	@Override
	public void add(String arg0) {
		if (!arg0.isEmpty())
			accList.add(arg0);

	}

	@Override
	public AccumulatorV2<String, List<String>> copy() {
		return new ListAccumulator(value());
	}

	@Override
	public boolean isZero() {
		return accList.size() == 0 ? true : false;
	}

	@Override
	public void merge(AccumulatorV2<String, List<String>> other) {
		add(other.value());

	}

	private void add(List<String> value) {
		value().addAll(value);
	}

	@Override
	public void reset() {
		accList = new ArrayList<String>();
	}

	@Override
	public List<String> value() {
		return accList;
	}

}
