package com.packt.sfjd.ch7;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.spark.util.AccumulatorV2;

public class ListAccumulator extends AccumulatorV2<String, CopyOnWriteArrayList<String>> {

	private static final long serialVersionUID = 1L;
	private CopyOnWriteArrayList<String> accList=null;
	
	public ListAccumulator() {
		accList = new CopyOnWriteArrayList<String>();
		
		
	}

	public ListAccumulator(List<String> value) {
		if (value.size() != 0) {
			accList = new CopyOnWriteArrayList<String>(value);
		}
	}

	@Override
	public void add(String arg0) {
		if (!arg0.isEmpty())
			accList.add(arg0);

	}

	@Override
	public AccumulatorV2<String, CopyOnWriteArrayList<String>> copy() {
		return new ListAccumulator(value());
	}

	@Override
	public boolean isZero() {
		return accList.size() == 0 ? true : false;
	}

	@Override
	public void merge(AccumulatorV2<String, CopyOnWriteArrayList<String>> other) {
		add(other.value());

	}

	private void add(List<String> value) {
		value().addAll(value);
	}

	@Override
	public void reset() {
		accList = new CopyOnWriteArrayList<String>();
	}

	@Override
	public CopyOnWriteArrayList<String> value() {
		return accList;
	}

}
