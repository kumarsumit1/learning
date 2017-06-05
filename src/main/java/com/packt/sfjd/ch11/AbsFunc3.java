package com.packt.sfjd.ch11;

import java.io.Serializable;

import scala.runtime.AbstractFunction2;

public class AbsFunc3 extends AbstractFunction2<Object, String, String> implements Serializable{

	@Override
	public String apply(Object arg0, String arg1) {
		
		return arg1+":";
	}
	
}