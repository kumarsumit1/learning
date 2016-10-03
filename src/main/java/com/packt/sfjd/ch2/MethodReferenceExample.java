package com.packt.sfjd.ch2;

import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class MethodReferenceExample {

	public static void main(String[] args) {
		Supplier<Stream<String>> streamSupplier =()->Stream.of( new String[]{"Stream","from","an","array","of","objects"} ) ;
		
		//Method Refernce instead of lambda function
		Optional<Integer> simpleSum= streamSupplier.get().map(x->x.length()).reduce(Integer::sum);
	

	}

}
