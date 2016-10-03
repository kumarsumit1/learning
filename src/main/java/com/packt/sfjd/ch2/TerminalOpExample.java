package com.packt.sfjd.ch2;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class TerminalOpExample {

	public static void main(String[] args) {
		// forEach
		Supplier<Stream<String>> streamSupplier =()->Stream.of( new String[]{"Stream","from","an","array","of","objects"} ) ;
		
		streamSupplier.get().forEach(P->System.out.println());
	//sum
		System.out.println(streamSupplier.get().mapToInt(x -> x.length()).sum());   //Notice here had we used MAP , we would have had to another map function to convert the in Int.
		 
    //reduce
		Optional<Integer> simpleSum= streamSupplier.get().map(x->x.length()).reduce((x,y)-> x+y);
		
		System.out.println( "The value with simpe reduce is ::"+simpleSum.get());
		
		Integer defaulValSum= streamSupplier.get().map(x->x.length()).reduce(0,(x,y)-> x+y);
		System.out.println( "The value with default reduce is ::"+defaulValSum);
		
		Integer valSum= streamSupplier.get().reduce(0,(x,y)-> x+y.length(),(acc1,acc2)->acc1+acc2);
		System.out.println("The value with with cobine reduce is ::"+valSum);
		
   //collect		
	
		StringBuilder concat = streamSupplier.get()
                .collect(() -> new StringBuilder(),
                         (sbuilder, str) -> sbuilder.append(str),
                         (sbuilder1, sbuiler2) -> sbuilder1.append(sbuiler2));
		
		
		StringBuilder concatM = streamSupplier.get()
                .collect(StringBuilder::new,
                         StringBuilder::append,
                         StringBuilder::append);
		
		String concatC = streamSupplier.get().collect(Collectors.joining());
	
	//System.out.println( joinWithReduce(Stream . of ( "foo" , "bar" , "baz" ) ));
	
	//System.out.println( joinWithCollect(Stream . of ( "foo" , "bar" , "baz" ) ));

	}
	
	static String joinWithReduce ( Stream < String > stream ) { // BAD 
		return stream.reduce( new StringBuilder (), StringBuilder :: append , StringBuilder :: append ). toString (); } 
	
	static String joinWithCollect ( Stream < String > stream ) { // OK 
		return stream.collect ( StringBuilder :: new , StringBuilder :: append , StringBuilder :: append ). toString (); }
	

}
