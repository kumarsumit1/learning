package com.packt.sfjd.ch2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class IntermediateOpExample {

	public static void main(String[] args) {
		
		//Filter Operation
		 IntStream.rangeClosed(1, 10)
		          .filter(s -> s>4)
                  .forEach(p -> System.out.println(p));
		 
		 //Map Operation
		 int incrementVal=6;
		 IntStream.rangeClosed(1, 10)
		          .filter(s -> s>4)
		 		  .map(x -> x+incrementVal)
                  .forEach(p -> System.out.println(p));
		 
		 Stream.of(new String[]{"Let me see what i get this time"}).map(x -> x.split("\\s+")).forEach(System.out::println);

		 //Sorted
		// Stream<String> ArrayStream = Stream.of( new String[]{"stream","from","an","array","of","objects"} );
		 
		 //http://stackoverflow.com/questions/23860533/copy-a-stream-to-avoid-stream-has-already-been-operated-upon-or-closed-java-8
		 Supplier<Stream<String>> streamSupplier =()-> Stream.of( new String[]{"stream","from","an","array","of","objects"} );
		 
		 
	    streamSupplier.get().sorted(Comparator.reverseOrder()).forEach(System.out::println);  
		//Comparator.reversed().
			
		//sorted(Comparator.reversed()).forEach(System.out::println);  
	    streamSupplier.get().sorted(Comparator.comparing(x -> x.toString().length()).reversed()).forEach(System.out::println);
	    
	    //flatMap
	    Stream<List<String>> namesOriginalList = Stream.of(
	    		Arrays.asList("Pankaj"), 
	    		Arrays.asList("David", "Lisa"),
	    		Arrays.asList("Amit"));
	    	//flat the stream from List<String> to String stream
	    	Stream<String> flatStream = namesOriginalList
	    		.flatMap(strList -> strList.stream());

	    	flatStream.forEach(System.out::println);
		 
	    	//
			
		//	Stream.of(1, 2, 3)
		//	.flatMap(x -> IntStream.range(0, x))
		//	.forEach(System.out::println);
			
		System.out.println(	" the count of stream is "+ 
				
			//String[] sr=(String[]) 
					
					Stream.of(new String[]{"Let,me,see,what,i,get,this,time","ok,now,what"})
					.peek(x->System.out.println( "the length of the sream is"+ x.length()))
					.map(x -> x.split(","))
			 .peek(x->System.out.println(x.length))
			 .count());
			 
			 
			// .collect(Collectors.toList())
			// .flatMap(x -> Arrays.stream(x)).forEach(System.out::println);
			
			 
			 /*.forEach(x -> {
				 for(String sr:x){
					 System.out.println(x.length);
					 System.out.println(sr);
				 }
			 });
			*/ 
			// .peek(x-> System.out.println(x))
		//	 .flatMap(Arrays::stream)
			// .forEach(System.out::println);
		          
	}

}
