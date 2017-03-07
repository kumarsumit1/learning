package com.packt.sfjd.ch5;

import java.io.Serializable;

public class Person implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String Name;
	private Integer Age;
	public String getName() {
		return Name;
	}
	public void setName(String name) {
		Name = name;
	}
	public Integer getAge() {
		return Age;
	}
	public void setAge(Integer age) {
		Age = age;
	}
	@Override
	public String toString() {
		return String.format("Person [Name=%s, Age=%s]", Name, Age);
	}
	
}
