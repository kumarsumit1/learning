package com.packt.sfjd.ch8;

import java.io.Serializable;

public class Employee implements Serializable{
	
public Employee(){}	
public Employee(String name, int id) {
		super();
		this.name = name;
		this.id = id;
	}
private String name;
private int id;
public String getName() {
	return name;
}
public void setName(String name) {
	this.name = name;
}
public int getId() {
	return id;
}
public void setId(int id) {
	this.id = id;
}

}
