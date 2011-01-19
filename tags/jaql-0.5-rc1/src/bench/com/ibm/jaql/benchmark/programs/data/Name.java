package com.ibm.jaql.benchmark.programs.data;

public class Name {
	private String name;
	private int id;
	
	
	public Name() {
		
	}
	
	public Name(String name, int id) {
		this.name = name;
		this.id = id;
	}

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
