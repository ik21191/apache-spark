package com.spark.dataframe.common;

import java.io.Serializable;

public class MyDocument implements Serializable {

	private static final long serialVersionUID = 1L;
	
	String Name;
	String City;
	int Marks;
	String test;
	
	public String getName() {
		return Name;
	}
	public void setName(String name) {
		Name = name;
	}
	public String getCity() {
		return City;
	}
	public void setCity(String city) {
		City = city;
	}
	public int getMarks() {
		return Marks;
	}
	public void setMarks(int marks) {
		Marks = marks;
	}
	public String getTest() {
		return test;
	}
	public void setTest(String test) {
		this.test = test;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return Name + "\t" + test;
	}
}
