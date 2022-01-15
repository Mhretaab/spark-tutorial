package com.spark.tutorial.ch05.dataformats;

import java.io.Serializable;

public class Person implements Serializable {
	private String name;
	private Integer Age;
	private String occupation;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getAge() {
		return Age;
	}

	public void setAge(Integer age) {
		Age = age;
	}

	public String getOccupation() {
		return occupation;
	}

	public void setOccupation(String occupation) {
		this.occupation = occupation;
	}

	@Override public String toString() {
		return "Person{" +
				"name='" + name + '\'' +
				", Age=" + Age +
				", occupation='" + occupation + '\'' +
				'}';
	}
}