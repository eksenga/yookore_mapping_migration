package com.yookos.data.migration;

public class Mapping {
	
	private static Mapping mapping;
	
	private Mapping() {
	}
	
	public static Mapping getInstance() {
		
		if (mapping == null) {
			mapping = new Mapping();
		}
		return mapping;
	}
	
	//public void add()

}
