package edu.ucla.cs.wis.bigdatalog.compiler.aggregate;

import java.util.ArrayList;
import java.util.List;

public enum BuiltInAggregateType {
	SUM(1, "sum"),
	COUNT(2, "count"),
	AVG(3, "avg"),
	MAX(4, "max"),
	MIN(5, "min"),
	COUNT_DISTINCT(6, "countd");
	
	private int id;
	private String name;
	
	private BuiltInAggregateType(int id, String name) {
		this.id = id;
		this.name = name;
	}
	
	public int getId() { return this.id; }
	
	public String getName() { return this.name; }
	
	public static BuiltInAggregateType getBuiltInAggregateType(int id) {
		for (BuiltInAggregateType type : BuiltInAggregateType.values()) {
			if (type.getId() == id)
				return type;
		}
		return null;
	}
	
	public static BuiltInAggregateType getBuiltInAggregateType(String name) {
		for (BuiltInAggregateType type : BuiltInAggregateType.values()) {
			if (type.getName().equals(name))
				return type;
		}
		return null;
	}
	
	public static List<String> getBuiltInAggregateFunctionNames() {
		List<String> names = new ArrayList<>();
		for (BuiltInAggregateType type : BuiltInAggregateType.values())
			names.add(type.name);
		
		return names;
	}
}
