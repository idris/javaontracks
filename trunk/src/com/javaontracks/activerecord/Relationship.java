package com.javaontracks.activerecord;

public class Relationship {
	public final String table;
	public final Class<?> foreignClass;
	public final String foreignKey;

	public Relationship(String table, String foreignKey, Class<?> foreignClass) {
		this.table = table;
		this.foreignKey = foreignKey;
		this.foreignClass = foreignClass;
	}
}