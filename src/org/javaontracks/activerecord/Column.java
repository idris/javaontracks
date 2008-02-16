package org.javaontracks.activerecord;

public final class Column {
	public final String name;
	public Object defaultValue;
	public final Class<?> type;
	public boolean allowNulls;

	public Column(String name, Class<?> columnClass) {
		this.name = name;
		this.type = columnClass;
	}

	public Column(String name, Object defaultValue, Class<?> columnClass, boolean allowNulls) {
		this.name = name;
		this.defaultValue = defaultValue;
		this.type = columnClass;
		this.allowNulls = allowNulls;
	}
}
