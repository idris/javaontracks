package com.javaontracks.activerecord;

import org.jvnet.inflector.Noun;

public abstract class ClassUtil {
	public static String getTableName(Class<?> c) {
		String name = removeCamelCase(c.getSimpleName());
		return Noun.pluralOf(name);
	}

	public static String removeCamelCase(String str) {
		char[] name = str.toCharArray();
		String ret = "";
		ret += Character.toLowerCase(name[0]);
		for(int i=1;i<name.length;i++) {
			if(Character.isUpperCase(name[i])) {
				ret += "_" + Character.toLowerCase(name[i]);
			} else {
				ret += name[i];
			}
		}
		return ret;
	}

	public static String toCamelCase(String str) {
		char[] name = str.toCharArray();
		String ret = "";
		ret += Character.toUpperCase(name[0]);
		for(int i=1;i<name.length;i++) {
			if(name[i] == '_') {
				ret += Character.toUpperCase(name[++i]);
			} else {
				ret += name[i];
			}
		}
		return ret;
	}

	public static String keyToReference(String key) {
		return key.substring(0, key.length()-2);
	}
}