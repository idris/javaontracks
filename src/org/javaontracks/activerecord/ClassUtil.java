package org.javaontracks.activerecord;

import org.javaontracks.util.EnglishNoun;

public abstract class ClassUtil {
	public static String getTableName(Class<?> c) {
		String name = removeCamelCase(c.getSimpleName());
		return pluralOf(name);
	}

	public static String getClassName(String otherTable) {
		//find a real way to get signular. this is unreliable.. assumes otherTable.chomp!
		String sing = "";// = otherTable.substring(0, otherTable.length()-1);
		String[] words = otherTable.split("_");
		for(int i=0;i<words.length-1;i++) {
			String word = words[i];
			word = Character.toUpperCase(word.charAt(0)) + word.substring(1).toLowerCase();
			sing += word;
		}
		String lastWord = words[words.length-1].toLowerCase();
		lastWord = singularOf(lastWord);
		lastWord = Character.toUpperCase(lastWord.charAt(0)) + lastWord.substring(1).toLowerCase();
		sing += lastWord;
		return toCamelCase(sing);
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

	public static String pluralOf(String word) {
		return EnglishNoun.pluralOf(word);
	}

	public static String singularOf(String word) {
		return EnglishNoun.singularOf(word);
	}
}