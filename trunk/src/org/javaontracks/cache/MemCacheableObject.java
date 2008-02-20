package org.javaontracks.cache;

public abstract class MemCacheableObject {
	protected int maxAge = 60 * 60 * 1000;

	public abstract String getKey();

	public java.util.Date getExpiry() {
		return new java.util.Date(System.currentTimeMillis() + maxAge);
	}
}