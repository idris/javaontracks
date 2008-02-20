/*
 * CacheableObject.java
 *
 * Created on June 2, 2003, 8:02 PM
 */

package org.javaontracks.cache;

/**
 * 
 * @author Zeki
 */
public abstract class CacheableObject extends MemCacheableObject {
	protected long lastUse = System.currentTimeMillis();

	public void touch() {
		lastUse = System.currentTimeMillis();
	}

	public abstract boolean isDirty();

	public boolean isUsed() {
		return !isStale();
	}

	public boolean isStale() {
		return getLastUseAge() > maxAge;
	}

	public long getLastUseAge() {
		return System.currentTimeMillis() - lastUse;
	}

	public abstract void store() throws Exception;

	public void store(java.sql.Connection con) throws Exception {
		store();
	}
}
