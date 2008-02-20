package org.javaontracks.cache;

import com.danga.MemCached.*;

public class MemCache {

	static {
		setup();
	}

	static void setup() {
		String[] serverlist = { "localhost:11211" };
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(serverlist);
		pool.setSocketConnectTO(500);
		pool.setSocketTO(500);
		pool.initialize();
	}

	static void setServers(String[] serverList) {
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(serverList);
	}

	public static void set(MemCacheableObject o) {
		MemCachedClient mc = new MemCachedClient();
		mc.set(o.getKey(), o, o.getExpiry());
	}

	public static void set(String key, String s) {
		set(key, s, 1000 * 60 * 60);
	}

	public static void set(String key, Object s, int maxAgeInMillis) {
		MemCachedClient mc = new MemCachedClient();
		mc.set(key, s, new java.util.Date(System.currentTimeMillis() + maxAgeInMillis));
	}

	public static void set(String key, String s, int maxAgeInMillis) {
		MemCachedClient mc = new MemCachedClient();
		mc.set(key, s, new java.util.Date(System.currentTimeMillis() + maxAgeInMillis));
	}

	public static void add(MemCacheableObject o) {
		MemCachedClient mc = new MemCachedClient();
		mc.add(o.getKey(), o, o.getExpiry());
	}

	public static Object get(String key) {
		MemCachedClient mc = new MemCachedClient();
		return mc.get(key);
	}

	public static void delete(MemCacheableObject o) {
//		MemCachedClient mc = new MemCachedClient();
		delete(o.getKey());
	}

	public static void delete(String key) {
		MemCachedClient mc = new MemCachedClient();
		mc.delete(key);
	}
}