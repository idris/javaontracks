package com.javaontracks.activerecord;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;
import java.util.Iterator;

import org.jvnet.inflector.Noun;

import com.javaontracks.cache.MemCache;

public abstract class ActiveRecord<T extends ActiveRecord<T>> implements Serializable {
//	private String tableName;
	protected Hashtable<String, Object> attributes;
//	protected Hashtable<String, Object> defaultAttributes; //TODO: add this in later... maybe
//	protected Hashtable<String, Class<?>> columns;
//	protected String primaryKey;
//	protected String sequence = null;
//	private Hashtable<String, Relationship> hasManyTables = new Hashtable<String, Relationship>();
//	private Hashtable<String, Relationship> belongsToTables = new Hashtable<String, Relationship>();

	private Hashtable<String, Vector<Integer>> hasManyIDs = new Hashtable<String, Vector<Integer>>();
	private Hashtable<String, Integer> belongsToIDs = new Hashtable<String, Integer>();

	private transient Hashtable<String, Vector<? extends ActiveRecord<?>>> hasMany;
	private transient Hashtable<String, ActiveRecord<?>> belongsTo;

	private transient String parent = null;

	private Vector<String> modified;

	protected static final int CACHE_TIME = 1000*60*60*5;

	private transient Table<T> table;

	protected static Hashtable<Class<?>, Table<?>> tables = new Hashtable<Class<?>, Table<?>>();

	public static boolean logQueries = false;

	public ActiveRecord() {
//		tableName = ClassUtil.getTableName(getClass());
//		primaryKey = ClassUtil.removeCamelCase(getClass().getSimpleName()) + "id";
		hasMany = new Hashtable<String, Vector<? extends ActiveRecord<?>>>();
		belongsTo = new Hashtable<String, ActiveRecord<?>>();
		Class<?> c = getClass();
		Table t = (Table<T>)(tables.get(c));
		if(t == null) {
			t = new Table(c);
			tables.put(c, t);
		}
		table = t;
//		loadTableDefaults(); //(do this only once per unique class)
		attributes = table.getDefaultAttributes();
		modified = new Vector<String>();
	}

	protected Connection getConnection() {
		return table.getConnection();
	}

	private void loadHasMany(String otherTable) throws Exception {
		Vector<Integer> ids = hasManyIDs.get(otherTable);
		Relationship r = table.hasMany.get(otherTable);
		ActiveRecord<?> other = (ActiveRecord<?>)(r.foreignClass.getConstructor().newInstance());
		if(ids == null) {
			Vector<? extends ActiveRecord<?>> kids = other.lookupWithCache(r.foreignKey + "=?", getPrimaryKey());
			Vector<Integer> kidIDs = new Vector<Integer>(kids.size());
			for(ActiveRecord<?> kid: kids) {
				kidIDs.add(kid.getPrimaryKey());
				kid.setParent(r.foreignKey, this);
			}
			hasManyIDs.put(otherTable, kidIDs);
			hasMany.put(otherTable, kids);
			cache();
			return;
		}

		Vector<ActiveRecord<?>>kids = new Vector<ActiveRecord<?>>();
		for(int id: ids) {
			ActiveRecord<?> kid = other.lookup(id);
			kids.add(kid);
		}
		hasMany.put(otherTable, kids);

//		Relationship r = table.hasMany.get(otherTable);
//		ActiveRecord<?> other = (ActiveRecord<?>)(r.foreignClass.getConstructor().newInstance());
////		Hashtable<String, Object> where = new Hashtable<String, Object>(1);
////		where.put(r.foreignKey, attributes.get(table.primaryKey));
//		Vector<? extends ActiveRecord<?>> kids = other.lookupWithCache(r.foreignKey + "=?", getPrimaryKey());
//		hasMany.put(otherTable, kids);
	}

	private void loadBelongsTo(String otherTable) throws Exception {
		Relationship r = table.belongsTo.get(otherTable);
		Integer otherID = (Integer)(attributes.get(r.foreignKey));
		if(otherID == null) {
			return;
		}
		ActiveRecord<?> other = (ActiveRecord<?>)(r.foreignClass.getConstructor().newInstance());
		belongsTo.put(otherTable, other.lookup(otherID.intValue()));
	}

	private void setParent(String foreignKey, ActiveRecord<?> obj) {
		String ref = ClassUtil.keyToReference(foreignKey);
		parent = ref;
		belongsTo.put(parent, obj);
	}

	public int getPrimaryKey() {
		return get(table.primaryKey);
	}

	public int getInt(String field) {
		try {
			return get(field);
		} catch(Exception ex) {
			System.out.println("getting " + field + " from " + table.name + " " + getPrimaryKey());
			System.out.println("value is " + get(field));
			throw new NullPointerException("getInt Error");
		}
	}

	public boolean getBoolean(String field) {
		return get(field);
	}

	public double getDouble(String field) {
		return get(field);
	}

	public <E> E get(String field) {
		if(table.columnNames.contains(field)) {
			return (E)(attributes.get(field));
		} else if(table.belongsTo.get(field) != null) {
			E ret = (E)(belongsTo.get(field));
			if(ret == null) {
				try {
					loadBelongsTo(field);
					ret = (E)(belongsTo.get(field));
				} catch(Exception ex) {
					//TODO: do something
					ex.printStackTrace();
				}
			}
			return ret;
		} else if(table.hasMany.get(field) != null) {
			if(hasMany.get(field) == null) {
				try {
					loadHasMany(field);
				} catch(Exception ex) {
					//TODO: do something
					ex.printStackTrace();
				}
			}
			return (E)(hasMany.get(field));
		}
		return null;
	}

	public void set(String field, Object value) {
		set(field, value, true);
	}

	public void set(String field, Object value, boolean modify) {
		try {
			//double check to make sure the class of Object is right
			if(field.equals(table.primaryKey) && attributes.get(table.primaryKey) != null) throw new Exception("Cannot change primary key: " + table.primaryKey);
			if(value == null) attributes.remove(field);
			Column col = table.columns.get(field);
			if(col != null) {
				Class<?> c = table.columns.get(field).type;
				if(value == null) {
					attributes.remove(field);
				} else if(value.getClass() != c) {
					throw new Exception("Tried to set '" + field + "' with class " + value.getClass().getName() + " but expected " + c.getName());
				} else {
					attributes.put(field, value);
				}
				if(modify) modify(field);
			} else if(table.belongsTo.get(field) != null) {
				Relationship r = table.belongsTo.get(field);
				if(value == null) {
					set(r.foreignKey, null);
					belongsTo.remove(field);
				} else {
					set(r.foreignKey, ((ActiveRecord<?>)value).getPrimaryKey());
					belongsTo.put(field, (ActiveRecord<?>)value);
				}
				if(modify) modify(r.foreignKey);
			} else {
				throw new Exception("Column '" + field + "' doesn't exist in " + table.name + ".");
			}
		} catch (Exception ex) {
			System.out.println("Warning: ActiveRecord could not set " + field);
			ex.printStackTrace();
		}
	}

	public void toggle(String field) {
		set(field, !getBoolean(field));
	}

	public void increment(String field) {
		set(field, getInt(field)+1);
	}

	public void decrement(String field) {
		set(field, getInt(field)-1);
	}

	public int executeUpdate(String set, String where) throws SQLException {
		Connection con = table.getConnection();
		Statement st = con.createStatement();
		int ret = st.executeUpdate("UPDATE " + table.name + " SET " + set + " WHERE " + where);
		st.close();
		con.close();
		return ret;
	}

	public int executeUpdate(String set, String where, Object... vals) throws SQLException {
		Connection con = table.getConnection();
		PreparedStatement ps = con.prepareStatement("UPDATE " + table.name + " SET " + set + " WHERE " + where);
		for(int i=1;i<=vals.length;i++) {
			ps.setObject(i, vals[i-1]);
		}
		int ret = ps.executeUpdate();
		ps.close();
		con.close();
		return ret;
	}

	public int executeDelete(String where, Object... vals) throws SQLException {
		Connection con = table.getConnection();
		PreparedStatement ps = con.prepareStatement("DELETE FROM " + table.name + " WHERE " + where);
		for(int i=1;i<=vals.length;i++) {
			ps.setObject(i, vals[i-1]);
		}
		int ret = ps.executeUpdate();
		ps.close();
		con.close();
		return ret;
	}

	public void setAll(Vector<T> els, Object... pairs) throws SQLException {
		String query = "UPDATE " + table.name + " SET ";
		String qs = "";
		for(int key=0;key<pairs.length/2;key+=2) {
			qs += ", " + pairs[key] + "=?";
		}
		qs = qs.substring(2);
		query += qs;
		qs = "";
		for(int i=0;i<els.size();i++) {
			qs += ",?";
		}
		qs = qs.substring(1);
		query += " WHERE " + table.primaryKey + " IN (" + qs + ")";
		Connection con = table.getConnection();
		PreparedStatement ps = con.prepareStatement(query);
		int i = 0;
		for(int val=1;val<pairs.length/2;val+=2) {
			ps.setObject(++i, pairs[val]);
		}
		for(T el: els) {
			ps.setInt(++i, el.getPrimaryKey());
		}
		ps.executeUpdate();
		ps.close();
		con.close();
	}

	public void reset() throws SQLException {
		Connection con = table.getConnection();
		PreparedStatement ps = con.prepareStatement("SELECT * FROM " + table.name + " WHERE " + table.primaryKey + "=?");
		ps.setInt(1, getPrimaryKey());
		ResultSet rs = ps.executeQuery();
		if(rs.next()) {
			readRow(rs);
		}
		rs.close();
		ps.close();
		con.close();
		hasMany.clear();
		belongsTo.clear();
		hasManyIDs.clear();
		belongsToIDs.clear();
		cache();
	}

	public void reset(String field) {
		if(table.belongsTo.get(field) != null) {
			belongsTo.remove(field);
			belongsToIDs.remove(field);
		} else if(table.hasMany.get(field) != null) {
			hasMany.remove(field);
			hasManyIDs.remove(field);
		}
		cache();
	}

	private void modify(String field) {
		if(modified.contains(field)) return;
		modified.add(field);
	}

	public void readRow(ResultSet rs) throws SQLException {
		for (String col: table.columnNames) {
			set(col, rs.getObject(col), false);
		}
	}

	public Vector<T> lookup() throws SQLException {
		Vector<Integer> ids = (Vector<Integer>)(MemCache.get(table.name + "_ids"));
		if(ids == null) {
			ids = new Vector<Integer>();
			Connection con = getConnection();
			String query = "select " + table.primaryKey + " from " + table.name;
			Vector<Object> vals = new Vector<Object>();
			if(logQueries) {
				System.out.println(query);
			}
			PreparedStatement ps = con.prepareStatement(query);
			int i = 0;
			for(Object val: vals) {
				ps.setObject(++i, val);
			}
			ResultSet rs = ps.executeQuery();
			while(rs.next()) {
				ids.add(rs.getInt(1));
			}
			rs.close();
			ps.close();
			con.close();
			MemCache.set(table.name + "_ids", ids, CACHE_TIME);
		}
		Vector<T> ret = new Vector<T>();
		for(int id: ids) {
			ret.add(lookup(id));
		}
		return ret;
	}

	public T lookup(int id) throws SQLException {
		T obj = (T)(MemCache.get(getMemCachedKey(id)));
		if(obj == null) {
			Hashtable<String, Object> map = new Hashtable<String, Object>(1);
			map.put(table.primaryKey, id);
			Vector<T> result = lookup(map);
			if(result.size() > 0) {
				obj = result.get(0);
				if(obj != null) MemCache.set(getMemCachedKey(id), obj, CACHE_TIME);
			}
		}
		return obj;
	}

	public Vector<T> lookup(String where, Object... vals) throws SQLException {
		Connection con = getConnection();
		String query = "select * from " + table.name;
		if(where != null) {
			query += " WHERE " + where;
		}
		PreparedStatement ps = con.prepareStatement(query);
		if(logQueries) {
			System.out.println(ps);
		}
		int i = 0;
		for(Object val: vals) {
			ps.setObject(++i, val);
		}
		ResultSet rs = ps.executeQuery();
		Vector<T> ret = read(rs);
		rs.close();
		ps.close();
		con.close();
		return ret;
	}

	public Vector<T> lookupWithCache(int limit, int offset, String where, Object... vals) throws SQLException {
		Connection con = getConnection();
		String query = "SELECT " + table.primaryKey + " FROM " + table.name;
		if(where != null) {
			query += " WHERE " + where;
		}
		query += " LIMIT " + limit + " OFFSET " + offset;
		PreparedStatement ps = con.prepareStatement(query);
		if(logQueries) {
			System.out.println(ps);
		}
		int i = 0;
		for(Object val: vals) {
			ps.setObject(++i, val);
		}
		ResultSet rs = ps.executeQuery();
		Vector<T> ret = new Vector<T>();
		while(rs.next()) {
			ret.add(lookup(rs.getInt(1)));
		}
		rs.close();
		ps.close();
		con.close();
		return ret;
	}

	public Vector<T> lookupWithCache(String where, Object... vals) throws SQLException {
		Connection con = getConnection();
		String query = "select " + table.primaryKey + " from " + table.name;
		if(where != null) {
			query += " WHERE " + where;
		}
		PreparedStatement ps = con.prepareStatement(query);
		if(logQueries) {
			System.out.println(ps);
		}
		int i = 0;
		for(Object val: vals) {
			ps.setObject(++i, val);
		}
		ResultSet rs = ps.executeQuery();
		Vector<T> ret = new Vector<T>();
		while(rs.next()) {
			ret.add(lookup(rs.getInt(1)));
		}
		rs.close();
		ps.close();
		con.close();
		return ret;
	}

	public Vector<T> lookup(int limit, int offset, String where, Object... vals) throws SQLException {
		Connection con = getConnection();
		String query = "SELECT * FROM " + table.name;
		if(where != null) {
			query += " WHERE " + where;
		}
		query += " LIMIT " + limit + " OFFSET " + offset;
		PreparedStatement ps = con.prepareStatement(query);
		if(logQueries) {
			System.out.println(ps);
		}
		int i = 0;
		for(Object val: vals) {
			ps.setObject(++i, val);
		}
		ResultSet rs = ps.executeQuery();
		Vector<T> ret = read(rs);
		rs.close();
		ps.close();
		con.close();
		return ret;
	}

	public Vector<T> lookup(Map<String, Object> where) throws SQLException {
		Connection con = getConnection();
		String query = "select * from " + table.name;
		Vector<Object> vals = new Vector<Object>();
		String whereClause = "";
		if(where != null) {
			for(String col: table.columnNames) {
				Object val = where.get(col);
				if(val == null) continue;
//				if(val.getClass() != columns.get(col).getClass()) continue;
				whereClause += " AND " + col + "=?";
				vals.add(val);
			}
			if(whereClause.length() > 5) {
				query += " WHERE" + whereClause.substring(4);
			}
		}
		if(logQueries) {
			System.out.println(query);
		}
		PreparedStatement ps = con.prepareStatement(query);
		int i = 0;
		for(Object val: vals) {
			ps.setObject(++i, val);
		}
		ResultSet rs = ps.executeQuery();
		Vector<T> ret = read(rs);
		rs.close();
		ps.close();
		con.close();
		return ret;
	}

	public Vector<T> lookup(Map<String, Object> where, String orderBy) throws SQLException {
		return lookup(where, orderBy, true);
	}

	public Vector<T> lookup(Map<String, Object> where, String orderBy, boolean ascending) throws SQLException {
		Connection con = getConnection();
		String query = "select * from " + table.name;
		Vector<Object> vals = new Vector<Object>();
		String whereClause = "";
		if(where != null) {
			for(String col: table.columnNames) {
				Object val = where.get(col);
				if(val == null) continue;
//				if(val.getClass() != columns.get(col).getClass()) continue;
				whereClause += " AND " + col + "=?";
				vals.add(val);
			}
			if(whereClause.length() > 5) {
				query += " WHERE" + whereClause.substring(4);
			}
		}
		query += " ORDER BY " + orderBy;
		query += ascending?" ASC":" DESC";
		if(logQueries) {
			System.out.println(query);
		}
		PreparedStatement ps = con.prepareStatement(query);
		int i = 0;
		for(Object val: vals) {
			ps.setObject(++i, val);
		}
		ResultSet rs = ps.executeQuery();
		Vector<T> ret = read(rs);
		rs.close();
		ps.close();
		con.close();
		return ret;
	}

	protected Vector<T> read(ResultSet rs) throws SQLException {
		Vector <T> ret = new Vector<T>();
		while (rs.next()) {
			try {
				T obj = (T)(getClass().getConstructor().newInstance());
				obj.readRow(rs);
				ret.add(obj);
			} catch (Exception ex) {
				System.out.println("Error: ActiveRecord could not instantiate: " + getClass().getName());
				ex.printStackTrace();
			}
		}
		return ret;
	}

	public void store() throws SQLException {
		String columnNames = "";
		String columnValues = "";
		modify(table.primaryKey);
		for(String col: modified) {
			columnNames += "," + col;
			columnValues += ",?";
		}
		columnNames = columnNames.substring(1);
		columnValues = columnValues.substring(1);
		String query = "INSERT INTO " + table.name + " (" + columnNames + ") VALUES (" + columnValues + ")";
		if(logQueries) {
			System.out.println(query);
		}
		Connection con = getConnection();
		if(attributes.get(table.primaryKey) == null) {
			PreparedStatement ps = con.prepareStatement("SELECT nextval(?)");
			ps.setString(1, table.name + "_" + table.primaryKey + "_seq");
			ResultSet rs = ps.executeQuery();
			if(rs.next()) {
				attributes.put(table.primaryKey, rs.getInt(1));
			}
			ps.close();
		}
		PreparedStatement ps = con.prepareStatement(query);
		int i = 0;
		for(String col: modified) {
			ps.setObject(++i, attributes.get(col));
		}
		ps.executeUpdate();
		ps.close();
		con.close();
		cache();
		resetParents();
		modified.removeAllElements();
	}

	public void update() throws SQLException {
		if(modified.size() == 0) {
			return;
		}
		String updateStr = "";
		for(String col: modified) {
			updateStr += "," + col + "=?";
		}
		updateStr = updateStr.substring(1);
		String query = "UPDATE " + table.name + " SET " + updateStr + " WHERE " + getPrimaryKeyCondition();
		if(logQueries) {
			System.out.println(query);
			System.out.println("modified (" + modified.size() + "): " + modified);
		}
		Connection con = getConnection();
		PreparedStatement ps = con.prepareStatement(query);
		int i = 0;
		for(String col: modified) {
			ps.setObject(++i, attributes.get(col));
		}
		ps.setObject(++i, attributes.get(table.primaryKey));
		ps.executeUpdate();
		ps.close();
		con.close();
		modified.removeAllElements();
		cache();
	}

	public void delete() throws SQLException {
		uncache();
		resetParents();
		Connection con = getConnection();
		String query = "DELETE FROM " + table.name + " WHERE " + getPrimaryKeyCondition();
		if(logQueries) {
			System.out.println(query);
		}
		PreparedStatement ps = con.prepareStatement(query);
		int i = 0;
		ps.setObject(++i, attributes.get(table.primaryKey));
		ps.executeUpdate();
		con.close();
	}

	protected String getPrimaryKeyCondition() {
		return table.primaryKey + "=?";
		/*
		String str = "";
		Iterator<String> it = primaryKeys.iterator();
		while(it.hasNext()) {
			String key = it.next();
			str += " AND " + key + "=?";
		}
		return str.substring(5);
		 */
	}

	public boolean equals(Object o) {
		try {
			if(o.getClass() != getClass()) return false;
			return ((ActiveRecord<T>)o).getPrimaryKey() == getPrimaryKey();
		} catch(Exception ex) {
			return false;
		}
	}

	private void resetParents() {
		Collection<Relationship> prs = table.belongsTo.values();
		for(Relationship r: prs) {
			Integer fk = get(r.foreignKey);
			if(fk == null) continue;
			MemCache.delete(r.table + "-" + fk.toString());
		}
		if(parent != null) {
			((ActiveRecord<?>)(get(parent))).reset(Noun.pluralOf(ClassUtil.keyToReference(parent)));
			parent = null;
		}
	}

	protected void cache() {
		MemCache.set(getMemCachedKey(), this, CACHE_TIME);
	}

	protected void uncache() {
		MemCache.delete(getMemCachedKey());
	}

	protected String getMemCachedKey() {
		return getMemCachedKey(getPrimaryKey());
	}

	protected String getMemCachedKey(int primaryKey) {
		return table.name + "-" + primaryKey;
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		reloadTransients();
	}

	protected void reloadTransients() {
		hasMany = new Hashtable<String, Vector<? extends ActiveRecord<?>>>();
		belongsTo = new Hashtable<String, ActiveRecord<?>>();
		Class<?> c = getClass();
		Table<T> t = (Table<T>)(tables.get(c));
		if(t == null) {
			t = new Table(c);
			tables.put(c, t);
		}
		table = t;
	}
/* just use ps.setObject() instead...
	protected static void setPreparedValue(PreparedStatement ps, int index, Object val) throws SQLException {
		if(val.getClass() == String.class) {
			ps.setString(index, (String)val);
		} else if(val.getClass() == Integer.class) {
			ps.setInt(index, ((Integer)val).intValue());
		} else if(val.getClass() == Boolean.class) {
			ps.setBoolean(index, ((Boolean)val).booleanValue());
		}
	}
*/
}