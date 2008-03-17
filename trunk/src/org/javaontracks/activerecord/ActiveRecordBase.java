package org.javaontracks.activerecord;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

import org.javaontracks.cache.MemCache;
import javax.servlet.ServletRequest;

public abstract class ActiveRecordBase<T extends ActiveRecordBase<T>> implements Serializable {
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

	private transient Hashtable<String, Vector<? extends ActiveRecordBase<?>>> hasMany;
	private transient Hashtable<String, ActiveRecordBase<?>> belongsTo;

	private transient String parent = null;

	private transient Vector<String> modified = new Vector<String>();

	protected static final int CACHE_TIME = 1000*60*60*5;

	private transient Table<T> table;

	protected static HashMap<Class<?>, Table<? extends ActiveRecordBase>> tables = new HashMap<Class<?>, Table<? extends ActiveRecordBase>>();

	public static boolean logQueries = false;

	private transient Connection con = null;

	public ActiveRecordBase() {
//		tableName = ClassUtil.getTableName(getClass());
//		primaryKey = ClassUtil.removeCamelCase(getClass().getSimpleName()) + "id";
		hasMany = new Hashtable<String, Vector<? extends ActiveRecordBase<?>>>();
		belongsTo = new Hashtable<String, ActiveRecordBase<?>>();
		Class<?> c = getClass();
		Table<T> t = (Table<T>)(tables.get(c));
		if(t == null) {
			t = new Table(c);
			tables.put(c, t);
		}
		table = t;
//		loadTableDefaults(); //(do this only once per unique class)
		attributes = table.getDefaultAttributes();
		modified = new Vector<String>();
	}

	protected synchronized Connection getConnection() {
		if(con == null) {
			con = table.getConnection();
		}
		return con;
	}

	private synchronized void loadHasMany(String otherTable) throws Exception {
		Vector<Integer> ids = hasManyIDs.get(otherTable);
		Relationship r = table.hasMany.get(otherTable);
		ActiveRecordBase<?> other = (ActiveRecordBase<?>)(r.foreignClass.newInstance());
		if(ids == null) {
			boolean conWasNull = con == null;
			try {
				con = getConnection();
				Vector<? extends ActiveRecordBase<?>> kids = other.lookupWithCache(r.foreignKey + "=?", getPrimaryKey());
				Vector<Integer> kidIDs = new Vector<Integer>(kids.size());
				for(ActiveRecordBase<?> kid: kids) {
					kidIDs.add(kid.getPrimaryKey());
					kid.setParent(r.foreignKey, this);
				}
				hasManyIDs.put(otherTable, kidIDs);
				hasMany.put(otherTable, kids);
				cache();
				return;
			} finally {
				if(conWasNull) closeCon();
			}
		}

		Vector<ActiveRecordBase<?>>kids = new Vector<ActiveRecordBase<?>>();
		for(int id: ids) {
			ActiveRecordBase<?> kid = other.lookup(id);
			kids.add(kid);
		}
		hasMany.put(otherTable, kids);

//		Relationship r = table.hasMany.get(otherTable);
//		ActiveRecordBase<?> other = (ActiveRecordBase<?>)(r.foreignClass.getConstructor().newInstance());
////		Hashtable<String, Object> where = new Hashtable<String, Object>(1);
////		where.put(r.foreignKey, attributes.get(table.primaryKey));
//		Vector<? extends ActiveRecordBase<?>> kids = other.lookupWithCache(r.foreignKey + "=?", getPrimaryKey());
//		hasMany.put(otherTable, kids);
	}

	private void loadBelongsTo(String otherTable) throws Exception {
		Relationship r = table.belongsTo.get(otherTable);
		Integer otherID = (Integer)(attributes.get(r.foreignKey));
		if(otherID == null) {
			return;
		}
		ActiveRecordBase<?> other = (ActiveRecordBase<?>)(r.foreignClass.newInstance());
		belongsTo.put(otherTable, other.lookup(otherID.intValue()));
	}

	private void setParent(String foreignKey, ActiveRecordBase<?> obj) {
		String ref = ClassUtil.keyToReference(foreignKey);
		parent = ref;
		belongsTo.put(parent, obj);
	}

	public int getPrimaryKey() {
		return getInt(table.primaryKey);
	}

	public String getString(String field) {
		return get(field);
	}

	public int getInt(String field) {
		return ((Integer)get(field)).intValue();
	}

	public boolean getBoolean(String field) {
		return ((Boolean)get(field)).booleanValue();
	}

	public double getDouble(String field) {
		return ((Double)get(field)).doubleValue();
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
					set(r.foreignKey, ((ActiveRecordBase<?>)value).getPrimaryKey());
					belongsTo.put(field, (ActiveRecordBase<?>)value);
				}
				if(modify) modify(r.foreignKey);
			} else {
				throw new Exception("Column '" + field + "' doesn't exist in " + table.name + ".");
			}
		} catch (Exception ex) {
			System.out.println("Warning: ActiveRecordBase could not set " + field);
			ex.printStackTrace();
		}
	}

	public void read(ServletRequest request) {
		for(Column col: table.columns.values()) {
			String val = request.getParameter(col.name);
			if(val == null) {
				continue;
			}
			if(col.type == String.class) {
				set(col.name, val);
			} else if(col.type == Integer.class) {
				set(col.name, Integer.parseInt(val));
			}
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
		int ret;
		Statement st = null;
		boolean conWasNull = con == null;
		try {
			con = getConnection();
			st = con.createStatement();
			ret = st.executeUpdate("UPDATE " + table.name + " SET " + set + " WHERE " + where);
		} finally {
			close(st);
			if(conWasNull) {
				closeCon();
			}
		}
		return ret;
	}

	public int executeUpdate(String set, String where, Object... vals) throws SQLException {
		int ret;
		PreparedStatement ps = null;
		boolean conWasNull = con == null;
		try {
			con = getConnection();
			ps = con.prepareStatement("UPDATE " + table.name + " SET " + set + " WHERE " + where);
			for(int i=1;i<=vals.length;i++) {
				ps.setObject(i, vals[i-1]);
			}
			ret = ps.executeUpdate();
		} finally {
			close(ps);
			if(conWasNull) closeCon();
		}
		return ret;
	}

	public int executeDelete(String where, Object... vals) throws SQLException {
		int ret;
		PreparedStatement ps = null;
		boolean conWasNull = con == null;
		try {
			con = getConnection();
			ps = con.prepareStatement("DELETE FROM " + table.name + " WHERE " + where);
			for(int i=1;i<=vals.length;i++) {
				ps.setObject(i, vals[i-1]);
			}
			ret = ps.executeUpdate();
		} finally {
			close(ps);
			if(conWasNull) closeCon();
		}
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
		PreparedStatement ps = null;
		boolean conWasNull = con == null;
		try {
			con = getConnection();
			ps = con.prepareStatement(query);
			int i = 0;
			for(int val=1;val<pairs.length/2;val+=2) {
				ps.setObject(++i, pairs[val]);
			}
			for(T el: els) {
				ps.setInt(++i, el.getPrimaryKey());
			}
			ps.executeUpdate();
		} finally {
			close(ps);
			if(conWasNull) closeCon();
		}
	}

	public void reset() throws SQLException {
		hasMany.clear();
		belongsTo.clear();
		hasManyIDs.clear();
		belongsToIDs.clear();

		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean conWasNull = con == null;
		try {
			con = getConnection();
			ps = con.prepareStatement("SELECT * FROM " + table.name + " WHERE " + table.primaryKey + "=?");
			ps.setInt(1, getPrimaryKey());
			rs = ps.executeQuery();
			if(rs.next()) {
				readRow(rs);
			}
		} finally {
			close(rs);
			close(ps);
			if(conWasNull) closeCon();
		}
		cache();
	}

	public void reset(String field) {
		if(table.belongsTo.get(field) != null) {
			belongsTo.remove(field);
			belongsToIDs.remove(field);
		} else if(table.hasMany.get(field) != null) {
			hasMany.remove(field);
			hasManyIDs.remove(field);
		} else {
			return;
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
			String query = "SELECT " + table.primaryKey + " FROM " + table.name;
			if(logQueries) {
				System.out.println(query);
			}
			PreparedStatement ps = null;
			ResultSet rs = null;
			boolean conWasNull = con == null;
			try {
				con = getConnection();
				ps = con.prepareStatement(query);
				rs = ps.executeQuery();
				while(rs.next()) {
					ids.add(rs.getInt(1));
				}
			} finally {
				close(rs);
				close(ps);
				if(conWasNull) closeCon();
			}
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
			Vector<T> result = lookup(table.primaryKey + "=?", id);
			if(result.size() > 0) {
				obj = result.get(0);
				if(obj != null) MemCache.set(getMemCachedKey(id), obj, CACHE_TIME);
			}
		}
		return obj;
	}

	public Vector<T> lookup(String where, Object... vals) throws SQLException {
		String query = "select * from " + table.name;
		if(where != null) {
			query += " WHERE " + where;
		}
		if(logQueries) {
			System.out.println(query);
		}

		PreparedStatement ps = null;
		ResultSet rs = null;
		Vector<T> ret;
		boolean conWasNull = con == null;
		try {
			con = getConnection();
			ps = con.prepareStatement(query);
			int i = 0;
			for(Object val: vals) {
				ps.setObject(++i, val);
			}
			rs = ps.executeQuery();
			ret = read(rs);
		} finally {
			close(rs);
			close(ps);
			if(conWasNull) closeCon();
		}
		return ret;
	}

	public Vector<T> lookupWithCache(int limit, int offset, String where, Object... vals) throws SQLException {
		String query = "SELECT " + table.primaryKey + " FROM " + table.name;
		if(where != null) {
			query += " WHERE " + where;
		}
		query += " LIMIT " + limit + " OFFSET " + offset;
		if(logQueries) {
			System.out.println(query);
		}

		PreparedStatement ps = null;
		ResultSet rs = null;
		Vector<T> ret = new Vector<T>();
		boolean conWasNull = con == null;
		try {
			con = getConnection();
			ps = con.prepareStatement(query);
			int i = 0;
			for(Object val: vals) {
				ps.setObject(++i, val);
			}
			rs = ps.executeQuery();
			while(rs.next()) {
				ret.add(lookup(rs.getInt(1)));
			}
		} finally {
			close(rs);
			close(ps);
			if(conWasNull) closeCon();
		}
		return ret;
	}

	public Vector<T> lookupWithCache(String where, Object... vals) throws SQLException {
		String query = "select " + table.primaryKey + " from " + table.name;
		if(where != null) {
			query += " WHERE " + where;
		}
		if(logQueries) {
			System.out.println(query);
		}

		PreparedStatement ps = null;
		ResultSet rs = null;
		Vector<T> ret = new Vector<T>();
		boolean conWasNull = con == null;
		try {
			con = getConnection();
			ps = con.prepareStatement(query);
			int i = 0;
			for(Object val: vals) {
				ps.setObject(++i, val);
			}
			rs = ps.executeQuery();
			while(rs.next()) {
				ret.add(lookup(rs.getInt(1)));
			}
		} finally {
			close(rs);
			close(ps);
			if(conWasNull) closeCon();
		}
		return ret;
	}

	public Vector<T> lookup(int limit, int offset, String where, Object... vals) throws SQLException {
		String query = "SELECT * FROM " + table.name;
		if(where != null) {
			query += " WHERE " + where;
		}
		query += " LIMIT " + limit + " OFFSET " + offset;
		if(logQueries) {
			System.out.println(query);
		}

		PreparedStatement ps = null;
		ResultSet rs = null;
		Vector<T> ret;
		boolean conWasNull = con == null;
		try {
			con = getConnection();
			ps = con.prepareStatement(query);
			int i = 0;
			for(Object val: vals) {
				ps.setObject(++i, val);
			}
			rs = ps.executeQuery();
			ret = read(rs);
		} finally {
			close(rs);
			close(ps);
			if(conWasNull) closeCon();
		}
		return ret;
	}

	public Vector<T> lookup(Map<String, Object> where) throws SQLException {
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

		PreparedStatement ps = null;
		ResultSet rs = null;
		Vector<T> ret;
		boolean conWasNull = con == null;
		try {
			con = getConnection();
			ps = con.prepareStatement(query);
			int i = 0;
			for(Object val: vals) {
				ps.setObject(++i, val);
			}
			rs = ps.executeQuery();
			ret = read(rs);
		} finally {
			close(rs);
			close(ps);
			if(conWasNull) closeCon();
		}
		return ret;
	}

	public Vector<T> lookup(Map<String, Object> where, String orderBy) throws SQLException {
		return lookup(where, orderBy, true);
	}

	public Vector<T> lookup(Map<String, Object> where, String orderBy, boolean ascending) throws SQLException {
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

		PreparedStatement ps = null;
		ResultSet rs = null;
		Vector<T> ret = null;
		boolean conWasNull = con == null;
		try {
			con = getConnection();
			ps = con.prepareStatement(query);
			int i = 0;
			for(Object val: vals) {
				ps.setObject(++i, val);
			}
			rs = ps.executeQuery();
			ret = read(rs);
		} finally {
			close(rs);
			close(ps);
			if(conWasNull) closeCon();
		}
		return ret;
	}

	protected Vector<T> read(ResultSet rs) throws SQLException {
		Vector <T> ret = new Vector<T>();
		while (rs.next()) {
			try {
				T obj = (T)(getClass().newInstance());
				obj.readRow(rs);
				ret.add(obj);
			} catch (Exception ex) {
				System.out.println("Error: ActiveRecordBase could not instantiate: " + getClass().getName());
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
		PreparedStatement ps = null;
		boolean conWasNull = con == null;
		if(attributes.get(table.primaryKey) == null) {
			ResultSet rs = null;
			try {
				con = getConnection();
				ps = con.prepareStatement("SELECT nextval(?)");
				ps.setString(1, table.name + "_" + table.primaryKey + "_seq");
				rs = ps.executeQuery();
				if(rs.next()) {
					attributes.put(table.primaryKey, rs.getInt(1));
				}
			} finally {
				close(rs);
				close(ps);
			}
		}

		try {
			ps = con.prepareStatement(query);
			int i = 0;
			for(String col: modified) {
				ps.setObject(++i, attributes.get(col));
			}
			ps.executeUpdate();
		} finally {
			close(ps);
			if(conWasNull) closeCon();
		}

		cache();
		resetParents();
		modified.removeAllElements();
	}

	public void update() throws SQLException {
		String times = "";
		long startTime = System.currentTimeMillis();
		if(modified.size() == 0) {
			return;
		}
		String updateStr = "";
		for(String col: modified) {
			updateStr += "," + col + "=?";
		}
		updateStr = updateStr.substring(1);
		String query = "UPDATE " + table.name + " SET " + updateStr + " WHERE " + table.primaryKey + "=?";
		if(logQueries) {
			System.out.println(query);
			System.out.println("modified (" + modified.size() + "): " + modified);
		}

		times += "starting connection stuff " + (System.currentTimeMillis()-startTime) + "ms";
		PreparedStatement ps = null;
		boolean conWasNull = con == null;
		try {
			con = getConnection();
			times += ("got connection " + (System.currentTimeMillis()-startTime) + "ms");
			ps = con.prepareStatement(query);
			int i = 0;
			for(String col: modified) {
				ps.setObject(++i, attributes.get(col));
			}
			ps.setObject(++i, attributes.get(table.primaryKey));
			times += ("executing " + (System.currentTimeMillis()-startTime) + "ms");
			ps.executeUpdate();
		} finally {
			close(ps);
			if(conWasNull) closeCon();
		}
		times += ("done. " + (System.currentTimeMillis()-startTime) + "ms");

		modified.removeAllElements();
		cache();
		times += ("cached. " + (System.currentTimeMillis()-startTime) + "ms");
		if(System.currentTimeMillis() - startTime > 3000) System.out.println(times);
	}

	public void delete() throws SQLException {
		uncache();
		resetParents();
		String query = "DELETE FROM " + table.name + " WHERE " + table.primaryKey + "=?";
		if(logQueries) {
			System.out.println(query);
		}
		PreparedStatement ps = null;
		boolean conWasNull = con == null;
		try {
			con = getConnection();
			ps = con.prepareStatement(query);
			ps.setObject(1, getPrimaryKey());
			ps.executeUpdate();
		} finally {
			close(ps);
			if(conWasNull) closeCon();
		}
	}

	public boolean equals(Object o) {
		try {
			if(o.getClass() != getClass()) return false;
			return ((ActiveRecordBase<T>)o).getPrimaryKey() == getPrimaryKey();
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
			((ActiveRecordBase<?>)(get(parent))).reset(ClassUtil.pluralOf(ClassUtil.keyToReference(parent)));
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
		hasMany = new Hashtable<String, Vector<? extends ActiveRecordBase<?>>>();
		belongsTo = new Hashtable<String, ActiveRecordBase<?>>();
		Class<?> c = getClass();
		Table<T> t = (Table<T>)(tables.get(c));
		if(t == null) {
			t = new Table(c);
			tables.put(c, t);
		}
		table = t;
	}

	public synchronized void closeCon() {
		if(con != null) {
			try {
				con.close();
			} catch(Exception ex) {
				
			} finally {
				con = null;
			}
		}
	}

	public static void close(Connection c) {
		if(c != null) {
			try {
				c.close();
			} catch(Exception ex) {

			}
		}
	}
	public static void close(Statement c) {
		if(c != null) {
			try {
				c.close();
			} catch(Exception ex) {

			}
		}
	}
	public static void close(ResultSet c) {
		if(c != null) {
			try {
				c.close();
			} catch(Exception ex) {

			}
		}
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