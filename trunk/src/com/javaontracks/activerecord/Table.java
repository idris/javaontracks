package com.javaontracks.activerecord;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class Table<T extends ActiveRecord<T>> {
	public final String name;
	public final Hashtable<String, Column> columns;
	public final Vector<String> columnNames;
	public final String primaryKey;
	public final Class<T> tableClass;
	public String primarySequence;
	public final Hashtable<String, Relationship> hasMany;
	public final Hashtable<String, Relationship> belongsTo;

	public Table(Class<T> arClass) {
		tableClass = arClass;
		name = ClassUtil.getTableName(tableClass);
		primaryKey = ClassUtil.removeCamelCase(tableClass.getSimpleName()) + "id";
		//TODO: get a real primarySequence. not like this.
		primarySequence = "seq_" + name + "_" + primaryKey;
		hasMany = new Hashtable<String, Relationship>();
		belongsTo = new Hashtable<String, Relationship>();
		columns = new Hashtable<String, Column>();
		columnNames = new Vector<String>();
		try {
			Connection con = getConnection();
			Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet rs = st.executeQuery("SELECT * FROM " + name + " limit 1");
			ResultSetMetaData md = rs.getMetaData();
			int columnCount = md.getColumnCount();
			for(int i=1;i<=columnCount;i++) {
				String cName = md.getColumnName(i);
				Class<?> c = Class.forName(md.getColumnClassName(i));
	/*
				if("java.lang.String".equals(c.getName())) {
					columns.put(cName, new Column<String>(cName, String.class));
				} else if("java.lang.Integer".equals(c.getName())) {
					columns.put(cName, new Column<Integer>(cName, Integer.class));
				} else if("java.lang.Boolean".equals(c.getName())) {
					columns.put(cName, new Column<Boolean>(cName, Boolean.class));
				}
	*/
				columns.put(cName, new Column(cName, c));
				columnNames.add(cName);
			}
			rs.close();
			st.close();

			DatabaseMetaData dmd = con.getMetaData();
			rs = dmd.getColumns("mudball", "public", name, null);
			while(rs.next()) {
				String name = rs.getString("COLUMN_NAME");
				String def = rs.getString("COLUMN_DEF");
				Object obj = null;
				Column c = columns.get(name);

				if(def != null) {
					try {
						if(c.type == Integer.class) {
							obj = Integer.valueOf(def);
						} else if(def.indexOf("'::") > 0) {
							obj = def.substring(1, def.indexOf("'::"));
						}
					} catch(Exception ex) {
						
					}
				}

				if(obj != null) {
					c.defaultValue = obj;
				}
			}
			rs.close();

			rs = dmd.getExportedKeys("mudball", "public", name); //for has_many
			while(rs.next()) {
				String otherTable = rs.getString("FKTABLE_NAME");
				String otherKey = rs.getString("FKCOLUMN_NAME");
				System.out.println(name + " has_many " + otherTable + " thru " + otherKey);
				hasMany(otherTable, otherKey);
//				System.out.println("PK: " + rs.getString("PKTABLE_NAME") + " - " + rs.getString("PK_NAME"));
//				System.out.println("FK: " + rs.getString("FKTABLE_NAME") + " - " + rs.getString("FK_NAME"));
//				System.out.println("columns: " + rs.getString("PKCOLUMN_NAME") + " " + rs.getString("FKCOLUMN_NAME"));
			}
			rs.close();
			rs = dmd.getImportedKeys("mudball", "public", name); //for belongs_to
			while(rs.next()) {
				String otherTable = rs.getString("PKTABLE_NAME");
				String foreignKey = rs.getString("FKCOLUMN_NAME");
				System.out.println(name + " belongs_to " + otherTable + " thru " + foreignKey + " (pk=" + rs.getString("PKCOLUMN_NAME") + ")");
				belongsTo(otherTable, rs.getString("PKCOLUMN_NAME"), foreignKey);
//				System.out.println("PK: " + rs.getString("PKTABLE_NAME") + " - " + rs.getString("PK_NAME"));
//				System.out.println("FK: " + rs.getString("FKTABLE_NAME") + " - " + rs.getString("FK_NAME"));
//				System.out.println("columns: " + rs.getString("PKCOLUMN_NAME") + " " + rs.getString("FKCOLUMN_NAME"));
			}

			con.close();

	/*
			DatabaseMetaData dmd = con.getMetaData();
			rs = dmd.getPrimaryKeys(null, null, tableName);
			if(rs.next()) {
				primaryKey = rs.getString("COLUMN_NAME");
			}
			rs.close();
			 */
		} catch(Exception ex) {
			//TODO: This is BAD!
			System.err.println("ERROR: ActiveRecord could not load table " + name);
			ex.printStackTrace();
		}
	}

	protected void hasMany(String otherTable, String otherKey) throws ClassNotFoundException {
		//find a real way to get the class. this is unreliable.. assumes otherTable.chomp!
		Class<?> otherClass = Class.forName(tableClass.getPackage().getName() + "." + ClassUtil.toCamelCase(otherTable.substring(0, otherTable.length()-1)));
		String name = otherTable;
		hasMany.put(name, new Relationship(otherTable, otherKey, otherClass));
	}

	protected void belongsTo(String otherTable, String key, String foreignKey) throws ClassNotFoundException {
		String name = key.substring(0, key.length() - 2);
		Class<?> otherClass = Class.forName(tableClass.getPackage().getName() + "." + ClassUtil.toCamelCase(name));
		belongsTo.put(ClassUtil.keyToReference(foreignKey), new Relationship(otherTable, foreignKey, otherClass));
	}
	
	protected Hashtable<String, Object>getDefaultAttributes() {
		Hashtable<String, Object> ret = new Hashtable<String, Object>(columns.size());
		for(Column col: columns.values()) {
			if(col.defaultValue != null) {
				ret.put(col.name, col.defaultValue);
			}
		}
		return ret;
	}

	protected Connection getConnection() {
		Driver drvr = null;
		try {
			Properties props = new Properties();
			FileInputStream fis;
			try {
				fis = new FileInputStream("/raid/www/members.freewebz.com/WEB-INF/classes/ActiveRecord.properties");
			} catch(Exception ex) {
				fis = new FileInputStream("ActiveRecord.properties");
			}
			props.load(fis);
			fis.close();
			String dbURL = props.getProperty(tableClass.getName().substring(0, tableClass.getName().length() - (tableClass.getSimpleName().length() + 1)));
			drvr = (Driver)Class.forName("org.postgresql.Driver").newInstance();
			return drvr.connect(dbURL, new Properties());
		} catch (Exception ex) {
			System.err.println("ERROR: ActiveRecord could not connect to database.\n" + ex.getMessage());
		}
		return null;
	}
}