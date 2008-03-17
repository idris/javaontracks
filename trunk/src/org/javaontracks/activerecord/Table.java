package org.javaontracks.activerecord;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Properties;

import javax.sql.DataSource;

public class Table<T extends ActiveRecordBase<T>> {
	public final String name;
	public final Hashtable<String, Column> columns;
	public final ArrayList<String> columnNames;
	public final String primaryKey;
	public final Class<T> tableClass;
	public String primarySequence;
	public final Hashtable<String, Relationship> hasMany;
	public final Hashtable<String, Relationship> belongsTo;
//	public final String dbURL;
//	public static Class<Driver> driverClass;
	public final String packageName;
	public static final HashMap<String, DataSource> sources = new HashMap<String, DataSource>();

	public Table(Class<T> arClass) {
		tableClass = arClass;
		name = ClassUtil.getTableName(tableClass);
		primaryKey = ClassUtil.removeCamelCase(tableClass.getSimpleName()) + "id";
		//TODO: get a real primarySequence. not like this.
		primarySequence = "seq_" + name + "_" + primaryKey;
		hasMany = new Hashtable<String, Relationship>();
		belongsTo = new Hashtable<String, Relationship>();
		columns = new Hashtable<String, Column>();
		columnNames = new ArrayList<String>();
		String pName = tableClass.getPackage().getName();
		packageName = pName.substring(pName.lastIndexOf('.')+1);

		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			con = getConnection();
			st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			rs = st.executeQuery("SELECT * FROM " + name + " limit 1");
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
			ActiveRecordBase.close(rs);
			ActiveRecordBase.close(st);

			DatabaseMetaData dmd = con.getMetaData();
			rs = dmd.getColumns(packageName, "public", name, null);
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
			ActiveRecordBase.close(rs);

			rs = dmd.getExportedKeys(packageName, "public", name); //for has_many
			while(rs.next()) {
				String otherTable = rs.getString("FKTABLE_NAME");
				String otherKey = rs.getString("FKCOLUMN_NAME");
				System.out.println(name + " has_many " + otherTable + " thru " + otherKey);
				hasMany(otherTable, otherKey);
//				System.out.println("PK: " + rs.getString("PKTABLE_NAME") + " - " + rs.getString("PK_NAME"));
//				System.out.println("FK: " + rs.getString("FKTABLE_NAME") + " - " + rs.getString("FK_NAME"));
//				System.out.println("columns: " + rs.getString("PKCOLUMN_NAME") + " " + rs.getString("FKCOLUMN_NAME"));
			}
			ActiveRecordBase.close(rs);
			rs = dmd.getImportedKeys(packageName, "public", name); //for belongs_to
			while(rs.next()) {
				String otherTable = rs.getString("PKTABLE_NAME");
				String foreignKey = rs.getString("FKCOLUMN_NAME");
				System.out.println(name + " belongs_to " + otherTable + " thru " + foreignKey + " (pk=" + rs.getString("PKCOLUMN_NAME") + ")");
				belongsTo(otherTable, rs.getString("PKCOLUMN_NAME"), foreignKey);
//				System.out.println("PK: " + rs.getString("PKTABLE_NAME") + " - " + rs.getString("PK_NAME"));
//				System.out.println("FK: " + rs.getString("FKTABLE_NAME") + " - " + rs.getString("FK_NAME"));
//				System.out.println("columns: " + rs.getString("PKCOLUMN_NAME") + " " + rs.getString("FKCOLUMN_NAME"));
			}

			ActiveRecordBase.close(rs);
			ActiveRecordBase.close(con);

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
		Class<?> otherClass = Class.forName(tableClass.getPackage().getName() + "." + ClassUtil.getClassName(otherTable));
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

	protected synchronized DataSource initializeSource() {
		DataSource source = sources.get(packageName);
		if(source != null) return source;
		Properties props = new Properties();
		FileInputStream fis;
		String propertiesFilename = packageName + ".properties";
		try {
			fis = new FileInputStream(propertiesFilename);
			props.load(fis);
			fis.close();
		} catch(Exception ex) {
			System.out.println("ActiveRecord: could not load properties file (" + propertiesFilename + ")");
		}
		String driverType = props.getProperty("driverType", "postgres");
		try {
			Class driver;
			if(driverType.equals("oracle")) {
				driver = Class.forName("org.javaontracks.activerecord.drivers.OracleDataSource");
//				source = new OracleDataSource(packageName, props);
			} else if(driverType.equals("mysql")) {
				driver = Class.forName("org.javaontracks.activerecord.drivers.MysqlDataSource");
//				source = new MysqlDataSource(packageName, props);
			} else { // if(driverType.equals("postgres")) {
				driver = Class.forName("org.javaontracks.activerecord.drivers.PostgreSQLDataSource");
//				source = new PostgreSQLDataSource(packageName, props);
			}
			source = (DataSource)driver.getConstructor(String.class, Properties.class).newInstance(packageName, props);
		} catch(Exception ex) {
			ex.printStackTrace();
		}
		sources.put(packageName, source);
		return source;
	}

	protected DataSource getSource() {
		DataSource source = sources.get(packageName);
		if(source == null) {
			source = initializeSource();
		}
		return source;
	}

	public Connection getConnection() {
		try {
			return getSource().getConnection();
		} catch (Exception ex) {
			System.err.println("ERROR: ActiveRecord could not connect to database.\n" + ex.getMessage());
			ex.printStackTrace();
		}
		return null;
	}
}