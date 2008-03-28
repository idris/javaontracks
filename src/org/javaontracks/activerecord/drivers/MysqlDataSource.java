package org.javaontracks.activerecord.drivers;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.io.PrintWriter;

/**
 * @author Idris
 */
public class MysqlDataSource implements DataSource {
	MysqlConnectionPoolDataSource source = null;
	protected int maxConnections = 50;
	protected int initialConnections = 5;

	public MysqlDataSource(String packageName, Properties props) throws SQLException {
		source = new MysqlConnectionPoolDataSource();
//		source.setDataSourceName("ActiveRecord-" + packageName);
		source.setServerName(props.getProperty("serverName"));
		try {
			int portNumber = Integer.parseInt(props.getProperty("port"));
			if(portNumber > 0) {
				source.setPortNumber(portNumber);
			}
		} catch(Exception ex) {}
		source.setDatabaseName(props.getProperty("databaseName"));
		source.setUser(props.getProperty("user"));
		source.setPassword(props.getProperty("password"));
		try {
			maxConnections = Integer.parseInt(props.getProperty("maxConnections", "50"));
		} catch (Exception ex) {
		}
		try {
			initialConnections = Integer.parseInt(props.getProperty("initialConnections", "5"));
		} catch (Exception ex) {
		}
		/*
		 * should use a ConnectionPoolDataSource (like below) manually
		 * source = new PGConnectionPoolDataSource();
		 * source.setServerName(props.getProperty("serverName"));
		 * source.setDatabaseName(props.getProperty("databaseName"));
		 * source.setUser(props.getProperty("user"));
		 * source.setPassword(props.getProperty("password"));
		 * connections = new Vector<PooledConnection>();
		 */
	}

	public Connection getConnection() throws SQLException {
		return source.getConnection();
	}

	public Connection getConnection(String username, String password) throws SQLException {
		return source.getConnection(username, password);
	}

	public PrintWriter getLogWriter() throws SQLException {
		return source.getLogWriter();
	}

	public int getLoginTimeout() throws SQLException {
		return source.getLoginTimeout();
	}

	public void setLogWriter(PrintWriter out) throws SQLException {
		source.setLogWriter(out);
	}

	public void setLoginTimeout(int seconds) throws SQLException {
		source.setLoginTimeout(seconds);
	}
}