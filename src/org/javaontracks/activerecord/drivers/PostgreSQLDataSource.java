package org.javaontracks.activerecord.drivers;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Vector;

import javax.sql.DataSource;
import javax.sql.PooledConnection;

import org.postgresql.ds.PGConnectionPoolDataSource;
import org.postgresql.ds.PGPoolingDataSource;

/**
 * @author idris
 * requires postgresql driver: http://jdbc.postgresql.org/download.html
 */
public class PostgreSQLDataSource implements DataSource {
	// PGConnectionPoolDataSource source = null;
	// Vector<PooledConnection> connections;
	PGPoolingDataSource source = null;

	public PostgreSQLDataSource(String packageName, Properties props) {
		source = new PGPoolingDataSource();
		source.setDataSourceName("ActiveRecord-" + packageName);
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
			source.setMaxConnections(Integer.parseInt(props.getProperty("maxConnections", "50")));
		} catch (Exception ex) {
		}
		try {
			source.setInitialConnections(Integer.parseInt(props.getProperty("initialConnections", "5")));
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