package br.com.joaoborges.database;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Driver;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author joaoborges
 */
public class DatabaseInfoCache {

	public static final String ORACLE_DRIVER = "oracle.jdbc.OracleDriver";
	public static final String POSTGRESQL_DRIVER = "org.postgresql.Driver";
	
	private static Properties properties = new Properties();
	private static ConcurrentHashMap<String, ConnectionInfo> Connection_Info_MAP = new ConcurrentHashMap<>();
	private static ConcurrentHashMap<String, Driver> DRIVER_INSTANCE_MAP = new ConcurrentHashMap<>();

	static {
		String propFile = System.getProperty("databaseConfigFile");
		if (propFile == null || propFile.length() == 0) {
			throw new UnsupportedOperationException("no configuration file");
		}
		File file = new File(propFile);
		if (!file.exists()) {
			throw new UnsupportedOperationException("configuration file not found " + propFile);
		}

		try (FileInputStream fis = new FileInputStream(file)) {
			properties.load(fis);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	public static boolean hasDatabase(String url) {
		return properties.containsKey(url);
	}

	public static ConnectionInfo findConnectionInfo(String url) {
		if (Connection_Info_MAP.get(url) == null) {
			String connectionInfo = properties.getProperty(url);
			if (connectionInfo == null || connectionInfo.isEmpty()) {
				throw new IllegalArgumentException("no info for " + url);
			}
			String[] info = connectionInfo.split("\\|");
			Driver driver = null;
			try {
				if (info[0].contains("jdbc:oracle:thin")) {
					driver = DRIVER_INSTANCE_MAP.get("oracle");
					if (driver == null) {
						driver = (Driver) Class.forName(ORACLE_DRIVER).newInstance();
						DRIVER_INSTANCE_MAP.put("oracle", driver);
					}
				} else if (info[0].contains("jdbc:postgresql")) {
					driver = DRIVER_INSTANCE_MAP.get("postgresql");
					if (driver == null) {
						driver = (Driver) Class.forName(POSTGRESQL_DRIVER).newInstance();
						DRIVER_INSTANCE_MAP.put("postgresql", driver);
					}
				} else {
					throw new IllegalArgumentException("unsupported driver url " + info[0]);
				}
			} catch (Exception e) {
				throw new RuntimeException(e.getMessage(), e);
			}

			ConnectionInfo cInfo = new ConnectionInfo();
			cInfo.driver = driver;
			cInfo.url = info[0];
			cInfo.username = info[1];
			cInfo.password = info[2];
			Connection_Info_MAP.put(url, cInfo);
		}
		return Connection_Info_MAP.get(url);
	}

	public static class ConnectionInfo {
		public Driver driver;
		public String url;
		public String username;
		public String password;
	}
}
