package db;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DB {

	private static Connection conn = null;

	public static Connection getConnection() {// implementação do método para gerar conexão no bd
		if (conn == null) {// se conexão ainda é nula
			try {
				Properties props = loadProperties(); // propriedades do banco
				String url = props.getProperty("dburl");// url do banco
				conn = DriverManager.getConnection(url, props);// conexão com o banco (instanciar um obj do tipo
																// connection)
			} catch (SQLException e) {
				throw new DbException(e.getMessage());
			}
		}
		return conn;
	}

	public static void closeConnection() { // para fechar a conexão com o banco
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				throw new DbException(e.getMessage());
			}
		}
	}

	//recupera as propriedades do BD
	private static Properties loadProperties() {
		try (FileInputStream fs = new FileInputStream("db.properties")) {
			Properties props = new Properties();
			props.load(fs);
			return props;
		} catch (IOException e) {
			throw new DbException(e.getMessage());
		}
	}

	//método para fechar o st mas tratando as exceptions
	public static void closeStatement(Statement st) { 
		if(st != null) {
			try {
				st.close();
			} catch (SQLException e) {
				throw new DbException(e.getMessage());
			}
		}
	}
	
	//método para fechar o rs mas tratando as exceptions
		public static void closeResultSet(ResultSet rs) { 
			if(rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					throw new DbException(e.getMessage());
				}
			}
		}
}
