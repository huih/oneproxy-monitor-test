import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.log4j.PropertyConfigurator;

public class TestMain {
  
    public static void main(String[] args) {
    	PropertyConfigurator.configure("log4j.properites");
    	Handler fh;
		try {
			fh = new FileHandler("./oracle_jdbc_log.log");
			fh.setLevel(Level.ALL);
			fh.setFormatter(new SimpleFormatter());
			Logger.getLogger("").addHandler(fh);
			Logger.getLogger("").setLevel(Level.ALL);
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long bt = 0, at = 0;
		
		bt = System.nanoTime();
		
		Connection conn = null;
    	
//		String url = "jdbc:sqlserver://127.0.0.1:9999;databaseName=sqldb;SelectMethod=Cursor";
//		String url = "jdbc:sqlserver://192.168.7.78:9999;databaseName=sqldb;SelectMethod=Cursor";
		String url = "jdbc:sqlserver://127.0.0.1:9999;databaseName=sqldb;";
    	String username = "admin";
    	String password = "0000";
    	
    	try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			conn = DriverManager.getConnection(url, username, password);
			
			TestSqlServerPreparedStatement tssps = new TestSqlServerPreparedStatement();
			tssps.test_select_bak_db(conn);
			tssps.test_select_master_db(conn);
			tssps.test_update_read_slave_db(conn);
			tssps.test_select_master_db2(conn);
			
			TestSqlServerStatement tsss = new TestSqlServerStatement();
			tsss.test_select_from_slave(conn);
			tsss.test_select_from_master(conn);
			tsss.test_select_slave_master(conn);
			tsss.test_select_master_slave_master(conn);
			tsss.test_select_slave_master_begin(conn);
				
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		at = System.nanoTime();
		long t = at - bt;
		System.out.println("sum Time:" + t + " ns, " + t/1000000 + " ms, " + t/1000000000 + " s");
    }
}