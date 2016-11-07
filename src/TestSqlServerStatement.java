import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestSqlServerStatement {
	
	private String master_db_type = "master";
	private String slave_db_type = "slave";

	private String getMethodName() {  
        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();  
        StackTraceElement e = stacktrace[2];  
        String methodName = e.getMethodName();  
        return methodName;  
    }
	
	public void test_select_from_slave(Connection conn) throws Exception {
		String sql = "select dbtype from dbo.bigtable where id = 10";
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()) {
			String dbType = rs.getString(1).trim();
			if (!dbType.equals(slave_db_type)) {
				throw new Exception("read data is not from slave");
			}
		}
		System.out.println(getMethodName() + " success");
	}
	
	public void test_select_from_master(Connection conn) throws Exception {
		String sql = "select dbtype from dbo.bigtable where id = 10";
		conn.setAutoCommit(false);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()) {
			String dbType = rs.getString(1).trim();
			if (!dbType.equals(master_db_type)) {
				throw new Exception("read data is not from master");
			}
		}
		rs.close();
		System.out.println(getMethodName() + " success");
		conn.commit();
		conn.setAutoCommit(true);
	}
	
	public void test_select_slave_master(Connection conn) throws Exception {
		String sql = "select dbtype from dbo.bigtable where id = 10";
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()) {
			String dbType = rs.getString(1).trim();
			if (!dbType.equals(slave_db_type)) {
				throw new Exception("read data is not from slave");
			}
		}
		
		conn.setAutoCommit(false);
		rs = stmt.executeQuery(sql);
		while(rs.next()) {
			String dbType = rs.getString(1).trim();
			if (!dbType.equals(master_db_type)) {
				throw new Exception("read data is not from master");
			}
		}
		conn.commit();
		conn.setAutoCommit(true);
		System.out.println(getMethodName() + " success");
	}
	
	public void test_select_master_slave_master(Connection conn) throws Exception {
		String sql = "select dbtype from dbo.bigtable where id = 10";
		Statement stmt = conn.createStatement();
		conn.setAutoCommit(false);
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()) {
			String dbType = rs.getString(1).trim();
			if (!dbType.equals(master_db_type)) {
				throw new Exception("read data is not from master");
			}
		}
		rs.close();
		conn.rollback();
		
		conn.setAutoCommit(true);
		rs = stmt.executeQuery(sql);
		while(rs.next()) {
			String dbType = rs.getString(1).trim();
			if (!dbType.equals(slave_db_type)) {
				throw new Exception("read data is not from slave");
			}
		}
		rs.close();

		conn.setAutoCommit(false);
		Statement stmt1 = conn.createStatement();
		rs = stmt1.executeQuery(sql);
		while(rs.next()) {
			String dbType = rs.getString(1).trim();
			if (!dbType.equals(master_db_type)) {
				throw new Exception("read data is not from master");
			}
		}
		conn.commit();
		conn.setAutoCommit(true);
		System.out.println(getMethodName() + " success");
	}
}
