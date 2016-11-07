import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestSqlServerPreparedStatement {

	private String master_db_type = "master";
	private String slave_db_type = "slave";

	private String getMethodName() {  
        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();  
        StackTraceElement e = stacktrace[2];  
        String methodName = e.getMethodName();  
        return methodName;  
    }
	
	//read data from slave database.
	public void test_select_bak_db(Connection conn) throws Exception {
		String sql = "select dbtype from dbo.bigtable where id = ?";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.setInt(1, 10);
		ResultSet rs = pstmt.executeQuery();
		while(rs.next()) {
			String dbtype = rs.getString(1).trim();
			if (!dbtype.equals(slave_db_type)) {
				throw new Exception("the data is not from slave");
			}
		}
		rs.close();
		System.out.println(getMethodName() + " success");
	}
	
	//read data from master 
	public void test_select_master_db(Connection conn) throws Exception {
		String sql = "select dbtype from dbo.bigtable where id = ?";
		conn.setAutoCommit(false);
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.setInt(1, 10);
		ResultSet rs = pstmt.executeQuery();
		while(rs.next()) {
			String dbtype = rs.getString(1).trim();
			if (!dbtype.equals(master_db_type)) {
				throw new Exception("the data is not from master");
			}
		}
		rs.close();
		conn.commit();
		conn.setAutoCommit(true);
		
		System.out.println(getMethodName() + " success");
	}
	
	//first read from master
	public void test_select_master_db2(Connection conn) throws Exception {
		String sql = "select dbtype from dbo.bigtable where id = ?";
		conn.setAutoCommit(false);
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.setInt(1, 10);
		ResultSet rs = pstmt.executeQuery();
		while(rs.next()) {
			String dbtype = rs.getString(1).trim();
			if (0 != dbtype.compareTo(master_db_type)) {
				throw new Exception("the data is not from master");
			}
		}
		rs.close();
		conn.commit();
		conn.setAutoCommit(true);
		
		pstmt.setInt(1, 20);
		rs = pstmt.executeQuery();
		while(rs.next()) {
			String dbtype = rs.getString(1).trim();
			if (0 != dbtype.compareTo(master_db_type)) {
				throw new Exception("the data is not from master");
			}
		}
		System.out.println(getMethodName() + " success");
	}
	//frist update data, and then read data.
	public void test_update_read_slave_db(Connection conn) throws Exception {
		String sql = "update dbo.bigtable set age = 110 where id = 1;";
		String sql1 = "select age from dbo.bigtable where id = ?";
		String sql2 = "select dbtype from dbo.bigtable where id = ?";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.executeUpdate();
		pstmt.close();
		
		conn.setAutoCommit(false);
		pstmt = conn.prepareStatement(sql1);
		pstmt.setInt(1, 1);
		ResultSet rs = pstmt.executeQuery();
		while(rs.next()) {
			int age = rs.getInt(1);
			if (age != 110) {
				throw new Exception("update age error");
			}
		}
		conn.commit();
		conn.setAutoCommit(true);
		
		//read from slave
		pstmt = conn.prepareStatement(sql2);
		pstmt.setInt(1, 1);
		rs = pstmt.executeQuery();
		while(rs.next()) {
			String dbtype = rs.getString(1).trim();
			if (0 != dbtype.compareTo(slave_db_type)) {
				throw new Exception("the data is not from salve");
			}
		}
		System.out.println(getMethodName() + " success");
	}
	
	
	
}
