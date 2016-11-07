import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestSqlServerStatement {
	
	private String master_db_type = "master";
	private String slave_db_type = "slave";
	private TestSqlServerTool tool = new TestSqlServerTool();
	
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
		System.out.println(tool.getMethodName() + " success");
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
		System.out.println(tool.getMethodName() + " success");
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
		System.out.println(tool.getMethodName() + " success");
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
		System.out.println(tool.getMethodName() + " success");
	}
	
	public void test_select_slave_master_begin(Connection conn) throws Exception {
		String sql = "select dbtype from dbo.bigtable where id = 10";
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()) {
			String dbType = rs.getString(1).trim();
			if (!dbType.equals(slave_db_type)) {
				throw new Exception("the data is not from salve");
			}
		}
		
		tool.start_trans_use_begin(conn);
		rs = stmt.executeQuery(sql);
		while(rs.next()) {
			String dbType = rs.getString(1).trim();
			if (!dbType.equals(master_db_type)) {
				throw new Exception("the data is not from master");
			}
		}
		tool.end_trans_use_commit(conn);
		
		rs = stmt.executeQuery(sql);
		while(rs.next()) {
			String dbType = rs.getString(1).trim();
			if (!dbType.equals(slave_db_type)) {
				throw new Exception("the data is not from salve");
			}
		}
		System.out.println(tool.getMethodName() + " success");
	}
}
