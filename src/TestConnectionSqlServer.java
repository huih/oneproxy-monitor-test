  
  
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.log4j.PropertyConfigurator;

class SqlServerThreadClient extends Thread {
	
	/**
	 * 1. 中间件中必须维护游标的唯一性
	 * 2. 中间件中必须维护prepared的唯一性
	 * **/
	
	//只测试statement的单一查询情况,得到正常的流程为sp_cursoropen->sp_cursorfetch->sp_cursorclose
	//这个过程中只有游标句柄
	public void test_select_statement(Connection conn) throws Exception {
		System.out.println("xxxxxxxxxxxxxxxxxxxxxtest_select_statementxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		String sql = "select id, name, age from dbo.bigtable where id < 10";
		java.sql.Statement stmt = conn.createStatement();//没有数据包的产生
		ResultSet rs = stmt.executeQuery(sql);//sp_cursoropen
		while(rs.next()) {//sp_cursorfetch
			int id = rs.getInt("id");
			String name = rs.getString("name");
			int age = rs.getInt("age");
			System.out.println("id: " + id + " name: " + name + " age:" + age);
		}
		rs.close();//sp_cursorclose
		stmt.close();
	}
	
	/**
	 * 目标： 检测在两个statement之间是否进行了游标关闭，两次得到的游标句柄是否相同
	 * 结论：在进行第二次executeQuery之前关闭了第一次的cursor, 但是第二次得到的游标句柄与第一次相同
	 * 处理方式：不用记录两次的句柄。
	 * @throws SQLException 
	 * */
	public void test_select_statement2(Connection conn) throws SQLException {
		System.out.println("xxxxxxxxxxxxxxxxxxxxx test_select_statement2 xxxxxxxxxxxxxxxxxxxxxxxxxxx");
		String sql = "select id, name, age from dbo.bigtable where id < 10";
		java.sql.Statement stmt = conn.createStatement();//没有数据包的产生
		ResultSet rs = stmt.executeQuery(sql);//sp_cursoropen
		while(rs.next()) {//sp_cursorfetch
			int id = rs.getInt("id");
			String name = rs.getString("name");
			int age = rs.getInt("age");
			System.out.println("id: " + id + " name: " + name + " age:" + age);
		}
		rs = stmt.executeQuery(sql);
		while(rs.next()) {
			int id = rs.getInt("id");
			String name = rs.getString("name");
			int age = rs.getInt("age");
			System.out.println("id: " + id + " name: " + name + " age:" + age);
		}
		rs.close();
		stmt.close();
	}
	
	/**
	 * @目标： 测试两个statement交叉查询的情况
	 * **/
	public void test_select_statement3(Connection conn) throws Exception {
		System.out.println("xxxxxxxxxxxxxxxxxxxxx test_select_statement3 xxxxxxxxxxxxxxxxxxxxxxxxxxx");
		String sql = "select id, name, age from dbo.bigtable where id < 100 and id > 80";
		String sql2 = "select id, name, age from dbo.bigtable where id < 200 and id > 180";
		java.sql.Statement stmt = conn.createStatement();//没有数据包的产生
		ResultSet rs = stmt.executeQuery(sql);
		
		java.sql.Statement stmt2 = conn.createStatement();
		ResultSet rs2 = stmt2.executeQuery(sql2);
		System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxx id < 100 and id > 80 xxxxxxxxxxxxxxxxxx");
		while(rs.next()) {
			int id = rs.getInt("id");
			String name = rs.getString("name");
			int age = rs.getInt("age");
			System.out.println("id: " + id + " name: " + name + " age:" + age);
		}
		
		System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx id < 200 and id > 180 xxxxxxxxxxxx");
		while(rs2.next()) {
			int id = rs2.getInt("id");
			String name = rs2.getString("name");
			int age = rs2.getInt("age");
			System.out.println("id: " + id + " name: " + name + " age:" + age);
		}
		rs2.close();
		
		rs.close();//sp_cursorclose
		stmt2.close();
		stmt.close();
	}
	
	/**
	 * @目标： 测试select 与 update交叉的情况 使用statement
	 * **/
	public void test_select_statement4(Connection conn) throws Exception {
		System.out.println("xxxxxxxxxxxxxxxxxxxxx test_select_statement3 xxxxxxxxxxxxxxxxxxxxxxxxxxx");
		String sql = "select id, name, age from dbo.bigtable where id < 10";
		String sql2 = "update dbo.bigtable set age = 10 where id = 1;";
		java.sql.Statement stmt = conn.createStatement();//没有数据包的产生
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()) {
			int id = rs.getInt("id");
			String name = rs.getString("name");
			int age = rs.getInt("age");
			System.out.println("id: " + id + " name: " + name + " age:" + age);
		}
		
		java.sql.Statement stmt2 = conn.createStatement();
		stmt2.execute(sql2);
		
		rs = stmt.executeQuery(sql);
		while(rs.next()) {
			int id = rs.getInt("id");
			String name = rs.getString("name");
			int age = rs.getInt("age");
			System.out.println("id: " + id + " name: " + name + " age:" + age);
		}
		
		rs.close();//sp_cursorclose
		stmt2.close();
		stmt.close();
	}
	
	
	/*
	 * 目标：测试prepared的方式运行情况
	 * 包过程：SP_CURSORPREPEXEC->SP_CURSORFETCH->SP_CURSORCLOSE->CURSORUNPREPARE
	 * **/
	public void test_select_preparedstatement(Connection conn) throws Exception {
		System.out.println("xxxxxxxxxxxxxxxxxxxxx test_select_preparedstatement xxxxxxxxxxxxxxxxxxxxxxxxxxx");
		String sql = "select id, name, age from dbo.bigtable where id = ?";
		PreparedStatement psmt = conn.prepareStatement(sql);
		psmt.setInt(1, 10);
		ResultSet rs = psmt.executeQuery();
		while(rs.next()) {
			int id = rs.getInt("id");
			String name = rs.getString("name");
			int age = rs.getInt("age");
			System.out.println("id: " + id + " name: " + name + " age:" + age);
		}
		rs.close();
		psmt.close();
	}
	
	/*
	 * 目标：测试在同一个prepared中，游标的处理情况
	 * 流程：SP_CURSORPREPEXEC->SP_CURSORFETCH->SP_CURSORCLOSE->
	 * SP_CURSOREXECUTE->SP_CURSORFETCH->SP_CURSORCLOSE->SP_CURSORUNPREPARE
	 * 结论：同一个prepared，同一个游标句柄，在sp_cursorexecute中需要使用前面提供的游标
	 * **/
	public void test_select_preparedstatement2(Connection conn) throws Exception {
		System.out.println("xxxxxxxxxxxxxxxxxxxxx test_select_preparedstatement2 xxxxxxxxxxxxxxxxxxxxxxxxxxx");
		String sql = "select id, name, age from dbo.bigtable where id = ?";
		PreparedStatement psmt = conn.prepareStatement(sql);
		psmt.setInt(1, 10);
		ResultSet rs = psmt.executeQuery();
		while(rs.next()) {
			int id = rs.getInt("id");
			String name = rs.getString("name");
			int age = rs.getInt("age");
			System.out.println("id: " + id + " name: " + name + " age:" + age);
		}
		
		psmt.setInt(1,  20);
		rs = psmt.executeQuery();//SP_CURSOREXECUTE
		while(rs.next()) {
			int id = rs.getInt("id");
			String name = rs.getString("name");
			int age = rs.getInt("age");
			System.out.println("id: " + id + " name: " + name + " age:" + age);
		}
		
		rs.close();
		psmt.close();
	}
	
	/**
	 * @目标：测试写update，应该在主上面执行
	 * **/
	public void test_update_statement(Connection conn) throws Exception
	{
		String sql = "update dbo.bigtable set age = 10 where id = 1;";
		java.sql.Statement stmt = conn.createStatement();//没有数据包的产生
		stmt.execute(sql);
		stmt.close();
	}
	
	/**
	 * 目标：先在备数据库上面执行sql，得到handle，再使用这个handle在主上面执行
	 * @throws SQLException 
	 * */
	public void test_select_bak_master_preparedstatement(Connection conn) throws SQLException
	{
		String sql = "select id, name, age from dbo.bigtable where id = ?";
		
		//在备上面执行
		PreparedStatement psmt = conn.prepareStatement(sql);
		psmt.setInt(1, 10);
		ResultSet rs = psmt.executeQuery();
		
		//2. 在主上面执行
		conn.setAutoCommit(false);		
		psmt.setInt(1, 20);
		rs = psmt.executeQuery();
		while(rs.next()) {
			int id = rs.getInt("id");
			String name = rs.getString("name");
			int age = rs.getInt("age");
			System.out.println("id: " + id + " name: " + name + " age:" + age); 
		}
		conn.commit();
	}
	
	/**
	 * 目标：先进行update,在通过prepared进行select
	 * @throws Exception 
	 * **/
	public void test_update_preparedSelect(Connection conn) throws Exception
	{
		String sql = "update dbo.bigtable set age = 10 where id = 1;";
		java.sql.Statement stmt = conn.createStatement();//没有数据包的产生
		stmt.execute(sql);
		stmt.close();
		
		sql = "select id, name, age from dbo.bigtable where id = ?";
		
		//在备上面执行
		PreparedStatement psmt = conn.prepareStatement(sql);
		psmt.setInt(1, 10);
		ResultSet rs = psmt.executeQuery();
		
		//2. 在主上面执行
		conn.setAutoCommit(false);		
		psmt.setInt(1, 20);
		rs = psmt.executeQuery();
		while(rs.next()) {
			int id = rs.getInt("id");
			String name = rs.getString("name");
			int age = rs.getInt("age");
			System.out.println("id: " + id + " name: " + name + " age:" + age); 
		}
		conn.commit();
	}
	
	public void SelectUser() {
		
		Connection conn = null;
    	
//		String url = "jdbc:sqlserver://127.0.0.1:9999;databaseName=sqldb;SelectMethod=Cursor";
		String url = "jdbc:sqlserver://192.168.7.78:9999;databaseName=sqldb;SelectMethod=Cursor";
    	String username = "admin";
    	String password = "0000";
    	String sql = "select id, name, age from dbo.bigtable where id = ?";
    	String sql1 = "select id, name, age from dbo.bigtable where name= ?";
    	String sql2 = "select id, name, age from dbo.bigtable where id < 10";
    	String sql3 = "select id, name ,age from dbo.bigtable where id = ? and name = ?";

    	try {
    		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    		conn = DriverManager.getConnection(url, username, password);
    		
    		test_select_statement(conn);
    		test_select_statement2(conn);
    		test_select_statement3(conn);
    		test_select_statement4(conn);
    		test_select_preparedstatement(conn);
    		test_select_preparedstatement2(conn);
    		
    		test_update_statement(conn);
    		test_select_bak_master_preparedstatement(conn);
    		
    		test_update_preparedSelect(conn);
    		    		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			try {
				if (conn != null)
					conn.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    }
	
	public void run() {
		long bt = System.nanoTime();
		SelectUser();
		long at = System.nanoTime();
		
		System.out.println("time:" + (at - bt) + " ns, " + (at-bt)/1000000 + " ms, " + (at-bt)/1000000000 + " s");
	}
};

public class TestConnectionSqlServer {
  
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
		
		int jobNum = 1;
		int CliNum = 1;
		long bt = 0, at = 0;
		
		bt = System.nanoTime();
		int k = 0;
		for (k = 0; k < jobNum; k++) {
			ArrayList<Thread> ta = new ArrayList<Thread>();
			int i = 0;
			for (i = 0; i < CliNum; ++i) {
				SqlServerThreadClient tc = new SqlServerThreadClient();
				Thread t = new Thread(tc);
				t.start();
				ta.add(t);
			}
			for (i = 0; i < ta.size(); ++i) {
				try {
					ta.get(i).join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		at = System.nanoTime();
		long t = at - bt;
		System.out.println("sum Time:" + t + " ns, " + t/1000000 + " ms, " + t/1000000000 + " s");
    }
}