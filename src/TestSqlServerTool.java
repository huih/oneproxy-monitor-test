import java.sql.Connection;
import java.sql.Statement;

public class TestSqlServerTool {
	public String getMethodName() {  
        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();  
        StackTraceElement e = stacktrace[2];  
        String methodName = e.getMethodName();  
        return methodName;  
    }
	
	public void start_trans_use_begin(Connection conn) throws Exception {
		String begin = "begin tran";
		Statement stmt = conn.createStatement();
		stmt.execute(begin);
	}
	
	public void end_trans_use_commit(Connection conn) throws Exception {
		String commit = "commit";
		Statement stmt = conn.createStatement();
		stmt.execute(commit);
	}
	
	public void end_trans_use_rollback(Connection conn) throws Exception {
		String rollback = "rollback";
		Statement stmt = conn.createStatement();
		stmt.execute(rollback);
	}
}

