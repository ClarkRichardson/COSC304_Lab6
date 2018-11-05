package JDBC_MitermPractice;

import java.sql.*;

public class first {
	public static void main(String[] args)
	{	String url = "jdbc:mysql://cosc304.ok.ubc.ca/WorksOn";
		String uid = "rlawrenc";
		String pw = "test";


		// Note: Loading a driver class is not required.
		try {	// Load driver class
			Class.forName("com.mysql.jdbc.Driver");
		}
		catch (java.lang.ClassNotFoundException e) {
			System.err.println("ClassNotFoundException: " +e);
			System.exit(1);
		}

		Connection con = null;
		try {
			con = DriverManager.getConnection(url, uid, pw);
			Statement stmt = con.createStatement();
			ResultSet rst = stmt.executeQuery(
				"SELECT eno, ename FROM Emp "+
				" WHERE eno IN (SELECT supereno FROM Emp) ORDER BY ename");

			while (rst.next())
			{	System.out.println("Supervisor: "+ rst.getString("ename"));

				// Now look up employees directly supervised by this supervisor
				// Note: Using a PreparedStatement here would be even better!
				Statement stmt2 = con.createStatement();

				String sql = "SELECT ename, salary FROM Emp WHERE supereno = '";
				sql += rst.getString("eno")+"' ORDER BY salary DESC";
				ResultSet rst2 = stmt2.executeQuery(sql);

				while (rst2.next())
				{
					System.out.println("   "+rst2.getString(1)+", "+rst2.getString(2));
				}
				System.out.println();
			}
		}
		catch (SQLException ex)
		{
			System.err.println("SQLException: " + ex);
		}
		finally
		{
			if (con != null)
			{
				try
				{
					con.close();
				}
				catch (SQLException ex)
				{
					System.err.println("SQLException: " + ex);
				}
			}
		}
	}
}
