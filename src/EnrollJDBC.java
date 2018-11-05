
/*
EnrollJDBC.java - JDBC program for accessing and updating an enrollment database on MySQL.

Lab Assignment #6

Student name: 	<your student name>
University id:	<your university id>
*/

import java.io.File;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Scanner;

import com.mysql.jdbc.DatabaseMetaData;

public class EnrollJDBC {
	/**
	 * Connection to database
	 */
	private static Connection con;

	/**
	 * Main method is only used for convenience. Use JUnit test file to verify your
	 * answer.
	 * 
	 * @param args
	 *            none expected
	 * @throws SQLException
	 *             if a database error occurs
	 * @throws ParseException
	 *             during date conversions
	 */
	public static void main(String[] args) throws SQLException, ParseException {
		EnrollJDBC app = new EnrollJDBC();

		app.connect();
		app.init();

		/*
		 * // List all students System.out.println("Executing list all students.");
		 * System.out.println(app.listAllStudents());
		 * 
		 * // List all professors in a department System.out.
		 * println("\nExecuting list professors in a department: Computer Science");
		 * System.out.println(app.listDeptProfessors("Computer Science"));
		 * System.out.println("\nExecuting list professors in a department: none");
		 * System.out.println(app.listDeptProfessors("none"));
		 * 
		 * // List all students in a course
		 * System.out.println("\nExecuting list students in course: COSC 304");
		 * System.out.println(app.listCourseStudents("COSC 304"));
		 * System.out.println("\nExecuting list students in course: DATA 301");
		 * System.out.println(app.listCourseStudents("DATA 301"));
		 * 
		 * // Compute GPA
		 * System.out.println("\nExecuting compute GPA for student: 45671234");
		 * System.out.println(EnrollJDBC.resultSetToString(app.computeGPA("45671234"),10
		 * )); System.out.println("\nExecuting compute GPA for student: 00000000");
		 * System.out.println(EnrollJDBC.resultSetToString(app.computeGPA("00000000"),10
		 * ));
		 * 
		 * // Add student SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		 * System.out.println("\nAdding student 55555555:"); app.addStudent("55555555",
		 * "Stacy Smith", "F", sdf.parse("1998-01-01"));
		 * System.out.println("\nAdding student 11223344:"); app.addStudent("11223344",
		 * "Jim Jones", "M", sdf.parse("1997-12-31"));
		 * System.out.println(app.listAllStudents());
		 * 
		 * // Delete student System.out.println("\nTest delete student:\n"); // Existing
		 * student (with courses) System.out.println("\nDeleting student 99999999:");
		 * app.deleteStudent("99999999"); // Non-existing student
		 * System.out.println("\nDeleting student 00000000:");
		 * app.deleteStudent("00000000"); System.out.println(app.listAllStudents());
		 * 
		 * // Update student System.out.println("\nUpdating student 99999999:");
		 * app.updateStudent("99999999", "Wang Wong", "F", sdf.parse("1995-11-08"),
		 * 3.23); System.out.println("\nUpdating student 00567454:");
		 * app.updateStudent("00567454", "Scott Brown", "M", null, 4.00);
		 * System.out.println(app.listAllStudents());
		 * 
		 * // New enrollment
		 * System.out.println("\nTest new enrollment in COSC 304 for 98123434:\n");
		 * app.newEnroll("98123434", "COSC 304", "001", 2.51);
		 * 
		 * // Update student mark System.out.
		 * println("\nTest update student mark for student 98123434 to 3.55:\n");
		 * app.updateStudentMark("98123434", "COSC 304", "001", 3.55);
		 * 
		 * // Update student GPA app.init();
		 * System.out.println("\nTest update student GPA for student:\n");
		 * app.newEnroll("98123434", "COSC 304", "001", 3.97);
		 * app.updateStudentGPA("98123434");
		 * 
		 * // Remove student from section app.removeStudentFromSection("98123434",
		 * "COSC 304", "001");
		 * 
		 * // Queries // Re-initialize all data app.init();
		 * System.out.println(EnrollJDBC.resultSetToString(app.query1(), 100));
		 * System.out.println(EnrollJDBC.resultSetToString(app.query2(), 100));
		 * System.out.println(EnrollJDBC.resultSetToString(app.query3(), 100));
		 * System.out.println(EnrollJDBC.resultSetToString(app.query4(), 100));
		 */

		app.close();
	}

	/**
	 * Makes a connection to the database and returns connection to caller.
	 * 
	 * @return connection
	 * @throws SQLException
	 *             if an error occurs
	 */
	public Connection connect() throws SQLException {
		// TODO: Fill in your connection information
		String url = "jdbc:mysql://cosc304.ok.ubc.ca/db_crichard";
		String uid = "crichard";
		String pw = "46873155";

		System.out.println("Connecting to database.");
		// Note: Must assign connection to instance variable as well as returning it
		// back to the caller
		con = DriverManager.getConnection(url, uid, pw);
		return con;
	}

	/**
	 * Closes connection to database.
	 */
	public void close() {
		System.out.println("Closing database connection.");
		try {
			if (con != null)
				con.close();
		} catch (SQLException e) {
			System.out.println(e);
		}
	}

	/**
	 * Creates the database and initializes the data.
	 */
	public void init() {
		String fileName = "data/university.ddl";
		Scanner scanner = null;

		try {
			// Create statement
			Statement stmt = con.createStatement();

			scanner = new Scanner(new File(fileName));
			// Read commands separated by ;
			scanner.useDelimiter(";");
			while (scanner.hasNext()) {
				String command = scanner.next();
				if (command.trim().equals(""))
					continue;
				// System.out.println(command); // Uncomment if want to see commands executed
				stmt.execute(command);
			}
		} catch (Exception e) {
			System.out.println(e);
		}
		if (scanner != null)
			scanner.close();

		System.out.println("Data successfully loaded.");
	}

	/**
	 * Returns a String with all the students in the database. Format: sid, sname,
	 * sex, birthdate, gpa 00005465, Joe Smith, M, 1997-05-01, 3.20
	 * 
	 * @return String containing all student information
	 */
	public String listAllStudents() throws SQLException {
		StringBuilder output = new StringBuilder();

		// Use a PreparedStatement for this query.
		// TODO: Traverse ResultSet and use StringBuilder.append() to add columns/rows
		// to output string

		output.append("sid, sname, sex, birthdate, gpa");

		String SQL = "SELECT * FROM student";

		PreparedStatement stmt = con.prepareStatement(SQL);

		ResultSet rst = stmt.executeQuery(SQL);

		while (rst.next()) {
			output.append("\n" + rst.getString("sid") + ", " + rst.getString("sname") + ", " + rst.getString("sex")
					+ ", " + rst.getString("birthdate") + ", " + rst.getString("gpa"));
		}

		System.out.println(output.toString());
		return output.toString();
	}

	/**
	 * Returns a String with all the professors in a given department name.
	 * 
	 * Format: Professor Name, Department Name Art Funk, Computer Science
	 * 
	 * @return String containing professor information
	 */
	public String listDeptProfessors(String deptName) throws SQLException {
		// Use a PreparedStatement for this query.
		// TODO: Traverse ResultSet and use StringBuilder.append() to add columns/rows
		// to output string

		StringBuilder output = new StringBuilder();

		output.append("Professor Name, Department Name");

		String SQL = "SELECT pname, dname FROM prof WHERE dname = ?";

		PreparedStatement pstmt = con.prepareStatement(SQL);
		pstmt.setString(1, deptName);

		ResultSet rst = pstmt.executeQuery();

		while (rst.next()) {
			output.append("\n" + rst.getString("pname") + ", " + rst.getString("dname"));
		}

		return output.toString();

	}

	/**
	 * Returns a String with all students in a given course number (all sections).
	 * 
	 * Format: Student Id, Student Name, Course Number, Section Number 00005465, Joe
	 * Smith, COSC 304, 001
	 * 
	 * @return String containing students
	 */
	public String listCourseStudents(String courseNum) throws SQLException {
		// Use a PreparedStatement for this query.
		// TODO: Traverse ResultSet and use StringBuilder.append() to add columns/rows
		// to output string
		StringBuilder output = new StringBuilder();

		output.append("Student Id, Student Name, Course Number, Section Number");

		String SQL = "SELECT S.sid, S.sname, C.cnum, E.secnum  FROM student S, enroll E, course C WHERE S.sid = E.sid AND E.cnum = C.cnum AND E.cnum = ?";

		PreparedStatement pstmt = con.prepareStatement(SQL);

		pstmt.setString(1, courseNum);

		ResultSet rst = pstmt.executeQuery();

		while (rst.next()) {
			output.append("\n" + rst.getString("sid") + ", " + rst.getString("sname") + ", " + rst.getString("cnum")
					+ ", " + rst.getString("secnum"));
		}

		return output.toString();
	}

	/**
	 * Returns a ResultSet with a row containing the computed GPA (named as gpa) for
	 * a given student id. You must use a PreparedStatement.
	 * 
	 * @return ResultSet containing computed GPA
	 */
	public ResultSet computeGPA(String studentId) throws SQLException {
		// TODO: Use a PreparedStatement
		String Query1 = "SELECT avg(grade) AS gpa FROM enroll WHERE sid = ?";

		PreparedStatement pstmt = con.prepareStatement(Query1);
		pstmt.setString(1, studentId);

		ResultSet rst = pstmt.executeQuery();

		return rst;
	}

	/**
	 * Inserts a student into the databases. You must use a PreparedStatement.
	 * Return the PreparedStatement.
	 * 
	 * @return PreparedStatement used for command
	 * @throws SQLException
	 * @throws ParseException
	 */
	public PreparedStatement addStudent(String studentId, String studentName, String sex, java.util.Date birthDate)
			throws SQLException {
		// TODO: Use a PreparedStatement and return it at the end of the method

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		String Query1 = "INSERT student (sid, sname, sex, birthdate) VALUES (?,?, ?, ?)";

		PreparedStatement pstmt = con.prepareStatement(Query1);
		pstmt.setString(1, studentId);
		pstmt.setString(2, studentName);
		pstmt.setString(3, sex);
		pstmt.setString(4, sdf.format(birthDate));

		pstmt.executeUpdate();

		return pstmt;
	}

	/**
	 * Deletes a student from the databases. You must use a PreparedStatement.
	 * Return the PreparedStatement.
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 * @return PreparedStatement used for command
	 */
	public PreparedStatement deleteStudent(String studentId) throws SQLException {
		// TODO: Use a PreparedStatement and return it at the end of the method
		String Query1 = "DELETE FROM student WHERE sid = ?";

		PreparedStatement pstmt = con.prepareStatement(Query1);

		pstmt.setString(1, studentId);

		pstmt.executeUpdate();

		return pstmt;
	}

	/**
	 * Updates a student in the databases. You must use a PreparedStatement. Return
	 * the PreparedStatement.
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 * @return PreparedStatement used for command
	 */
	public PreparedStatement updateStudent(String studentId, String studentName, String sex, java.util.Date birthDate,
			double gpa) throws SQLException {
		// TODO: Use a PreparedStatement and return it at the end of the method

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		String Query1 = "UPDATE student SET sname = ?,sex = ?, birthdate = ?, gpa = ? WHERE sid = ?";

		PreparedStatement pstmt = con.prepareStatement(Query1);

		pstmt.setString(1, studentName);
		pstmt.setString(2, sex);
		if (birthDate == null) {
			pstmt.setString(3, null);
		} else {
			pstmt.setString(3, sdf.format(birthDate)); // use if not null set to date if null set to current date
		}
		pstmt.setString(4, Double.toString(gpa));
		pstmt.setString(5, studentId);

		pstmt.executeUpdate();

		return pstmt;
	}

	/**
	 * Creates a new enrollment in a course section. You must use a
	 * PreparedStatement. Return the PreparedStatement.
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 * @return PreparedStatement used for command
	 */
	public PreparedStatement newEnroll(String studentId, String courseNum, String sectionNum, Double grade)
			throws SQLException {
		// TODO: Use a PreparedStatement and return it at the end of the method

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		String Query1 = "INSERT enroll (sid, cnum, secnum, grade) VALUES (?,?, ?, ?)";

		PreparedStatement pstmt = con.prepareStatement(Query1);
		pstmt.setString(1, studentId);
		pstmt.setString(2, courseNum);
		pstmt.setString(3, sectionNum);
		if (grade == null)
			pstmt.setString(4, null);
		else
			pstmt.setString(4, Double.toString(grade));

		pstmt.executeUpdate();

		return pstmt;
	}

	/**
	 * Updates a student's GPA based on courses taken. You must use a
	 * PreparedStatement. Return the PreparedStatement.
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 * @return PreparedStatement used for command
	 */
	public PreparedStatement updateStudentGPA(String studentId) throws SQLException {
		// TODO: Use a PreparedStatement and return it at the end of the method
		ResultSet rst = computeGPA(studentId);
		rst.toString();
		rst.next();
		double newGPA = rst.getDouble(1);
		String Query1 = "UPDATE student SET gpa = ? WHERE sid = ?";

		PreparedStatement pstmt = con.prepareStatement(Query1);

		pstmt.setString(1, Double.toString(newGPA));
		pstmt.setString(2, studentId);

		pstmt.executeUpdate();

		return pstmt;
	}

	/**
	 * Removes a student from a course and updates their GPA. You must use a
	 * PreparedStatement. Return the PreparedStatement.
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 * @return PreparedStatement used for command
	 */
	public PreparedStatement removeStudentFromSection(String studentId, String courseNum, String sectionNum)
			throws SQLException {
		// TODO: Use a PreparedStatement and return it at the end of the method

		String Query1 = "DELETE FROM enroll WHERE sid = ? AND cnum = ? AND secnum = ?";

		PreparedStatement pstmt = con.prepareStatement(Query1);
		pstmt.setString(1, studentId);
		pstmt.setString(2, courseNum);
		pstmt.setString(3, sectionNum);

		pstmt.executeUpdate();

		return pstmt;
	}

	/**
	 * Updates a student's mark in an enrolled course section and updates their
	 * grade. You must use a PreparedStatement.
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 * @return PreparedStatement used for command
	 */
	public PreparedStatement updateStudentMark(String studentId, String courseNum, String sectionNum, double grade)
			throws SQLException {
		// TODO: Use a PreparedStatement and return it at the end of the method
		String Query1 = "UPDATE enroll SET grade = ? WHERE sid = ? AND cnum = ? AND secnum = ? ";

		PreparedStatement pstmt = con.prepareStatement(Query1);
		pstmt.setString(1, Double.toString(grade));
		pstmt.setString(2, studentId);
		pstmt.setString(3, courseNum);
		pstmt.setString(4, sectionNum);

		pstmt.executeUpdate();

		return pstmt;
	}

	/**
	 * Return the list of students (id and name) that have not registered in any
	 * course section. Hint: Left join can be used instead of a subquery.
	 * 
	 * @return ResultSet
	 * @throws SQLException
	 *             if an error occurs
	 */
	public ResultSet query1() throws SQLException {
		System.out.println("\nExecuting query #1.");
		// TODO: Execute the SQL query and return a ResultSet.
		String Query1 = "SELECT DISTINCT S.sid, S.sname FROM student S LEFT JOIN enroll E ON  S.sid = E.sid WHERE gpa IS NULL";

		PreparedStatement pstmt = con.prepareStatement(Query1);

		ResultSet rst = pstmt.executeQuery();

		return rst;
	}

	/**
	 * For each student return their id and name, number of course sections
	 * registered in (called numcourses), and gpa (average of grades). Return only
	 * students born after March 15, 1992. A student is also only in the result if
	 * their gpa is above 3.1 or registered in 0 courses. Order by GPA descending
	 * then student name ascending and show only the top 5.
	 * 
	 * @return ResultSet
	 * @throws SQLException
	 *             if an error occurs
	 */
	public ResultSet query2() throws SQLException {
		System.out.println("\nExecuting query #2.");
		// TODO: Execute the SQL query and return a ResultSet.
		String Query1 = "SELECT DISTINCT S.sid, S.sname, COUNT(gpa) AS numcourses, AVG(E.grade) AS gpa FROM student S LEFT JOIN enroll E ON  S.sid = E.sid WHERE birthdate > '1992-03-15'  GROUP BY S.sid  HAVING AVG(gpa) > 3.1 OR AVG(gpa) IS NULL ORDER BY AVG(gpa) DESC LIMIT 5";

		PreparedStatement pstmt = con.prepareStatement(Query1);

		ResultSet rst = pstmt.executeQuery();

		return rst;
	}

	/**
	 * For each course, return the number of sections (numsections), total number of
	 * students enrolled (numstudents), average grade (avggrade), and number of
	 * distinct professors who taught the course (numprofs). Only show courses in
	 * Chemistry or Computer Science department. Make sure to show courses even if
	 * they have no students. Do not show a course if there are no professors
	 * teaching that course. Format: cnum, numsections, numstudents, avggrade,
	 * numprof
	 * 
	 * 
	 * @return ResultSet
	 * @throws SQLException
	 *             if an error occurs
	 */
	public ResultSet query3() throws SQLException {
		System.out.println("\nExecuting query #3.");
		// TODO: Execute the SQL query and return a ResultSet.
		String Query1 = "SELECT S.cnum, COUNT(DISTINCT S.secnum) AS numsections, COUNT(E.sid) as numstudents, AVG(E.grade) AS avggrade, COUNT(DISTINCT S.pname) as numprofs FROM enroll E RIGHT JOIN section S ON E.cnum = S.cnum AND E.secnum = S.secnum LEFT JOIN prof P ON P.pname = S.pname WHERE P.dname = 'Chemistry' OR P.dname = 'Computer Science' GROUP BY S.cnum";

		PreparedStatement pstmt = con.prepareStatement(Query1);

		ResultSet rst = pstmt.executeQuery();

		return rst;
	}

	/**
	 * Return the students who received a higher grade than their course section
	 * average in at least two courses. Order by number of courses higher than the
	 * average and only show top 5. Format: EmployeeId, EmployeeName, orderCount
	 * 
	 * @return ResultSet
	 * @throws SQLException
	 *             if an error occurs
	 */
	public ResultSet query4() throws SQLException {
		System.out.println("\nExecuting query #4.");
		// TODO: Execute the SQL query and return a ResultSet.

		String subQuery1 = "SELECT cnum, secnum, AVG(grade) as avg FROM enroll GROUP BY cnum"; // i took out secnum in
																								// the group by

		String subQuery2 = "SELECT E.sid, S.sname, E.grade, Sub.avg FROM enroll E JOIN (" + subQuery1
				+ ") AS Sub ON Sub.cnum = E.cnum JOIN student S ON S.sid = E.sid HAVING (E.grade > Sub.avg)";

		String Query1 = "SELECT sQ.sid, sQ.sname, COUNT(*) AS numhigher FROM (" + subQuery2
				+ ") as sQ GROUP BY sQ.sid ORDER BY numhigher DESC LIMIT 5";

		String newquery = "SELECT S.sid, S.sname, COUNT(*) AS numhigher "
				+ "FROM enroll E JOIN student S ON E.sid = S.sid "
				+ "WHERE E.grade > (SELECT AVG(grade) FROM enroll En WHERE E.cnum = En.cnum AND E.secnum = En.secnum) "
				+ "GROUP BY S.sid " + "HAVING COUNT(*) > 2 " + "ORDER BY numhigher DESC, S.sname ASC " + "LIMIT 5";

		PreparedStatement pstmts1 = con.prepareStatement(subQuery1);
		PreparedStatement pstmts2 = con.prepareStatement(subQuery2);
		PreparedStatement pstmt = con.prepareStatement(newquery);

		ResultSet rsts1 = pstmts1.executeQuery();
		ResultSet rsts2 = pstmts2.executeQuery();
		ResultSet rst = pstmt.executeQuery();

		System.out.println("Subquery 1");
		System.out.println(resultSetToString(rsts1, 100));
		System.out.println("\nSubquery 2");
		System.out.println(resultSetToString(rsts2, 100) + "\n");

		return rst;
	}

	/*
	 * Do not change anything below here.
	 */
	/**
	 * Converts a ResultSet to a string with a given number of rows displayed. Total
	 * rows are determined but only the first few are put into a string.
	 * 
	 * @param rst
	 *            ResultSet
	 * @param maxrows
	 *            maximum number of rows to display
	 * @return String form of results
	 * @throws SQLException
	 *             if a database error occurs
	 */
	public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException {
		if (rst == null)
			return "No Resultset.";

		StringBuffer buf = new StringBuffer(5000);
		int rowCount = 0;
		ResultSetMetaData meta = rst.getMetaData();
		buf.append("Total columns: " + meta.getColumnCount());
		buf.append('\n');
		if (meta.getColumnCount() > 0)
			buf.append(meta.getColumnName(1));
		for (int j = 2; j <= meta.getColumnCount(); j++)
			buf.append(", " + meta.getColumnName(j));
		buf.append('\n');

		while (rst.next()) {
			if (rowCount < maxrows) {
				for (int j = 0; j < meta.getColumnCount(); j++) {
					Object obj = rst.getObject(j + 1);
					buf.append(obj);
					if (j != meta.getColumnCount() - 1)
						buf.append(", ");
				}
				buf.append('\n');
			}
			rowCount++;
		}
		buf.append("Total results: " + rowCount);
		return buf.toString();
	}
}
