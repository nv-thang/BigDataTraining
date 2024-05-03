import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class Main {

	private static String driverClass = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {

		try {
			Class.forName(driverClass);
		} catch (ClassNotFoundException exception) {

			exception.printStackTrace();
			System.exit(1);
		}
		Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
		Statement stmt = con.createStatement();

		String tableName = "data";
		stmt.execute("drop table " + tableName);
		stmt.execute("create table " + tableName + " (city string, temperature int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE");

		// show tables
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		ResultSet res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}

		// describe table
		sql = "describe " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}

		// load data into table
		String filepath = "/home/cloudera/Desktop/data.csv";
		sql = "load data local inpath '" + filepath + "' into table "
				+ tableName;
		System.out.println("Running: " + sql);
		stmt.execute(sql);

		// select * query
		sql = "select * from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(String.valueOf(res.getString(1)) + "\t"
					+ res.getInt(2));
		}

		stmt.close();
		con.close();
	}
}
