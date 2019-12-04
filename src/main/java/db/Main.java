package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.net.ssl.SSLException;

public class Main {
	static private final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

	// update USER, PASS and DB URL according to credentials provided by the website:
	// https://remotemysql.com/
	// in future move these hard coded strings into separated config file or even better env variables
	static private final String DB = "uVGwDMAL93";
	static private final String DB_URL = "jdbc:mysql://remotemysql.com/"+ DB + "?useSSL=false";
	static private final String USER = "uVGwDMAL93";
	static private final String PASS = "rFLY1aRee6";

	public static void main(String[] args) throws SSLException {
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

			System.out.println("\t============");

			String sql = "SELECT * FROM flights";
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				int num = rs.getInt("num");
				String origin = rs.getString("origin");
				String destination = rs.getString("destination");
				int distance = rs.getInt("distance");
				int price = rs.getInt("price");

				System.out.format("Number %5s Origin %15s destinations %18s Distance %5d Price %5d\n", num, origin, destination, distance, price);
			}

			System.out.println("\t============");

			sql = "SELECT origin, destination, distance, num FROM flights";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String origin = rs.getString("origin");
				String destination = rs.getString("destination");
				int distance = rs.getInt("distance");

				System.out.print("From: " + origin);
				System.out.print(",\tTo: " + destination);
				System.out.println(",\t\tDistance: " + distance);
			}

			System.out.println("\t============");
			
			sql = "SELECT origin, destination FROM flights WHERE distance > ?";
			PreparedStatement prep_stmt = conn.prepareStatement(sql);
			prep_stmt.setInt(1, 200);
			rs = prep_stmt.executeQuery();
			while (rs.next()) {
				String origin = rs.getString("origin");
				System.out.println("From: " + origin);
			}
			
			
			// update flight 387 price to 2019
			sql = "SELECT num, price FROM flights WHERE num = 387 ";
			rs = stmt.executeQuery(sql);
			rs.last();
			rs.updateInt("PRICE", 2019);
			rs.updateRow();
			
			// print flight 387 price 
			sql = "SELECT num, origin, destination, distance, price FROM flights WHERE num = 387 ";
			rs = stmt.executeQuery(sql);
			{
				rs.last();
				int num = rs.getInt("num");
				String origin = rs.getString("origin");
				String destination = rs.getString("destination");
				int distance = rs.getInt("distance");
				int price = rs.getInt("price");
				System.out.format("\n\nNumber %5s Origin %15s destinations %18s Distance %5d Price %5d\n", num, origin, destination, distance, price);
			}
			
			// update prices of distance > 1000 by 100
			sql = "SELECT price, distance, num FROM flights WHERE distance > 1000 ";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				int price = rs.getInt("price");
				rs.updateInt("price", (price+100));
				rs.updateRow();
			}
			
			// update prices of price < 300 by 25
			sql = "SELECT price, num FROM flights WHERE price < 300 ";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				int price = rs.getInt("price");
				rs.updateInt("price", (price-25));
				rs.updateRow();
			}
			
			System.out.println("\t******** after first update **********");

			sql = "SELECT * FROM flights";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				int num = rs.getInt("num");
				String origin = rs.getString("origin");
				String destination = rs.getString("destination");
				int distance = rs.getInt("distance");
				int price = rs.getInt("price");

				System.out.format("Number %5s Origin %15s destinations %18s Distance %5d Price %5d\n", num, origin, destination, distance, price);
			}
			
			// update prices of distance > 1000 by 100 (PreparedStatement)
			PreparedStatement updatePricesplus = conn.prepareStatement("UPDATE flights " + "SET price = price + ? WHERE distance > ?");
			updatePricesplus.setInt(1, 100);
			updatePricesplus.setInt(2, 1000);
			updatePricesplus.execute();	
			
			// update prices of price < 300 by 25 (PreparedStatement)
			PreparedStatement updatePricesminus = conn.prepareStatement("UPDATE flights " + "SET price = price - ? WHERE price < ?");
			updatePricesminus.setInt(1, 25);
			updatePricesminus.setInt(2, 300);
			updatePricesminus.execute();			
			
			System.out.println("\t******** after PreparedStatement update **********");

			sql = "SELECT * FROM flights";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				int num = rs.getInt("num");
				String origin = rs.getString("origin");
				String destination = rs.getString("destination");
				int distance = rs.getInt("distance");
				int price = rs.getInt("price");

				System.out.format("Number %5s Origin %15s destinations %18s Distance %5d Price %5d\n", num, origin, destination, distance, price);
			}
			
			rs.close();
			stmt.close();
			conn.close();

		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println("SQLException: " + se.getMessage());
            System.out.println("SQLState: " + se.getSQLState());
            System.out.println("VendorError: " + se.getErrorCode());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}
}
