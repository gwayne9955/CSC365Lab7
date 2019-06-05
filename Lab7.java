import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import java.util.Map;
import java.util.Scanner;
import java.util.LinkedHashMap;
import java.time.LocalDate;
import java.util.List;
import java.util.ArrayList;

/*
Introductory JDBC examples based loosely on the BAKERY dataset from CSC 365 labs.

-- MySQL setup:
drop table if exists hp_goods, hp_customers, hp_items, hp_receipts;
create table hp_goods as select * from BAKERY.goods;
create table hp_customers as select * from BAKERY.customers;
create table hp_items as select * from BAKERY.items;
create table hp_receipts as select * from BAKERY.receipts;

grant all on spring2019.hp_goods to hasty@'%';
grant all on spring2019.hp_customers to hasty@'%';
grant all on spring2019.hp_items to hasty@'%';
grant all on spring2019.hp_receipts to hasty@'%';

-- Shell init:
export CLASSPATH=$CLASSPATH:mysql-connector-java-5.1.44-bin.jar:.
export L7_JDBC_URL=jdbc:mysql://db.labthreesixfive.com/gwayne?autoReconnect=true\&useSSL=false
export L7_JDBC_USER=
export L7_JDBC_PW=
 */
public class Lab7 {
	public static void main(String[] args) {
		try {
			setupDB();
			welcomeUser();
			getInputs();
		} catch (SQLException e) {
			System.err.println("SQLException: " + e.getMessage());
		}
	}

	private static void welcomeUser() {
		System.out.println("\nWelcome to the Inn Database!\nType in 'Help' for more information on how to use the program\n......................................\n");
	}

	private static void getInputs() {
		System.out.print("Please enter a command (or type 'Help' or 'Quit'): ");
		Scanner scanner = new Scanner(System.in);
		String input = scanner.nextLine().trim();

		while (!(input.equals("Q") || input.equals("Quit"))) {
		   produceAnswer(input);
		   System.out.println();
		   System.out.print("Please enter a command (or type 'Help' or 'Quit'): ");
		   input = scanner.nextLine().trim();
		}

		// if user enters Q or Quit, the while loop closes
		System.out.println("Goodbye! Thank you for using our Inn Database");
		scanner.close();
	}

	private static void produceAnswer(String input) {
		switch (input.toLowerCase()) {
			case "help":
			case "h":
				printHelp();
				break;
			case "R1":
				System.out.println("Performing R1");
				break;
			case "R2":
				System.out.println("Performing R2");
				break;
			case "R3":
				System.out.println("Performing R3");
				break;
			case "R4":
				System.out.println("Performing R4");
				break;
			case "R5":
				System.out.println("Performing R5");
				break;
			case "R6":
				System.out.println("Performing R6");
				break;
			default:
				System.out.println("Wrongly formatted expression");
				break;
		}
	}

	private static void printHelp() {
		String helpStr = "USAGE OF THE INN DATABASE\n......................................\n" +
			"• 'R1': Rooms and Rates.\n\tWhen this option is selected, the system will output a list of rooms to the user sorted by popularity (highest to lowest)\n" +
			"• 'R2': Reservations.\n\tWhen this option is selected, the system will produce a numbered list of matching rooms available for booking, along with\n\t" +
				"a prompt to allow booking by option number. If no exact matches are found, the system will suggest 5 possibilities for different rooms or dates.\n" +
			"• 'R3': Reservation Change.\n\tWhen this option is selected, the system will allow the user to make changes to an existing reservation, accepting from\n\t" +
				"the user a reservation code and new values for the reservation.\n" +
			"• 'R4': Reservation Cancellation.\n\tWhen this option is selected, the system will allow the user to cancel an existing reservation, accepting from the user a\n\t" +
				"reservation code, confirming the cancellation, then removing the reservation record from the database.\n" +
			"• 'R5': Detailed Reservation Information.\n\tWhen this option is selected, the system will present the user with a search prompt or form that allows them to enter any combination of the fields.\n" +
			"• 'R6': Revenue.\n\tWhen this option is selected, the system shall provide a month-by-month overview of revenue for an entire year.\n" +
			"......................................\n";

		System.out.println(helpStr);
	}

	// Demo1 - Establish JDBC connection, execute DDL statement
	private void demo1() throws SQLException {

		// Step 0: Load MySQL JDBC Driver
		// No longer required as of JDBC 2.0 / Java 6
		try {
			Class.forName("com.mysql.jdbc.Driver");
			System.out.println("MySQL JDBC Driver loaded");
		} catch (ClassNotFoundException ex) {
			System.err.println("Unable to load JDBC Driver");
			System.exit(-1);
		}

		// Step 1: Establish connection to RDBMS
		try (Connection conn = DriverManager.getConnection(System.getenv("L7_JDBC_URL"), System.getenv("L7_JDBC_USER"),
				System.getenv("L7_JDBC_PW"))) {
			// Step 2: Construct SQL statement
			String sql = "ALTER TABLE hp_goods ADD COLUMN AvailUntil DATE";

			// Step 3: (omitted in this example) Start transaction

			try (Statement stmt = conn.createStatement()) {

				// Step 4: Send SQL statement to DBMS
				boolean exRes = stmt.execute(sql);

				// Step 5: Handle results
				System.out.format("Result from ALTER: %b %n", exRes);
			}

			// Step 6: (omitted in this example) Commit or rollback transaction
		}
		// Step 7: Close connection (handled by try-with-resources syntax)
	}

	private static void setupDB() throws SQLException {
		System.out.println("........ Setting up Database .........");
		try (Connection conn = DriverManager.getConnection(System.getenv("L7_JDBC_URL"), System.getenv("L7_JDBC_USER"),
				System.getenv("L7_JDBC_PW"))) {

					String dropRoomsSql = "DROP TABLE IF EXISTS lab7_rooms;";

					String dropReservationsSql = "DROP TABLE IF EXISTS lab7_reservations;";

					String roomsSql = "CREATE TABLE IF NOT EXISTS lab7_rooms (" +
			  			"RoomCode char(5) PRIMARY KEY, " +
			  			"RoomName varchar(30) NOT NULL, " +
			  			"Beds int(11) NOT NULL, " +
			  			"bedType varchar(8) NOT NULL, " +
			  			"maxOcc int(11) NOT NULL, " +
			  			"basePrice DECIMAL(6,2) NOT NULL, " +
			  			"decor varchar(20) NOT NULL, " +
			  			"UNIQUE (RoomName)" +
						");";

					String reservationsSql = "CREATE TABLE IF NOT EXISTS lab7_reservations (" +
  						"CODE int(11) PRIMARY KEY, " +
  						"Room char(5) NOT NULL, " +
  						"CheckIn date NOT NULL, " +
  						"Checkout date NOT NULL, " +
  						"Rate DECIMAL(6,2) NOT NULL, " +
  						"LastName varchar(15) NOT NULL, " +
  						"FirstName varchar(15) NOT NULL, " +
  						"Adults int(11) NOT NULL, " +
  						"Kids int(11) NOT NULL, " +
  						"UNIQUE (Room, CheckIn), " +
  						"UNIQUE (Room, Checkout), " +
  						"FOREIGN KEY (Room) REFERENCES lab7_rooms (RoomCode)" +
						");";

					String roomsInsertSql = "INSERT INTO lab7_rooms SELECT * FROM INN.rooms;";

					String reservationsInsertSql = "INSERT INTO lab7_reservations SELECT CODE, Room, " +
   					"DATE_ADD(CheckIn, INTERVAL 9 YEAR), " +
   					"DATE_ADD(Checkout, INTERVAL 9 YEAR), " +
   					"Rate, LastName, FirstName, Adults, Kids FROM INN.reservations;";

					Statement stmt = conn.createStatement();
					stmt.addBatch(dropReservationsSql);
					stmt.addBatch(dropRoomsSql);
					stmt.addBatch(roomsSql);
					stmt.addBatch(reservationsSql);
					stmt.addBatch(roomsInsertSql);
					stmt.addBatch(reservationsInsertSql);
					stmt.executeBatch();

					System.out.println("Successfully setup Room and Reservations tables");
				}
	}

	// Demo2 - Establish JDBC connection, execute SELECT query, read & print result
	private void demo2() throws SQLException {

		// Step 1: Establish connection to RDBMS
		try (Connection conn = DriverManager.getConnection(System.getenv("L7_JDBC_URL"), System.getenv("L7_JDBC_USER"),
				System.getenv("L7_JDBC_PW"))) {
			// Step 2: Construct SQL statement
			String sql = "SELECT * FROM hp_goods";

			// Step 3: (omitted in this example) Start transaction

			// Step 4: Send SQL statement to DBMS
			try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {

				// Step 5: Receive results
				while (rs.next()) {
					String flavor = rs.getString("Flavor");
					String food = rs.getString("Food");
					float price = rs.getFloat("price");
					System.out.format("%s %s ($%.2f) %n", flavor, food, price);
				}
			}

			// Step 6: (omitted in this example) Commit or rollback transaction
		}
		// Step 7: Close connection (handled by try-with-resources syntax)
	}

	// Demo3 - Establish JDBC connection, execute DML query (UPDATE)
	// -------------------------------------------
	// Never (ever) write database code like this!
	// -------------------------------------------
	private void demo3() throws SQLException {

		// Step 1: Establish connection to RDBMS
		try (Connection conn = DriverManager.getConnection(System.getenv("L7_JDBC_URL"), System.getenv("L7_JDBC_USER"),
				System.getenv("L7_JDBC_PW"))) {
			// Step 2: Construct SQL statement
			Scanner scanner = new Scanner(System.in);
			System.out.print("Enter a flavor: ");
			String flavor = scanner.nextLine();
			System.out.format("Until what date will %s be available (YYYY-MM-DD)? ", flavor);
			String availUntilDate = scanner.nextLine();

			// -------------------------------------------
			// Never (ever) write database code like this!
			// -------------------------------------------
			String updateSql = "UPDATE hp_goods SET AvailUntil = '" + availUntilDate + "' " + "WHERE Flavor = '"
					+ flavor + "'";

			// Step 3: (omitted in this example) Start transaction

			try (Statement stmt = conn.createStatement()) {

				// Step 4: Send SQL statement to DBMS
				int rowCount = stmt.executeUpdate(updateSql);

				// Step 5: Handle results
				System.out.format("Updated %d records for %s pastries%n", rowCount, flavor);
			}

			// Step 6: (omitted in this example) Commit or rollback transaction

		}
		// Step 7: Close connection (handled implcitly by try-with-resources syntax)
	}

	// Demo4 - Establish JDBC connection, execute DML query (UPDATE) using
	// PreparedStatement / transaction
	private void demo4() throws SQLException {

		// Step 1: Establish connection to RDBMS
		try (Connection conn = DriverManager.getConnection(System.getenv("L7_JDBC_URL"), System.getenv("L7_JDBC_USER"),
				System.getenv("L7_JDBC_PW"))) {
			// Step 2: Construct SQL statement
			Scanner scanner = new Scanner(System.in);
			System.out.print("Enter a flavor: ");
			String flavor = scanner.nextLine();
			System.out.format("Until what date will %s be available (YYYY-MM-DD)? ", flavor);
			LocalDate availDt = LocalDate.parse(scanner.nextLine());

			String updateSql = "UPDATE hp_goods SET AvailUntil = ? WHERE Flavor = ?";

			// Step 3: Start transaction
			conn.setAutoCommit(false);

			try (PreparedStatement pstmt = conn.prepareStatement(updateSql)) {

				// Step 4: Send SQL statement to DBMS
				pstmt.setDate(1, java.sql.Date.valueOf(availDt));
				pstmt.setString(2, flavor);
				int rowCount = pstmt.executeUpdate();

				// Step 5: Handle results
				System.out.format("Updated %d records for %s pastries%n", rowCount, flavor);

				// Step 6: Commit or rollback transaction
				conn.commit();
			} catch (SQLException e) {
				conn.rollback();
			}

		}
		// Step 7: Close connection (handled implcitly by try-with-resources syntax)
	}

	// Demo5 - Construct a query using PreparedStatement
	private void demo5() throws SQLException {

		// Step 1: Establish connection to RDBMS
		try (Connection conn = DriverManager.getConnection(System.getenv("L7_JDBC_URL"), System.getenv("L7_JDBC_USER"),
				System.getenv("L7_JDBC_PW"))) {
			Scanner scanner = new Scanner(System.in);
			System.out.print("Find pastries with price <=: ");
			Double price = Double.valueOf(scanner.nextLine());
			System.out.print("Filter by flavor (or 'Any'): ");
			String flavor = scanner.nextLine();

			List<Object> params = new ArrayList<Object>();
			params.add(price);
			StringBuilder sb = new StringBuilder("SELECT * FROM hp_goods WHERE price <= ?");
			if (!"any".equalsIgnoreCase(flavor)) {
				sb.append(" AND Flavor = ?");
				params.add(flavor);
			}

			try (PreparedStatement pstmt = conn.prepareStatement(sb.toString())) {
				int i = 1;
				for (Object p : params) {
					pstmt.setObject(i++, p);
				}

				try (ResultSet rs = pstmt.executeQuery()) {
					System.out.println("Matching Pastries:");
					int matchCount = 0;
					while (rs.next()) {
						System.out.format("%s %s ($%.2f) %n", rs.getString("Flavor"), rs.getString("Food"),
								rs.getDouble("price"));
						matchCount++;
					}
					System.out.format("----------------------%nFound %d match%s %n", matchCount,
							matchCount == 1 ? "" : "es");
				}
			}
		}
	}
}