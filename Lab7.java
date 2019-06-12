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
        System.out.println(
                "Welcome to the Inn Database!\nType in 'Help' for more information on how to use the program\n......................................\n");
    }

    private static void getInputs() throws SQLException {
        System.out.print("Please enter a command (or type 'Help' or 'Quit'): ");
        Scanner scanner = new Scanner(System.in);
        String input = scanner.nextLine().trim();

        while (!(input.toLowerCase().equals("q") || input.toLowerCase().equals("quit"))) {
            produceAnswer(input);
            System.out.println();
            System.out.print("Please enter a command (or type 'Help' or 'Quit'): ");
            input = scanner.nextLine().trim();
        }

        // if user enters Q or Quit, the while loop closes
        System.out.println("Goodbye! Thank you for using our Inn Database");
        scanner.close();
    }

    private static void produceAnswer(String input) throws SQLException {
        switch (input.toLowerCase()) {
        case "help":
        case "h":
            printHelp();
            break;
        case "r1":
            r1();
            break;
        case "r2":
            System.out.println("Performing R2");
            break;
        case "r3":
            System.out.println("Performing R3");
            break;
        case "r4":
            r4();
            break;
        case "r5":
            System.out.println("Performing R5");
            break;
        case "r6":
            System.out.println("Performing R6");
            break;
        default:
            System.out.println("Wrongly formatted expression");
            break;
        }
    }

    private static void printHelp() {
        String helpStr = "USAGE OF THE INN DATABASE\n......................................\n"
                + "• 'R1': Rooms and Rates.\n\tWhen this option is selected, the system will output a list of rooms to the user sorted by popularity (highest to lowest)\n"
                + "• 'R2': Reservations.\n\tWhen this option is selected, the system will produce a numbered list of matching rooms available for booking, along with\n\t"
                + "a prompt to allow booking by option number. If no exact matches are found, the system will suggest 5 possibilities for different rooms or dates.\n"
                + "• 'R3': Reservation Change.\n\tWhen this option is selected, the system will allow the user to make changes to an existing reservation, accepting from\n\t"
                + "the user a reservation code and new values for the reservation.\n"
                + "• 'R4': Reservation Cancellation.\n\tWhen this option is selected, the system will allow the user to cancel an existing reservation, accepting from the user a\n\t"
                + "reservation code, confirming the cancellation, then removing the reservation record from the database.\n"
                + "• 'R5': Detailed Reservation Information.\n\tWhen this option is selected, the system will present the user with a search prompt or form that allows them to enter any combination of the fields.\n"
                + "• 'R6': Revenue.\n\tWhen this option is selected, the system shall provide a month-by-month overview of revenue for an entire year.\n"
                + "......................................\n";

        System.out.println(helpStr);
    }

    private static void r1() throws SQLException {
        try (Connection conn = DriverManager.getConnection(System.getenv("L7_JDBC_URL"), System.getenv("L7_JDBC_USER"),
                System.getenv("L7_JDBC_PW"))) {

            String sql = "with LengthOfStay as "
                    + "(select Room, max(CheckIn) CI, max(CheckOut) RecentCO, DATEDIFF(max(CheckOut), max(CheckIn)) LengthPrev "
                    + "from lab7_reservations " + "where CheckIn <= CURDATE() " + "group by room "
                    + "order by LengthPrev desc, RecentCO)" + ", NextDay as " + "(select Room, "
                    + "(case when min(CheckIn) < CURDATE() then min(CheckOut) " + "else CURDATE() "
                    + "end) NextAvailable " + "from lab7_reservations " + "where CheckOut >= CURDATE() "
                    + "group by Room " + "order by NextAvailable)" + ", RoomPopularity as "
                    + "(select rm.RoomCode Room, " + "round(sum( "
                    + "(case when datediff(CURDATE(), rv.Checkout) <= 180 and datediff(CURDATE(), rv.Checkout) >= 0 "
                    + "then datediff(rv.Checkout, rv.Checkin) " + "else 0 " + "end) - "
                    + "(case when datediff(CURDATE(), rv.Checkin) > 180 and datediff(CURDATE(), rv.Checkout) <= 180 "
                    + "then datediff(CURDATE(), rv.Checkin) - 180 " + "else 0 " + "end)) / 180, 2) Popularity "
                    + "from lab7_rooms rm join lab7_reservations rv on rm.RoomCode = rv.Room " + "group by rm.RoomCode "
                    + "order by Popularity) "
                    + "select rm.RoomCode, rm.RoomName, rm.Beds, rm.bedType, rm.maxOcc, rm.basePrice, rm.decor, Popularity, NextAvailable, LengthPrev, RecentCO from RoomPopularity "
                    + "inner join NextDay on RoomPopularity.Room=NextDay.Room "
                    + "inner join LengthOfStay on LengthOfStay.Room=NextDay.Room "
                    + "inner join lab7_rooms rm on rm.RoomCode = NextDay.Room "
                    + "order by Popularity desc, NextAvailable, LengthPrev desc, RecentCO;";

            try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
                TableGenerator tableGenerator = new TableGenerator();

                List<String> headersList = new ArrayList<>();
                headersList.add("RoomCode");
                headersList.add("RoomName");
                headersList.add("Beds");
                headersList.add("bedType");
                headersList.add("maxOcc");
                headersList.add("basePrice");
                headersList.add("decor");
                headersList.add("Popularity");
                headersList.add("NextAvailable");
                headersList.add("LengthPrev");
                headersList.add("RecentCO");

                List<List<String>> rowsList = new ArrayList<>();

                while (rs.next()) {
                    List<String> row = new ArrayList<>();
                    row.add(rs.getString("rm.RoomCode"));
                    row.add(rs.getString("rm.RoomName"));
                    row.add(String.valueOf((int) rs.getFloat("rm.Beds")));
                    row.add(rs.getString("rm.bedType"));
                    row.add(String.valueOf((int) rs.getFloat("rm.maxOcc")));
                    row.add(String.valueOf(rs.getFloat("rm.basePrice")));
                    row.add(rs.getString("rm.decor"));
                    row.add(String.valueOf(rs.getFloat("Popularity")));
                    row.add(rs.getString("NextAvailable"));
                    row.add(String.valueOf((int) rs.getFloat("LengthPrev")));
                    row.add(rs.getString("RecentCO"));

                    rowsList.add(row);

                    // System.out.format("%s %s %d %s %d ($%.2f) %s ($%.2f) %s %d %s %n", roomCode,
                    // roomName, (int) beds, bedType, (int) maxOcc, basePrice, decor, popularity,
                    // nextAvailable, (int) lengthPrev, recentCO);
                }
                System.out.println(tableGenerator.generateTable(headersList, rowsList));
            }
        }
    }

    private static void r4() throws SQLException {
      System.out.print("\nPlease enter a reservation code (example: 12345) that you would like to cancel: ");
      Scanner scanner = new Scanner(System.in);
      String res = scanner.nextLine().trim();

      System.out.print("Are you sure you want to cancel reservation " + res + "? (Y or N) ");
      String confirm = scanner.nextLine().trim().toLowerCase();
      while (!(confirm.equals("y") || confirm.equals("n"))) {
         System.out.print("Sorry, are you sure you want to cancel reservation " + res + "? (Y or N) ");
         confirm = scanner.nextLine().trim().toLowerCase();
      }
      if (confirm.equals("y")) {
         try (Connection conn = DriverManager.getConnection(System.getenv("L7_JDBC_URL"), System.getenv("L7_JDBC_USER"),
                 System.getenv("L7_JDBC_PW"))) {
             List<Object> params = new ArrayList<Object>();
             params.add(res);
             StringBuilder sb = new StringBuilder("DELETE FROM lab7_reservations WHERE CODE = ?");
             try (PreparedStatement pstmt = conn.prepareStatement(sb.toString())) {
                 int i = 1;
                 for (Object p : params) {
                     pstmt.setObject(i++, p);
                 }
                 int rowCount = pstmt.executeUpdate();
                 if (rowCount > 0) {
                    System.out.println("Cancelled reservation " + res + ", " + rowCount + " row(s) affected");
                 }
                 else {
                    System.out.println("No matching records were found with reservation code " + res);
                 }
             }
         }
      }
      else {
         return;
      }
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

            String roomsSql = "CREATE TABLE IF NOT EXISTS lab7_rooms (" + "RoomCode char(5) PRIMARY KEY, "
                    + "RoomName varchar(30) NOT NULL, " + "Beds int(11) NOT NULL, " + "bedType varchar(8) NOT NULL, "
                    + "maxOcc int(11) NOT NULL, " + "basePrice DECIMAL(6,2) NOT NULL, " + "decor varchar(20) NOT NULL, "
                    + "UNIQUE (RoomName)" + ");";

            String reservationsSql = "CREATE TABLE IF NOT EXISTS lab7_reservations (" + "CODE int(11) PRIMARY KEY, "
                    + "Room char(5) NOT NULL, " + "CheckIn date NOT NULL, " + "Checkout date NOT NULL, "
                    + "Rate DECIMAL(6,2) NOT NULL, " + "LastName varchar(15) NOT NULL, "
                    + "FirstName varchar(15) NOT NULL, " + "Adults int(11) NOT NULL, " + "Kids int(11) NOT NULL, "
                    + "UNIQUE (Room, CheckIn), " + "UNIQUE (Room, Checkout), "
                    + "FOREIGN KEY (Room) REFERENCES lab7_rooms (RoomCode)" + ");";

            String roomsInsertSql = "INSERT INTO lab7_rooms SELECT * FROM INN.rooms;";

            String reservationsInsertSql = "INSERT INTO lab7_reservations SELECT CODE, Room, "
                    + "DATE_ADD(CheckIn, INTERVAL 9 YEAR), " + "DATE_ADD(Checkout, INTERVAL 9 YEAR), "
                    + "Rate, LastName, FirstName, Adults, Kids FROM INN.reservations;";

            Statement stmt = conn.createStatement();
            stmt.addBatch(dropReservationsSql);
            stmt.addBatch(dropRoomsSql);
            stmt.addBatch(roomsSql);
            stmt.addBatch(reservationsSql);
            stmt.addBatch(roomsInsertSql);
            stmt.addBatch(reservationsInsertSql);
            stmt.executeBatch();

            System.out.println("Successfully setup Room and Reservations tables\n");
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