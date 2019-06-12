import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
        System.exit(0);
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
            r2();
            break;
        case "r3":
            r3();
            break;
        case "r4":
            r4();
            break;
        case "r5":
            r5();
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

    private static boolean validDate(String inDate) {
        String[] comps = inDate.split("/");
        if (comps.length > 3) {
            return false;
        }
        for (int i = 0; i < comps.length; i++) {
            try {
                Integer.parseInt(comps[i]);
            } catch (NumberFormatException e) {
                return false;
            }
            if (comps[i].length() != 2) {
                if (i!=2) {
                    return false;
                } else if (comps[i].length() != 4) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean validDate(String checkOut, String checkIn) {
        if (!validDate(checkOut)) {
            return false;
        }
        String[] checkInParts = checkIn.split("/");
        String[] checkOutParts = checkOut.split("/");
        for (int i = 2; i >= 0; i--) {
            if (Integer.parseInt(checkInParts[i]) > Integer.parseInt(checkOutParts[i])) {
                return false;
            }
        }
        return true;

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
                }
                System.out.println(tableGenerator.generateTable(headersList, rowsList));
            }
        }
    }

    private static void r2() throws SQLException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Let's get you booked in a room. Please enter some information to get going.");
        
        System.out.print("First Name: ");
        String fName = scanner.nextLine().trim();
        
        System.out.print("Last Name: ");
        String lName = scanner.nextLine().trim();
        
        System.out.print("Room Preference (if none enter 'Any'): ");
        String room = scanner.nextLine().trim();
        if (room.toLowerCase().equals("any")) {
            room = "%";
        }

        System.out.print("Desired Bed Type Preference (if none enter 'Any'): ");
        String bed = scanner.nextLine().trim();
        if (bed.toLowerCase().equals("any")) {
            bed = "%";
        }

        System.out.print("Check In Date (mm/dd/yyyy): ");
        String checkIn = scanner.nextLine().trim();
        while(!validDate(checkIn)) {
            System.out.println("That date is invalid, please try again.");
            System.out.print("Check In Date (mm/dd/yyyy): ");
            checkIn = scanner.nextLine().trim();
        }

        System.out.print("Check Out Date (mm/dd/yyyy): ");
        String checkOut = scanner.nextLine().trim();
        while(!validDate(checkOut, checkIn)) {
            System.out.println("That date is invalid, please try again.");
            System.out.print("Check Out Date (mm/dd/yyyy): ");
            checkOut = scanner.nextLine().trim();
        }

        int children = 0;
        while (true) {
            try {
                System.out.print("Number of Children: ");
                children = Integer.parseInt(scanner.nextLine().trim());
                break;
            } catch (NumberFormatException e) {
                System.out.println("Please enter a number.");
            }
        }

        int adults = 0;
        while (true) {
            try {
                System.out.print("Number of Adults: ");
                adults = Integer.parseInt(scanner.nextLine().trim());
                break;
            } catch (NumberFormatException e) {
                System.out.println("Please enter a number.");
            }
        }

        int occ = children + adults;

        String query = String.format("select * from lab7_rooms where maxOcc >= %d and bedType like \"%s\" and RoomCode like \"%s\" and RoomCode not in (select Room from lab7_reservations where CheckIn > \"%s\" and CheckOut < \"%s\" group by Room)", occ, "%", "%",formatDate(checkOut), formatDate(checkIn));
        String anyRoomMax = "select max(maxOcc) total from lab7_rooms";

        int res_num = 0;

        List<List<String>> rowsList = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(System.getenv("L7_JDBC_URL"), System.getenv("L7_JDBC_USER"), System.getenv("L7_JDBC_PW"))) {
            try(Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(anyRoomMax)) {
                while(rs.next()) {
                    int overallMax = rs.getInt("total");
                    if (overallMax < occ) {
                        System.out.println(String.format("We're sorry, we cannot accomodate parties larger than %d in any single room.\nPlease make multiple reservations.", overallMax));
                        return;
                    }
                }
            }
            try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
                TableGenerator tableGenerator = new TableGenerator();

                List<String> headersList = new ArrayList<>();
                headersList.add("OptionNumber");
                headersList.add("RoomCode");
                headersList.add("RoomName");
                headersList.add("Beds");
                headersList.add("bedType");
                headersList.add("maxOcc");
                headersList.add("basePrice");
                headersList.add("decor");

                while (rs.next()) {
                    res_num += 1;
                    List<String> row = new ArrayList<>();
                    row.add(Integer.toString(res_num));
                    row.add(rs.getString("RoomCode"));
                    row.add(rs.getString("RoomName"));
                    row.add(rs.getString("Beds"));
                    row.add(rs.getString("bedType"));
                    row.add(rs.getString("maxOcc"));
                    row.add(rs.getString("basePrice"));
                    row.add(rs.getString("decor"));

                    rowsList.add(row);
                }
                if (res_num >= 0){
                    System.out.println("Here are the avaialbe rooms,matching your dates, occupancy, and bed type.");
                    System.out.println(tableGenerator.generateTable(headersList, rowsList));
                } else {
                    // find similar rooms
                }
            }
        }

        System.out.print("Please choose the room option you would liike by giving the number. If you would like to cancel, type \'cancel\'.\nGive your selection: ");
        String selection = scanner.nextLine().trim();
        int choice = 0;
        if (selection.toLowerCase().equals("cancel")){
            return;
        } else {
            while (true) {
                try {
                    choice = Integer.parseInt(selection);
                    if (choice < 1 || choice > res_num) {
                        throw new NumberFormatException();
                    }
                    break;
                } catch (NumberFormatException e) {
                    System.out.print("Please enter a valid number\nGive your selection: ");
                    selection = scanner.nextLine().trim();
                }
            }
        }

        List<String> roomSelection = rowsList.get(choice-1);
        System.out.println(roomSelection);
        double totalCharge = 0;
        int totalDays = 0;
        String bad = String.format("select DATEDIFF(\"%s\", \"%s\") Diff", formatDate(checkOut), formatDate(checkIn));
        try (Connection conn = DriverManager.getConnection(System.getenv("L7_JDBC_URL"), System.getenv("L7_JDBC_USER"), System.getenv("L7_JDBC_PW"))) {
            try(Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(bad)) {
                double baseRate = Double.parseDouble(roomSelection.get(6));
                while(rs.next()){
                    totalDays = rs.getInt("Diff");
                }
                String day;
                int dayOfWeek = 0;
                for (int i = 0; i < totalDays; i++) {
                    String tempQuery = String.format("SELECT DATE_ADD(\"%s\", INTERVAL %d DAY) day", formatDate(checkIn), i);
                    System.out.println(tempQuery);
                    try (Connection connect = DriverManager.getConnection(System.getenv("L7_JDBC_URL"), System.getenv("L7_JDBC_USER"), System.getenv("L7_JDBC_PW"))) {
                        try(Statement satmt = connect.createStatement(); ResultSet res = satmt.executeQuery(tempQuery)) {
                            while(res.next()){
                                day = res.getString("day");
                            }
                        }
                    }
                    try (Connection connection = DriverManager.getConnection(System.getenv("L7_JDBC_URL"), System.getenv("L7_JDBC_USER"), System.getenv("L7_JDBC_PW"))) {
                        try(Statement stamt = connection.createStatement(); ResultSet resSet = stamt.executeQuery(tempQuery)) {
                            while(resSet.next()) {
                                dayOfWeek = resSet.getInt("day");
                                System.out.println(dayOfWeek);
                                if (dayOfWeek == 7 || dayOfWeek == 1) {
                                    totalCharge += baseRate*1.1;
                                }
                            }
                        }
                    }
                    
                }
            }
        }
        String sb = String.format("INSERT INTO lab7_reservations (CODE, Room, CheckIn, CheckOut, Rate, LastName, Adults, Kids) VALUES (\"%d\", \"%s\", \"%s\", \"%s\", \"%.02f\", \"%s\", \"%s\", \"%d\", \"%d\"", 00001, roomSelection.get(1), formatDate(checkIn), formatDate(checkOut), totalCharge/totalDays, adults, children);
        try (Connection conn = DriverManager.getConnection(System.getenv("L7_JDBC_URL"), System.getenv("L7_JDBC_USER"), System.getenv("L7_JDBC_PW"))) {
            try(Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sb)) {

            }
        }
        

        return;

    }

    private static String formatDate(String date) {
        String[] subs = date.split("/");
        return String.format("%s-%s-%s", subs[2], subs[0], subs[1]);
    }

    private static void r3() throws SQLException {
        Scanner scanner = new Scanner(System.in);
        List<Object> params = new ArrayList<Object>();
        int paramCount = 0;
        StringBuilder sb = new StringBuilder("UPDATE lab7_reservations SET");

        System.out.print("\nPlease enter a Reservation code for updating the reservation: ");
        String resCode = scanner.nextLine().trim();
        while (resCode.equalsIgnoreCase("")) {
            System.out.print("Sorry, please enter a Reservation code for updating the reservation: ");
            resCode = scanner.nextLine().trim();
        }

        System.out.print("\nPlease enter a First name (or leave blank for 'no change'): ");
        String firstName = scanner.nextLine().trim();
        if (!"".equalsIgnoreCase(firstName)) {
            paramCount++;
            sb.append(" FirstName = ?");
            params.add(firstName);
        }

        System.out.print("\nPlease enter a Last name (or leave blank for 'no change'): ");
        String lastName = scanner.nextLine().trim();
        if (!"".equalsIgnoreCase(lastName)) {
            paramCount++;
            if (paramCount > 1)
                sb.append(",");
            else
                sb.append(" LastName = ?");
            params.add(lastName);
        }

        System.out.print("\nPlease enter a CheckIn date (or leave blank for 'no change'): ");
        String checkIn = scanner.nextLine().trim();
        if (!"".equalsIgnoreCase(checkIn)) {
            paramCount++;
            if (paramCount > 1)
                sb.append(",");
            else
                sb.append(" CheckIn = ?");
            params.add(checkIn);
        }

        System.out.print("\nPlease enter a CheckOut date (or leave blank for 'no change'): ");
        String checkOut = scanner.nextLine().trim();
        if (!"".equalsIgnoreCase(checkOut)) {
            paramCount++;
            if (paramCount > 1)
                sb.append(",");
            else
                sb.append(" CheckOut = ?");
            params.add(checkOut);
        }

        System.out.print("\nPlease enter the Number of children (or leave blank for 'no change'): ");
        String numChildren = scanner.nextLine().trim();
        if (!"".equalsIgnoreCase(numChildren)) {
            paramCount++;
            if (paramCount > 1)
                sb.append(",");
            else
                sb.append(" Kids = ?");
            params.add(numChildren);
        }

        System.out.print("\nPlease enter the Number of adults (or leave blank for 'no change'): ");
        String numAdults = scanner.nextLine().trim();
        if (!"".equalsIgnoreCase(numAdults)) {
            paramCount++;
            if (paramCount > 1)
                sb.append(",");
            else
                sb.append(" Adults = ?");
            params.add(numAdults);
        }

        if (paramCount == 0) {
            System.out.println("No entries updated");
            return;
        }

        sb.append(" WHERE CODE = ?");
        params.add(resCode);

        try (Connection conn = DriverManager.getConnection(System.getenv("L7_JDBC_URL"), System.getenv("L7_JDBC_USER"),
                System.getenv("L7_JDBC_PW"))) {
            try (PreparedStatement pstmt = conn.prepareStatement(sb.toString())) {
                int i = 1;
                for (Object p : params) {
                    pstmt.setObject(i++, p);
                }

                int rowCount = pstmt.executeUpdate();
                if (rowCount > 0) {
                    System.out.println("Updated reservation " + resCode + ", " + rowCount + " row(s) affected");
                } else {
                    System.out.println("No matching records were found with reservation code " + resCode);
                }
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
            try (Connection conn = DriverManager.getConnection(System.getenv("L7_JDBC_URL"),
                    System.getenv("L7_JDBC_USER"), System.getenv("L7_JDBC_PW"))) {
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
                    } else {
                        System.out.println("No matching records were found with reservation code " + res);
                    }
                }
            }
        } else {
            return;
        }
    }

    private static void r5() throws SQLException {
        Scanner scanner = new Scanner(System.in);
        List<Object> params = new ArrayList<Object>();
        int paramCount = 0;
        StringBuilder sb = new StringBuilder("SELECT * FROM lab7_reservations");

        System.out.print("\nPlease enter a First name (or leave blank for 'Any'): ");
        String firstName = scanner.nextLine().trim();
        if (!"".equalsIgnoreCase(firstName)) {
            paramCount++;
            if (paramCount == 1)
                sb.append(" WHERE FirstName LIKE ?");
            params.add(firstName);
        }

        System.out.print("\nPlease enter a Last name (or leave blank for 'Any'): ");
        String lastName = scanner.nextLine().trim();
        if (!"".equalsIgnoreCase(lastName)) {
            paramCount++;
            if (paramCount == 1)
                sb.append(" WHERE LastName LIKE ?");
            else
                sb.append(" AND LastName LIKE ?");
            params.add(lastName);
        }

        System.out.print(
                "\nPlease enter a range of dates (example format: '2019-10-01 to 2019-10-06') (or leave blank for 'Any'): ");
        String dateRange = scanner.nextLine().trim();
        String[] dateArr = dateRange.split(" to ");
        while (!(dateArr.length == 2 || dateRange.equalsIgnoreCase(""))) {
            System.out.print(
                    "Sorry, please enter a range of dates (example format: '2019-10-01 to 2019-10-06') (or leave blank for 'Any'): ");
            dateRange = scanner.nextLine().trim();
            dateArr = dateRange.split(" to ");
        }
        if (!"".equalsIgnoreCase(dateRange)) {
            paramCount++;
            if (paramCount == 1)
                sb.append(" WHERE (CheckIn between ? and ? OR CheckOut between ? and ?)");
            else
                sb.append(" AND (CheckIn between ? and ? OR CheckOut between ? and ?)");
            params.add(dateArr[0]);
            params.add(dateArr[1]);
            params.add(dateArr[0]);
            params.add(dateArr[1]);
        }

        System.out.print("\nPlease enter a Room code (or leave blank for 'Any'): ");
        String roomCode = scanner.nextLine().trim();
        if (!"".equalsIgnoreCase(roomCode)) {
            paramCount++;
            if (paramCount == 1)
                sb.append(" WHERE Room LIKE ?");
            else
                sb.append(" AND Room LIKE ?");
            params.add(roomCode);
        }

        System.out.print("\nPlease enter a Reservation code (or leave blank for 'Any'): ");
        String resCode = scanner.nextLine().trim();
        if (!"".equalsIgnoreCase(resCode)) {
            paramCount++;
            if (paramCount == 1)
                sb.append(" WHERE CODE LIKE ?");
            else
                sb.append(" AND CODE LIKE ?");
            params.add(resCode);
        }
        try (Connection conn = DriverManager.getConnection(System.getenv("L7_JDBC_URL"), System.getenv("L7_JDBC_USER"),
                System.getenv("L7_JDBC_PW"))) {
            try (PreparedStatement pstmt = conn.prepareStatement(sb.toString())) {
                int i = 1;
                for (Object p : params) {
                    pstmt.setObject(i++, p);
                }

                try (ResultSet rs = pstmt.executeQuery()) {
                    TableGenerator tableGenerator = new TableGenerator();
                    List<String> headersList = new ArrayList<>();
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int columnCount = rsmd.getColumnCount();

                    for (i = 1; i <= columnCount; i++) {
                        String name = rsmd.getColumnName(i);
                        headersList.add(name);
                    }

                    List<List<String>> rowsList = new ArrayList<>();

                    while (rs.next()) {
                        List<String> row = new ArrayList<>();
                        for (i = 1; i <= columnCount; i++) {
                            String name = rsmd.getColumnName(i);
                            String className = rsmd.getColumnClassName(i);
                            if (className.equalsIgnoreCase("java.math.BigDecimal")) {
                                row.add(String.valueOf(rs.getFloat(name)));
                            } else if (className.equalsIgnoreCase("java.lang.Integer")) {
                                row.add(String.valueOf((int) rs.getFloat(name)));
                            } else {
                                row.add(rs.getString(name));
                            }
                        }
                        rowsList.add(row);
                    }
                    System.out.println(tableGenerator.generateTable(headersList, rowsList));
                }
            }
        }
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

            String dropReservationsOverlapPreventionInsertTrigger = "DROP TRIGGER IF EXISTS prevent_overlap_reservations_insert;";

            String reservationsOverlapPreventionInsertTrigger = "CREATE TRIGGER prevent_overlap_reservations_insert BEFORE INSERT ON lab7_reservations "
                    + "FOR EACH ROW " + "BEGIN " + "IF (EXISTS (SELECT Code FROM lab7_reservations R "
                    + "WHERE R.Room = NEW.Room AND NOT NEW.CheckOut <= R.CheckIn AND NOT NEW.CheckIn >= R.CheckOut)) "
                    + "THEN " + "SIGNAL SQLSTATE '45000' "
                    + "SET MESSAGE_TEXT = 'ERROR: THERE EXISTS A CURRENT RESERVATION WITH THAT CONFLICTS WITH THIS DATE RANGE'; "
                    + "END IF; " + "END;";

            String dropReservationsOverlapPreventionUpdateTrigger = "DROP TRIGGER IF EXISTS prevent_overlap_reservations_update;";

            String reservationsOverlapPreventionUpdateTrigger = "CREATE TRIGGER prevent_overlap_reservations_update BEFORE UPDATE ON lab7_reservations "
                    + "FOR EACH ROW " + "BEGIN " + "IF (EXISTS (SELECT Code FROM lab7_reservations R "
                    + "WHERE R.Room = NEW.Room AND R.Code != NEW.Code AND NOT NEW.CheckOut <= R.CheckIn AND NOT NEW.CheckIn >= R.CheckOut)) "
                    + "THEN " + "SIGNAL SQLSTATE '45000' "
                    + "SET MESSAGE_TEXT = 'ERROR: THERE EXISTS A CURRENT RESERVATION WITH THAT CONFLICTS WITH THIS DATE RANGE'; "
                    + "END IF; " + "END;";

            Statement stmt = conn.createStatement();
            stmt.addBatch(dropReservationsSql);
            stmt.addBatch(dropRoomsSql);
            stmt.addBatch(roomsSql);
            stmt.addBatch(reservationsSql);
            stmt.addBatch(roomsInsertSql);
            stmt.addBatch(reservationsInsertSql);
            stmt.addBatch(dropReservationsOverlapPreventionInsertTrigger);
            stmt.addBatch(reservationsOverlapPreventionInsertTrigger);
            stmt.addBatch(dropReservationsOverlapPreventionUpdateTrigger);
            stmt.addBatch(reservationsOverlapPreventionUpdateTrigger);
            stmt.executeBatch();

            System.out.println("Successfully setup Room and Reservations tables\n");
        }
    }
}