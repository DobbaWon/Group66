import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;

public class Code {
    private static class CharlieReader {
        public CharlieReader() {
            String databaseName = "CharlieDB";
            Connection connection = createDatabase(databaseName);

            createTables(connection);
            String[][] csv = extractCSV("38700514.csv");
            populateTables(csv, connection);
            runQueries(connection);
        }

        private void createTables(Connection connection) {
            try {
                Statement statement = connection.createStatement();

                String createManagerTable = "CREATE TABLE IF NOT EXISTS Manager(Manager_ID INTEGER NOT NULL, First_Name VARCHAR(50), Last_Name VARCHAR(50), Age INTEGER, PRIMARY KEY (Manager_ID))";
                statement.executeUpdate(createManagerTable);
                String createTeamTable = "CREATE TABLE IF NOT EXISTS Team(Team_ID INTEGER NOT NULL, Team_Name VARCHAR(100), Team_Abbreviation VARCHAR(5), Year_Founded INTEGER, Manager_ID INTEGER NOT NULL UNIQUE, PRIMARY KEY(Team_ID)), FOREIGN KEY(Manager_ID) REFERENCES Manager(Manager_ID))";
                statement.executeUpdate(createTeamTable);
                String createPlayerTable = "CREATE TABLE IF NOT EXISTS Player(Player_ID INTEGER NOT NULL, First_Name VARCHAR(50), Last_Name VARCHAR(50), Shirt_Number INTEGER, Age INTEGER, Team_ID INTEGER, PRIMARY KEY(Player_ID), FOREIGN KEY(Team_ID) REFERENCES Team(Team_ID))";
                statement.executeUpdate(createPlayerTable);

                System.out.println("Created Tables");

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        
        private void populateTables(String[][] csv, Connection connection){
            try{
                Statement statement = connection.createStatement();
                // Team table is column 0-4
                // Manager table is column 5-8
                // Player table is column 9-14
                // Add one to the y index as we skip column headers

                for (int i = 1; i < 21; i++){
                    // Filling the team and manager tables:

                    String insertTeam =     "INSERT IGNORE INTO Team (Team_ID, Team_Name, Team_Abbreviation, Year_Founded, Manager_ID) VALUES (" +csv[0][i] + ", '" + csv[1][i] + "', '" + csv[2][i] + "', " + csv[3][i] + ", " + csv[4][i] + ")";
                    statement.executeUpdate(insertTeam);
                    
                    String insertManager = "INSERT IGNORE INTO Manager (Manager_ID, First_Name, Last_Name, Age) VALUES (" + csv[5][i] + ", '" + csv[6][i] + "', '" + csv[7][i] + "', " + csv[8][i] + ")";
                    statement.executeUpdate(insertManager);
                }

                for (int i = 1; i < 201; i++){
                    // Filling the player tables:
                    String insertPlayer =  "INSERT IGNORE INTO Player (Player_ID, First_Name, Last_Name, Shirt_Number, Age, Team_ID) " "VALUES (" + csv[9][i] + ", '" + csv[10][i] + "', '" + csv[11][i] + "', " + csv[12][i] + ", " + csv[13][i] + ", " + csv[14][i] + ")";
                    statement.executeUpdate(insertPlayer);
                }

                System.out.println("Populated Tables");
                
            } catch (SQLException e){
                e.printStackTrace();
            }
            
        }

        private void runQueries(Connection connection){
            String averageAge = "SELECT Manager.Age, Team.Manager_ID, Manager.First_Name, Manager.Last_Name FROM Manager, Team WHERE Team.Manager_ID = Manager.Manager_ID GROUP BY Manager.Age, Team.Manager_ID ORDER BY Manager.Age";
            
            String managersByAge = "SELECT Manager.Age, Team.Manager_ID, Manager.First_Name, Manager.Last_Name FROM Manager, Team WHERE Team.Manager_ID = Manager.Manager_ID GROUP BY Manager.Age, Team.Manager_ID ORDER BY Manager.Age";
            
            String deleteArsenalManager = "DELETE Manager_ID FROM Manager WHERE Manager_ID = 1";

            String deleteArsenal = "DELETE Team_ID FROM Team WHERE Team_ID = 1";

            try {
                Statement statement = connection.createStatement();
                statement.executeQuery(averageAge);
                statement.executeQuery(managersByAge);
                try{
                    statement.executeQuery(deleteArsenalManager);
                    System.out.println("Deleted Arsenal's Manager Successfully");

                } catch (SQLException e){
                    if (e.getErrorCode() == 1451) {
                        System.out.println("Deleting Arsenal's Manager failed: Foreign Key constraint violated.\n");
                    } else {
                        e.printStackTrace();
                    }
                }
                try{
                    statement.executeQuery(deleteArsenal);
                    System.out.println("Deleted Arsenal Successfully");

                } catch (SQLException e){
                    if (e.getErrorCode() == 1451) {
                        System.out.println("Deleting Arsenal failed: Foreign Key constraint violated.\n");
                    } else {
                        e.printStackTrace();
                    }
                }
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static class JoeReader {
        public JoeReader() {
            String databaseName = "JoeDB";
            String csvName = "38771225.csv";
            Connection connection = createDatabase(databaseName);

            createTables(connection);

            String[][] data = extractCSV(csvName);
            populateTables(data, connection, databaseName);

            dmlQueries(connection);

            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        private void createTables(Connection connection) {
            try {
                Statement statement = connection.createStatement();

                String createManagerTable = "CREATE TABLE IF NOT EXISTS Manager ( " +
                                            "Manager_ID INTEGER NOT NULL, " +
                                            "First_Name VARCHAR(50), " +
                                            "Last_Name VARCHAR(50), " +
                                            "Age INTEGER, " +
                                            "PRIMARY KEY (Manager_ID) );";
                statement.executeUpdate(createManagerTable);

                String createTeamTable =    "CREATE TABLE IF NOT EXISTS Team ( " +
                                            "Team_ID INTEGER NOT NULL, " +
                                            "Team_Name VARCHAR(100), " +
                                            "Team_Abbreviation VARCHAR(5), " +
                                            "Manager_ID INTEGER NOT NULL UNIQUE, " +
                                            "Year_Founded INTEGER, " +
                                            "PRIMARY KEY (Team_ID), " +
                                            "FOREIGN KEY (Manager_ID) REFERENCES Manager(Manager_ID) );";
                statement.executeUpdate(createTeamTable);

                String createPlayerTable =  "CREATE TABLE IF NOT EXISTS Player ( " +
                                            "Player_ID INTEGER NOT NULL, " +
                                            "First_Name VARCHAR(50), " +
                                            "Last_Name VARCHAR(50), " +
                                            "Team_ID INTEGER NOT NULL, " +
                                            "Age INTEGER, " +
                                            "Shirt_Number INTEGER, " +
                                            "PRIMARY KEY (Player_ID), " +
                                            "FOREIGN KEY (Team_ID) REFERENCES Team(Team_ID) );";
                statement.executeUpdate(createPlayerTable);

                System.out.println("Tables created successfully.");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        private void populateTables(String[][] csv, Connection connection, String databaseName) {
            try {
                Statement statement = connection.createStatement();

                for (int i = 0; i < csv[0].length; i++) {
                    String insertTeam =     "INSERT IGNORE INTO Team (" +
                                            "Team_ID, Team_Name, Team_Abbreviation, Manager_ID, Year_Founded) " +
                                            "VALUES (" +
                                            csv[0][i] + ", '" + csv[1][i] + "', '" + csv[2][i] + "', " + csv[3][i] + ", " + csv[4][i] + ")";
                    statement.executeUpdate(insertTeam);
                }

                for (int i = 0; i < csv[5].length; i++) {
                    String insertManager =  "INSERT IGNORE INTO Manager (" +
                                            "Manager_ID, First_Name, Last_Name, Age) " +
                                            "VALUES (" +
                                            csv[5][i] + ", '" + csv[6][i] + "', '" + csv[7][i] + "', " + csv[8][i] + ")";
                    statement.executeUpdate(insertManager);
                }

                for (int i = 0; i < csv[9].length; i++) {
                    String insertPlayer =  "INSERT IGNORE INTO Player (" +
                                            "Player_ID, First_Name, Last_Name, Team_ID, Age, Shirt_Number) " +
                                            "VALUES (" +
                                            csv[9][i] + ", '" + csv[10][i] + "', '" + csv[11][i] + "', " + csv[12][i] + ", " + csv[13][i] + ", " + csv[14][i] + ")";
                    statement.executeUpdate(insertPlayer);
                }

                System.out.println("Tables for database '" + databaseName + "' populated.\n");

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        private void dmlQueries(Connection connection) {
            try {
                Statement statement = connection.createStatement();

                String deletionA =  "DELETE FROM Manager " +
                                    "WHERE Manager_ID = 1";

                try {
                    statement.executeUpdate(deletionA);
                    System.out.println("Manager_ID = 1 has been deleted from the table 'Manager'.\n");
                } catch (SQLException e) {
                    if (e.getErrorCode() == 1451) {
                        System.out.println("Deletion query A failed: Foreign Key constraint violated.\n");
                    } else {
                        e.printStackTrace();
                    }
                }
                
                String deletionB =  "DELETE FROM Team " +
                                    "WHERE Team_ID = 1";

                try {
                    statement.executeUpdate(deletionB);
                    System.out.println("Team_ID = 1 has been deleted from the table 'Team'.\n");
                } catch (SQLException e) {
                    if (e.getErrorCode() == 1451) {
                        System.out.println("Deletion query B failed: Foreign Key constraint violated.\n");
                    } else {
                        e.printStackTrace();
                    }
                }

                String groupByA =   "SELECT First_Name, Last_Name, Shirt_Number " +
                                    "FROM Player " +
                                    "WHERE Team_ID = 1 AND Shirt_Number < 10 " +
                                    "GROUP BY First_Name, Last_Name, Shirt_Number";
                
                try (
                    ResultSet resultSetA = statement.executeQuery(groupByA);
                ) {
                    System.out.println("Players who play for Team_ID = 1 with shirt numbers less than 10:");

                    while (resultSetA.next()) {
                        String firstName = resultSetA.getString("First_Name");
                        String lastName = resultSetA.getString("Last_Name");
                        int shirtNumber = resultSetA.getInt("Shirt_Number");
                        System.out.println(firstName + " " + lastName + ", Shirt Number: " + shirtNumber);
                    }

                    System.out.println("\n");
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                String groupByB =   "SELECT First_Name, Last_Name, Age " +
                                    "FROM Manager " +
                                    "WHERE Age > 50 " +
                                    "GROUP BY First_Name, Last_Name, Age";

                try (
                    ResultSet resultSetB = statement.executeQuery(groupByB);
                ) {
                    System.out.println("Managers who are older than 50:");

                    while (resultSetB.next()) {
                        String firstName = resultSetB.getString("First_Name");
                        String lastName = resultSetB.getString("Last_Name");
                        int age = resultSetB.getInt("Age");
                        System.out.println(firstName + " " + lastName + ", Age: " + age);
                    }

                    System.out.println("\n");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static class KaweeshaReader {
        public KaweeshaReader() {
            String databaseName = "KaweeshaDB";
            String csvName = "39166236.csv";
            Connection connection = createDatabase(databaseName);

            createTables(connection);
            String[][] data = extractCSV(csvName);
            populateTables(data, connection, databaseName);
        }
        private void createTables(Connection connection) {
            try {
                Statement statement = connection.createStatement();

                String createManagerTable = "CREATE TABLE IF NOT EXISTS Manager ( " +
                                            "Manager_ID INTEGER NOT NULL, " +
                                            "First_Name VARCHAR(50), " +
                                            "Last_Name VARCHAR(50), " +
                                            "PRIMARY KEY (Manager_ID) );";
                statement.executeUpdate(createManagerTable);

                String createTeamTable =    "CREATE TABLE IF NOT EXISTS Team ( " +
                                            "Team_ID INTEGER NOT NULL, " +
                                            "Team_Name VARCHAR(100), " +
                                            "Team_Abbreviation VARCHAR(5), " +
                                            "Year_Founded DATE, " +
                                            "Manager_ID INTEGER NOT NULL, " +
                                            "PRIMARY KEY (Team_ID), " +
                                            "FOREIGN KEY (Manager_ID) REFERENCES Manager(Manager_ID) )" +
                                            "CONSTRAINT fk_Team_Manager FOREIGN KEY (Manager_ID) REFERENCES Manager (Manager_ID) ON DELETE RESTRICT "
                                            ;
                statement.executeUpdate(createTeamTable);

                String createPlayerTable =  "CREATE TABLE IF NOT EXISTS Player ( " +
                                            "Player_ID INTEGER NOT NULL, " +
                                            "First_Name VARCHAR(50), " +
                                            "Last_Name VARCHAR(50), " +
                                            "Shirt_Number INTEGER, " +
                                            "Position VARCHAR(50)," +
                                            "Date_Of_Birth DATE, " +
                                            "Nationality VARCHAR(2)," +
                                            "Team_ID INTEGER NOT NULL, " +
                                            "PRIMARY KEY (Player_ID), " +
                                            "CONSTRAINT fk_Player_Team FOREIGN KEY (Team_ID) REFERENCES Team (Team_ID) " +
                                            "ON DELETE RESTRICT;";
                statement.executeUpdate(createPlayerTable);

                System.out.println("Kaw Tables created successfully.");

            } catch(SQLException e) {
                e.printStackTrace();
            }
        }             
        private void populateTables(String[][] csv, Connection connection, String databaseName) {
            try {
                Statement statement = connection.createStatement();

                for (int i = 0; i <csv[0].length;i++) {
                    String insertTeam = "INSERT IGNORE INTO Team (" +
                                        "Team_ID, Team_Name, Team_Abbreviation, Year_Founded, Manager_ID)" +
                                        "VALUES (" +
                                            csv[0][i] + ", '" + csv[1][i] + "', '" + csv[2][i] + "', " + csv[3][i] + ", " + csv[4][i] + ")";


                }

                for (int i = 0; i < csv[13].length; i++) {
                    String insertManager =  "INSERT IGNORE INTO Manager (" +
                                            "Manager_ID, First_Name, Last_Name)" +
                                            "VALUES (" +
                                            csv[13][i] + ", '" + csv[14][i] + "', '" + csv[15][i] + ")";
                    statement.executeUpdate(insertManager);

                }
                for (int i = 0; i < csv[5].length; i++) {
                    String insertPlayer =  "INSERT IGNORE INTO Player (" +
                                            "Player_ID, First_Name, Last_Name,Shirt_Number, Nationality, Position, Date_Of_Birth, Team_ID)" +
                                            "VALUES (" +
                                            csv[5][i] + ", '" + csv[6][i] + "', '" + csv[7][i] + "', " + csv[8][i] + ", " + csv[9][i] + ", " + csv[10][i] + ", " + csv[11][i] + ", " + csv[12][i] + ")";
                    statement.executeUpdate(insertPlayer);
                }
                System.out.println("Tables for database '" + databaseName + "' populated.");
                } catch(SQLException e) {
                    e.printStackTrace();
                }
            }


        private Connection createDatabase(String databaseName) {
            String jdbcUrl = "jdbc:mysql://localhost:3306/";
    
            try (
                Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()
            ) {
                String createDatabase = "CREATE DATABASE IF NOT EXISTS " + databaseName;
                statement.executeUpdate(createDatabase);
                System.out.println("Database '" + databaseName + "' created successfully.");
    
                String useDatabase = "USE " + databaseName;
                statement.executeUpdate(useDatabase);
                System.out.println("Database '" + databaseName + "' is now in use.");
                return connection;
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
        }

        private void createTables() {

        }
    }

    public static Connection createDatabase(String databaseName) {
        String jdbcUrl = "jdbc:mysql://localhost:3306/";

        try {
            Connection connection = DriverManager.getConnection(jdbcUrl);
            Statement statement = connection.createStatement();

            String createDatabase = "CREATE DATABASE IF NOT EXISTS " + databaseName;
            statement.executeUpdate(createDatabase);
            System.out.println("Database '" + databaseName + "' created successfully.");

            String useDatabase = "USE " + databaseName;
            statement.executeUpdate(useDatabase);

            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String[][] extractCSV(String filename){
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            List<String[]> allRows = new ArrayList<>();

            // Read each line from the CSV file
            while ((line = br.readLine()) != null) {
                // Split the line into an array of values using a comma as the delimiter
                String[] row = line.split(",");
                allRows.add(row);
            }

            // Assuming the first row contains column headers
            String[] headers = allRows.get(0);

            // Create arrays for each column dynamically
            int numColumns = headers.length;
            String[][] dataArrays = new String[numColumns][];

            // Initialize arrays
            for (int i = 0; i < numColumns; i++) {
                dataArrays[i] = new String[allRows.size()];
            }
            // System.out.println(allRows.size());
            // System.out.println(numColumns);

            // Populate arrays with data
            for (int i = 1; i < allRows.size(); i++) {
                String[] row = allRows.get(i);
                for (int j = 0; j < numColumns; j++) {
                    dataArrays[j][i - 1] = row[j];
                }
            }

            return dataArrays; 
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


    public static void main(String[] args) {
        CharlieReader charlie = new CharlieReader();
        // JoeReader joe = new JoeReader();
         KaweeshaReader kaweesha = new KaweeshaReader();
    }
}
