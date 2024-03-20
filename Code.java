import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
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
            String[][] csv = extractCSV("test");
            populateTables(csv, connection);
            runQueries(connection);
        }

        private void createTables(Connection connection) {
            try {
                Statement statement = connection.createStatement();

                String createManagerTable = "CREATE TABLE IF NOT EXISTS Manager(Manager_ID INTEGER, First_Name VARCHAR(50), Last_Name VARCHAR(50), Age INTEGER, PRIMARY KEY (Manager_ID))";
                statement.executeUpdate(createManagerTable);
                String createTeamTable = "CREATE TABLE IF NOT EXISTS Team(Team_ID INTEGER, Team_Name VARCHAR(100), Team_Abbreviation VARCHAR(5), Year_Founded INTEGER, Manager_ID INTEGER NOT NULL, PRIMARY KEY(Team_ID)), FOREIGN KEY(Manager_ID) REFERENCES Manager(Manager_ID))";
                statement.executeUpdate(createTeamTable);
                String createPlayerTable = "CREATE TABLE IF NOT EXISTS Player(Player_ID INTEGER, First_Name VARCHAR(50), Last_Name VARCHAR(50), Shirt_Number INTEGER, Age INTEGER, Team_ID INTEGER, PRIMARY KEY(Player_ID), FOREIGN KEY(Team_ID) REFERENCES Team(Team_ID))";
                statement.executeUpdate(createPlayerTable);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        
        private void populateTables(String[][] csv, Connection connection){

        }

        private void runQueries(Connection connection){
            try {
                Statement statement = connection.createStatement();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            
            String averageAge;
            String managersByAge;
            String deleteArsenalManager;
            String youngestTwenty;
        }
    }

    private static class JoeReader {
        public JoeReader() {
            String databaseName = "JoeDB";
            Connection connection = createDatabase(databaseName);

            createTables(connection);
        }

        private void createTables(Connection connection) {
            try {
                Statement statement = connection.createStatement();

                String createManagerTable = "CREATE TABLE Manager ( " +
                                            "Manager_ID INTEGER NOT NULL, " +
                                            "First_Name VARCHAR(50), " +
                                            "Last_Name VARCHAR(50), " +
                                            "Age INTEGER, " +
                                            "PRIMARY KEY (Manager_ID) );";
                statement.executeUpdate(createManagerTable);
                System.out.println("Manager table created.");

                String createTeamTable =    "CREATE TABLE Team ( " +
                                            "Team_ID INTEGER NOT NULL, " +
                                            "Team_Name VARCHAR(100), " +
                                            "Team_Abbreviation VARCHAR(5), " +
                                            "Manager_ID INTEGER NOT NULL, " +
                                            "Year_Founded INTEGER, " +
                                            "PRIMARY KEY (Team_ID), " +
                                            "FOREIGN KEY (Manager_ID) REFERENCES Manager(Manager_ID) );";
                statement.executeUpdate(createTeamTable);
                System.out.println("Team table created.");

                String createPlayerTable =  "CREATE TABLE Player ( " +
                                            "Player_ID INTEGER NOT NULL, " +
                                            "First_Name VARCHAR(50), " +
                                            "Last_Name VARCHAR(50), " +
                                            "Team_ID INTEGER NOT NULL, " +
                                            "Age INTEGER, " +
                                            "Shirt_Number INTEGER, " +
                                            "PRIMARY KEY (Player_ID), " +
                                            "FOREIGN KEY (Team_ID) REFERENCES Team(Team_ID) );";
                statement.executeUpdate(createPlayerTable);
                System.out.println("Player table created.");

                System.out.println("Tables created successfully.");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static class KaweeshaReader {
        public KaweeshaReader() {
            String databaseName = "KaweeshaDB";
            Connection connection = createDatabase(databaseName);
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
            System.out.println(allRows.size());
            System.out.println(numColumns);

            // Populate arrays with data
            for (int i = 1; i < allRows.size()-1; i++) {
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
        // CharlieReader charlie = new CharlieReader();
        JoeReader joe = new JoeReader();
        // KaweeshaReader kaweesha = new KaweeshaReader();
    }
}