import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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
                String createTeamTable = "CREATE TABLE IF NOT EXISTS Team(Team_ID INTEGER NOT NULL, Team_Name VARCHAR(100), Team_Abbreviation VARCHAR(5), Year_Founded INTEGER, Manager_ID INTEGER NOT NULL UNIQUE, PRIMARY KEY(Team_ID), FOREIGN KEY(Manager_ID) REFERENCES Manager(Manager_ID))";
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
                // Team table is column 0-4
                // Manager table is column 5-8
                // Player table is column 9-14
                // Add one to the y index as we skip column headers

                String insertTeam = "INSERT IGNORE INTO Team (Team_ID, Team_Name, Team_Abbreviation, Year_Founded, Manager_ID) VALUES (?, ?, ?, ?, ?)";
                String insertManager = "INSERT IGNORE INTO Manager (Manager_ID, First_Name, Last_Name, Age) VALUES (?, ?, ?, ?)";
                String insertPlayer = "INSERT IGNORE INTO Player (Player_ID, First_Name, Last_Name, Shirt_Number, Age, Team_ID) VALUES (?, ?, ?, ?, ?, ?)";

                PreparedStatement teamStatement = connection.prepareStatement(insertTeam);
                PreparedStatement managerStatement = connection.prepareStatement(insertManager);
                PreparedStatement playerStatement = connection.prepareStatement(insertPlayer);

                for (int i = 1; i < 21; i++){
                    // Filling the team and manager tables:
                    try {
                        teamStatement.setInt(1, Integer.parseInt(csv[0][i]));
                        teamStatement.setString(2, csv[1][i]);
                        teamStatement.setString(3, csv[2][i]);
                        teamStatement.setInt(4, Integer.parseInt(csv[3][i]));
                        teamStatement.setInt(5, Integer.parseInt(csv[4][i]));
                        teamStatement.executeUpdate();
                
                        managerStatement.setInt(1, Integer.parseInt(csv[5][i]));
                        managerStatement.setString(2, csv[6][i]);
                        managerStatement.setString(3, csv[7][i]);
                        managerStatement.setInt(4, Integer.parseInt(csv[8][i]));
                        managerStatement.executeUpdate();
                    } catch (NumberFormatException | NullPointerException e){
                    }
                }

                for (int i = 1; i < 201; i++){
                    // Filling the player tables:
                    try{
                        playerStatement.setInt(1, Integer.parseInt(csv[9][i]));
                        playerStatement.setString(2, csv[10][i]);
                        playerStatement.setString(3, csv[11][i]);
                        playerStatement.setInt(4, Integer.parseInt(csv[12][i]));
                        playerStatement.setInt(5, Integer.parseInt(csv[13][i]));
                        playerStatement.setInt(6, Integer.parseInt(csv[14][i]));
                        playerStatement.executeUpdate();
                    } catch (NumberFormatException | NullPointerException e){

                    }
                }

                System.out.println("Populated Tables");
                
            } catch (SQLException e){
                e.printStackTrace();
            }
            
        }

        private void runQueries(Connection connection){
            String averageAge = "SELECT Manager.Age, Team.Manager_ID, Manager.First_Name, Manager.Last_Name FROM Manager, Team WHERE Team.Manager_ID = Manager.Manager_ID GROUP BY Manager.Age, Team.Manager_ID ORDER BY Manager.Age";
            
            String managersByAge = "SELECT Manager.Age, Team.Manager_ID, Manager.First_Name, Manager.Last_Name FROM Manager, Team WHERE Team.Manager_ID = Manager.Manager_ID GROUP BY Manager.Age, Team.Manager_ID ORDER BY Manager.Age";
            
            String deleteArsenalManager = "DELETE FROM Manager WHERE Manager_ID = 1";

            String deleteArsenal = "DELETE FROM Team WHERE Team_ID = 1";

            try {
                Statement statement = connection.createStatement();
                Statement otherStatement = connection.createStatement();

                ResultSet resultsAvg = statement.executeQuery(averageAge);
                ResultSet resultsAge = otherStatement.executeQuery(managersByAge);

                System.out.println("Average Age:");
                while (resultsAvg.next()) {
                    System.out.println("Age: " + resultsAvg.getInt("Age") +
                                   ", Manager_ID: " + resultsAvg.getInt("Manager_ID") +
                                   ", First Name: " + resultsAvg.getString("First_Name") +
                                   ", Last Name: " + resultsAvg.getString("Last_Name"));
                }
                resultsAvg.close();
                statement.close();

                System.out.println("\nManagers by Age:");
                while (resultsAge.next()) {
                    System.out.println("Age: " + resultsAge.getInt("Age") +
                                   ", Manager_ID: " + resultsAge.getInt("Manager_ID") +
                                   ", First Name: " + resultsAge.getString("First_Name") +
                                   ", Last Name: " + resultsAge.getString("Last_Name"));
                }
                resultsAge.close();
                otherStatement.close();
                statement = connection.createStatement();
                try{
                    statement.executeUpdate(deleteArsenalManager);
                    System.out.println("Deleted Arsenal's Manager Successfully");

                } catch (SQLException e){
                    if (e.getErrorCode() == 1451) {
                        System.out.println("Deleting Arsenal's Manager failed: Foreign Key constraint violated.\n");
                    } else {
                        e.printStackTrace();
                    }
                }
                try{
                    statement.executeUpdate(deleteArsenal);
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
            populateTables(data, connection);
            dmlQueries(connection);
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
                                            "Years_Since_Foundation INTEGER, " +
                                            "Manager_ID INTEGER NOT NULL, " +
                                            "PRIMARY KEY (Team_ID), " +
                                            "FOREIGN KEY (Manager_ID) REFERENCES Manager(Manager_ID) );"
                                            ;
                statement.executeUpdate(createTeamTable);

                String createPlayerTable =  "CREATE TABLE IF NOT EXISTS Player ( " +
                                            "Player_ID INTEGER NOT NULL, " +
                                            "First_Name VARCHAR(50), " +
                                            "Last_Name VARCHAR(50), " +
                                            "Shirt_Number INTEGER, " +
                                            "Position VARCHAR(50)," +
                                            "Age INTEGER, " +
                                            "Nationality VARCHAR(50)," +
                                            "Team_ID INTEGER NOT NULL, " +
                                            "PRIMARY KEY (Player_ID), " +
                                            "FOREIGN KEY (Team_ID) REFERENCES Team(Team_ID) );";
                statement.executeUpdate(createPlayerTable);

                System.out.println("Kaw Tables created successfully.");

            } catch(SQLException e) {
                e.printStackTrace();
            }
        }             
        private void populateTables(String[][] csv, Connection connection) {
            try {
                //Team table is column 0-4
                //Player columns are 5-12
                //Manager table is column 13-15

                String insertManager = "INSERT INTO Manager (Manager_ID, First_Name, Last_Name) VALUES (?, ?, ?)";
                String insertTeam = "INSERT INTO Team (Team_ID, Team_Name, Team_Abbreviation, Years_Since_Foundation, Manager_ID) VALUES (?, ?, ?, ?, ?)";
                String insertPlayer = "INSERT  INTO Player (Player_ID, First_Name, Last_Name, Shirt_Number, Position, Age, Nationality, Team_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
                
                PreparedStatement teamStatement = connection.prepareStatement(insertTeam);
                PreparedStatement managerStatement = connection.prepareStatement(insertManager);
                PreparedStatement playerStatement = connection.prepareStatement(insertPlayer);


                for (int i = 0; i < 57; i++){
                    // Filling the team tables:
                    try {
                        managerStatement.setInt(1, Integer.parseInt(csv[4][i]));
                        managerStatement.setString(2, csv[5][i]);
                        managerStatement.setString(3, csv[6][i]);
                        managerStatement.executeUpdate();
                
                    } catch (NumberFormatException | NullPointerException e){
                       // System.out.println(e.getMessage());
                    }
                }

                for (int i = 0; i < 33; i++){
                    // Filling the team tables:
                    try {
                        teamStatement.setInt(1, Integer.parseInt(csv[0][i]));
                        teamStatement.setString(2, csv[1][i]);
                        teamStatement.setString(3, csv[2][i]);
                        teamStatement.setInt(4, Integer.parseInt(csv[3][i]));
                        teamStatement.setInt(5, Integer.parseInt(csv[4][i]));
                        teamStatement.executeUpdate();
                
                    } catch (NumberFormatException | NullPointerException e){
                     //   System.out.println(e.getMessage());
                    }
                }

                
                for (int i = 0; i < 246; i++){
                    // Filling the player tables:
                    try{
                        playerStatement.setInt(1, Integer.parseInt(csv[7][i]));
                        playerStatement.setString(2, csv[8][i]);
                        playerStatement.setString(3, csv[9][i]);
                        playerStatement.setInt(4, Integer.parseInt(csv[10][i]));
                        playerStatement.setString(5, csv[11][i]);
                        playerStatement.setInt(6, Integer.parseInt(csv[12][i]));
                        playerStatement.setString(7, csv[13][i]);
                        playerStatement.setInt(8, Integer.parseInt(csv[0][i]));
                        playerStatement.executeUpdate();
                    } catch (NumberFormatException | NullPointerException e){
                        //System.out.println(e.getMessage());
                    }
                }
            
            } catch (SQLException e) {
                e.printStackTrace();
            }

            System.out.println("Populated Tables");
            
            }


            private void dmlQueries(Connection connection) {
                try {
                    Statement statement = connection.createStatement();
    
                    String deletionA =  "DELETE FROM Manager " +
                                        "WHERE Manager_ID = 2";
    
                    try {
                        statement.executeUpdate(deletionA);
                        System.out.println("Manager_ID = 2 has been deleted from the table 'Manager'.\n");
                    } catch (SQLException e) {
                        if (e.getErrorCode() == 1451) {
                            System.out.println("Deletion query A failed: Foreign Key constraint violated.\n");
                        } else {
                            e.printStackTrace();
                        }
                    }
                    
                    String deletionB =  "DELETE FROM Team " +
                                        "WHERE Team_ID = 2";
    
                    try {
                        statement.executeUpdate(deletionB);
                        System.out.println("Team_ID = 2 has been deleted from the table 'Team'.\n");
                    } catch (SQLException e) {
                        if (e.getErrorCode() == 1451) {
                            System.out.println("Deletion query B failed: Foreign Key constraint violated.\n");
                        } else {
                            e.printStackTrace();
                        }
                    }
    
                    String groupByA =   "SELECT Nationality, Count(Nationality) " +
                                                            "FROM Player " +
                                                            "WHERE Team_ID = 1 " +
                                                            "GROUP BY Nationality";
                    
                    try (
                        ResultSet resultSetA = statement.executeQuery(groupByA);
                    ) {
                        System.out.println("Count of Nationalities in Team_ID of 1:");
    
                        while (resultSetA.next()) {
                            String nationality = resultSetA.getString("Nationality");
                            int count = resultSetA.getInt("Count(Nationality)");
                            System.out.println("Nationality: " + nationality + ", Count: " + count);
                        }
    
                        System.out.println("\n");
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
    
                    String groupByB =   "SELECT Position, AVG(Age) AS Average_Age " +
                                        "FROM Player " +
                                        "WHERE Team_ID = 2 " +
                                        "GROUP BY Position";
    
                    try (
                        ResultSet resultSetB = statement.executeQuery(groupByB);
                    ) {
                        System.out.println("Average age of players in each position for Team_ID 2:");
    
                        while (resultSetB.next()) {
                            String position = resultSetB.getString("Position");
                            double averageAge = resultSetB.getDouble("Average_Age");
                            System.out.println("Position: " + position + ", Average Age: " + averageAge);
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
        JoeReader joe = new JoeReader();
        KaweeshaReader kaweesha = new KaweeshaReader();
    }
}
