import java.sql.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;

public class Code {
    private static class CharlieReader {
        public CharlieReader() {
            String databaseName = "CharlieDB";
            Connection connection = createConnection();

            createDatabase(connection, databaseName);
            createTables(connection);
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
    }

    private static class JoeReader {
        public JoeReader() {
            String databaseName = "JoeDB";
            Connection connection = createConnection();

            createDatabase(connection, databaseName);
            createTables(connection);
        }

        private void createTables(Connection connection) {
            try {
                Statement statement = connection.createStatement();

                String createManagerTable = "CREATE TABLE IF NOT EXISTS Manager ( " +
                                            "Manager_ID INTEGER NOT NULL, " +
                                            "First_Name TINYTEXT NOT NULL, " +
                                            "Last_Name TINYTEXT, " +
                                            "Age INTEGER, " +
                                            "PRIMARY KEY (Manager_ID) );";
                statement.executeUpdate(createManagerTable);

                String createTeamTable =    "CREATE TABLE IF NOT EXISTS Team ( " +
                                            "Team_ID INTEGER NOT NULL, " +
                                            "Team_Name TINYTEXT NOT NULL, " +
                                            "Team_Abbreviation VARCHAR(5), " +
                                            "Manager_ID INTEGER NOT NULL, " +
                                            "Year_Founded INTEGER, " +
                                            "PRIMARY KEY (Team_ID), " +
                                            "FOREIGN KEY (Manager_ID) REFERENCES Manager(Manager_ID) );";
                statement.executeUpdate(createTeamTable);

                String createPlayerTable =  "CREATE TABLE IF NOT EXISTS Player ( " +
                                            "Player_ID INTEGER NOT NULL, " +
                                            "First_Name TINYTEXT NOT NULL, " +
                                            "Last_Name TINYTEXT, " +
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
    }

    private static class KaweeshaReader {
        public KaweeshaReader() {
            String databaseName = "KaweeshaDB";
            Connection connection = createConnection();

            createDatabase(connection, databaseName);
        }

        private void createTables() {

        }
    }

    public static Connection createConnection() {
        String jdbcUrl = "jdbc:mysql://localhost:3306/";
        Connection connection = null;

        try {
            connection = DriverManager.getConnection(jdbcUrl);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return connection;
    }

    public static void createDatabase(Connection connection, String databaseName) {
        try {
            Statement statement = connection.createStatement();
            String createDatabase = "CREATE DATABASE IF NOT EXISTS " + databaseName;
            statement.executeUpdate(createDatabase);
            System.out.println("Database '" + databaseName + "' created successfully.");

            String useDatabase = "USE " + databaseName;
            statement.executeUpdate(useDatabase);
            System.out.println("Database '" + databaseName + "' is now in use.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        CharlieReader charlie = new CharlieReader();
        JoeReader joe = new JoeReader();
        KaweeshaReader kaweesha = new KaweeshaReader();
    }
}