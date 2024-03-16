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
            String username = "Charlie";
            String password = "38700514";

            createDatabase(databaseName, username, password);
        }
    }

    private static class JoeReader {
        public JoeReader() {
            String databaseName = "JoeDB";
            String username = "Joe";
            String password = "38771225";

            createDatabase(databaseName, username, password);
        }
    }

    private static class KaweeshaReader {
        public KaweeshaReader() {
            String databaseName = "KaweeshaDB";
            String username = "Kaweesha";
            String password = "39166236";

            createDatabase(databaseName, username, password);
        }
    }

    public static void createDatabase(String name, String username, String password) {
        String jdbcUrl = "jdbc:mysql://localhost:3306/";

        try (
            Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
            Statement statement = connection.createStatement();
        ) {
            String createDatabaseQuery = "CREATE DATABASE " + name;
            statement.executeUpdate(createDatabaseQuery);
            System.out.println("Database '" + name + "' created successfully.");

            String useDatabaseQuery = "USE " + name;
            statement.executeUpdate(useDatabaseQuery);
            System.out.println("Database '" + name + "' is now in use.");
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