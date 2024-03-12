// This is our shared main file

import java.sql.*; 
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;

public class CharliesReader{

    public CharliesReader(){
        Connection connection = CreateConnection();
        CreateDatabase(connection);
        CreateTables(connection);
        ReadCSV();
        PopulateTables();
        RunQueries();
    }

	public String databaseName = "CharliesDB";

    public Connection CreateConnection(){
        String jdbcUrl = "jdbc:mysql://localhost:3306/";
        Connection connection = null;
		try
		{ 
			connection = DriverManager.getConnection(jdbcUrl);
		}
		catch (SQLException e) 
		{
		    e.printStackTrace();
		}		
		return connection;
    }

    public void CreateDatabase(Connection connection){
        try{
		    Statement statement = connection.createStatement();
		    String createDatabase = "CREATE DATABASE IF NOT EXISTS " + databaseName;
		    statement.executeUpdate(createDatabase);
        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }

    public void CreateTables(Connection connection){
        try {
            Statement statement = connection.createStatement();

            // Use the database:
            String UseDatabase = "use " + databaseName;
            statement.executeUpdate(UseDatabase);

            // Now create tables:
            String createManagerTable = "CREATE TABLE IF NOT EXISTS Manager(Manager_ID INTEGER, First_Name VARCHAR(50), Last_Name VARCHAR(50), Age INTEGER, PRIMARY KEY (Manager_ID))";
            statement.executeUpdate(createManagerTable);
            String createTeamTable = "CREATE TABLE IF NOT EXISTS Team(Team_ID INTEGER, Team_Name VARCHAR(100), Team_Abbreviation VARCHAR(5), Year_Founded INTEGER, Manager_ID INTEGER NOT NULL, PRIMARY KEY(Team_ID)), FOREIGN KEY(Manager_ID) REFERENCES Manager(Manager_ID))";
		    statement.executeUpdate(createTeamTable);
            String createPlayerTable = "CREATE TABLE IF NOT EXISTS Player(Player_ID INTEGER, First_Name VARCHAR(50), Last_Name VARCHAR(50), Shirt_Number INTEGER, Age INTEGER, Team_ID INTEGER, PRIMARY KEY(Player_ID), FOREIGN KEY(Team_ID) REFERENCES Team(Team_ID))";
            statement.executeUpdate(createPlayerTable);
        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }

    public void ReadCSV(){

    }

    public void PopulateTables(){

    }

    public void RunQueries(){

    }
}

public class JoesReader{

}

public class KaweeshasReader{

}

public class Main{
    public static void main(String[] args){
        CharliesReader charlie = new CharliesReader();
        JoesReader joe;
        KaweeshasReader kaweesha;
    }
}
