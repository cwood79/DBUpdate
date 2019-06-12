import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.*;
import java.util.Scanner;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;


public class DBUpdate {

    public static final String FILEPATTERN=".csv";
    public static Connection conn;

    public int recsReceive=0;
    public int recsSuccess=0;
    public int recsFail=0;

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {

        Scanner fileScanner = new Scanner(System.in);
        System.out.println("----DB Updater----");

        System.out.println("Enter the name of the csv file you want to convert");

        String fileName = fileScanner.nextLine();

        while(!fileName.endsWith(FILEPATTERN))
        {
            System.out.println("Invalid format for csv file name");
            System.out.println("Enter the name of the csv you want to convert");

            fileName = fileScanner.nextLine();
        }
        // parse csvs + add to database
        parseCSV(fileName);

    }

    public static void parseCSV(String fileName) throws ClassNotFoundException, SQLException, IOException {

        // Initialize JDBC database in memory
        Class.forName("org.sqlite.JDBC");

        conn=DriverManager.getConnection("jdbc:sqlite::memory:");

        Statement stmt=conn.createStatement();
        ResultSet rs;

        stmt.executeUpdate("DROP TABLE IF EXISTS db");

        stmt.executeUpdate("CREATE TABLE db " +
                "(A VARCHAR(100) NOT NULL, " +
                "B VARCHAR(100) NOT NULL, " +
                "C VARCHAR(100) NOT NULL, " +
                "D BOOLEAN NOT NULL, " +
                "E VARCHAR(100) NOT NULL, " +
                "F VARCHAR(100) NOT NULL, " +
                "G MONEY NOT NULL, " +
                "H BOOLEAN NOT NULL, " +
                "I BOOLEAN NOT NULL, " +
                "J VARCHAR(100) NOT NULL)");


        Reader in = new FileReader(fileName);
        Iterable<CSVRecord> records = CSVFormat.EXCEL.withFirstRecordAsHeader().withNullString("").parse(in);
        for (CSVRecord record: records)
        {
            String A = record.get("A");
            String B = record.get("B");
            System.out.println(A+" "+B);
        }

        // print stats
        printStats();

        stmt.close();
        conn.close();
    }

    // print stats
    public static void printStats()
    {

    }

    // utility method to print database for testing
    public static void printDatabase()
    {

    }

}
