import java.io.*;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Scanner;
import java.util.TimeZone;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;


public class DBUpdate {

    public static final String FILEPATTERN = ".csv";
    public static final String TSFORMAT = "yyyy.MM.dd.HH.mm.ss";
    public static Connection conn;

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
/*
        stmt.executeUpdate("CREATE TABLE db " +
                "(A VARCHAR(100) NOT NULL, " +
                "B VARCHAR(100) NOT NULL, " +
                "C VARCHAR(100) NOT NULL, " +
                "D VARCHAR(100) NOT NULL, " +
                "E VARCHAR(1500) NOT NULL, " +
                "F VARCHAR(100) NOT NULL, " +
                "G MONEY NOT NULL, " +
                "H BOOLEAN NOT NULL, " +
                "I BOOLEAN NOT NULL, " +
                "J VARCHAR(100) NOT NULL)");
*/
        stmt.executeUpdate("CREATE TABLE db " +
                "(A VARCHAR(100), " +
                "B VARCHAR(100), " +
                "C VARCHAR(100), " +
                "D VARCHAR(100), " +
                "E VARCHAR(1500), " +
                "F VARCHAR(100), " +
                "G MONEY, " +
                "H BOOLEAN, " +
                "I BOOLEAN, " +
                "J VARCHAR(100))");

        int recsReceive=0;
        int recsSuccess=0;
        int recsFail=0;

        String badFileName="bad-data-"+getTimeStamp()+".csv";
        String logFileName="dbstats-" + getTimeStamp()+".txt";

        FileWriter fw = new FileWriter(badFileName);

        Reader in = new FileReader(fileName);
        Iterable<CSVRecord> records = CSVFormat.EXCEL.withFirstRecordAsHeader()
                                               .withNullString("")
                                               .withIgnoreEmptyLines().parse(in);
        for (CSVRecord record: records)
        {
            recsReceive++;

            if(!record.isConsistent())
            {
                recsFail++;
                fw.write(record.toString()+"\r\n");
            }
            else
            {
                System.out.println(record.get("A"));
                recsSuccess++;
                insertIntoDB(record);
            }

        }

        fw.close();

        // log stats
        logStats(logFileName,recsReceive,recsSuccess,recsFail);

        stmt.close();
        conn.close();
    }

    public static void insertIntoDB(CSVRecord record) throws SQLException {
        String sql = "INSERT INTO db VALUES(?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement stmt = conn.prepareStatement(sql);

        stmt.setString(1,record.get(0));
        stmt.setString(2,record.get(1));
        stmt.setString(3,record.get(2));
        stmt.setString(4,record.get(3)); // TODO: fix gender
        stmt.setString(5,record.get(4));
        stmt.setString(6,record.get(5));
        stmt.setFloat(7, 1.0f);//Float.parseFloat(record.get(6))); // TODO: fix money
        stmt.setBoolean(8,Boolean.parseBoolean(record.get(7)));
        stmt.setBoolean(9,Boolean.parseBoolean(record.get(8)));
        stmt.setString(10,record.get(9));

        stmt.executeUpdate();
    }

    public static void logStats(String logFileName,int recsReceive, int recsSuccess, int recsFail) throws IOException {

        FileWriter fw = new FileWriter(logFileName);
        fw.write("----Database Statisticss----\r\n");
        fw.write(recsReceive + " records received\r\n");
        fw.write(recsSuccess + " records successful\r\n");
        fw.write(recsFail + " records failed\r\n");
        fw.close();
    }

    public static String getTimeStamp()
    {
        SimpleDateFormat sdf = new SimpleDateFormat(TSFORMAT);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

        return sdf.format(timestamp);
    }

    // utility method to print database for testing
    public static void printDatabase()
    {


    }
}
