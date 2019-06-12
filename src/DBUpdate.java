import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Scanner;
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

        // Initialize sqlite database in memory
        Class.forName("org.sqlite.JDBC");

        conn=DriverManager.getConnection("jdbc:sqlite::memory:");

        Statement stmt=conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS db");

        stmt.executeUpdate("CREATE TABLE db " +
                "(A VARCHAR(100) NOT NULL, " +
                "B VARCHAR(100) NOT NULL, " +
                "C VARCHAR(100) NOT NULL, " +
                "D BOOLEAN NOT NULL, " +
                "E VARCHAR(1500) NOT NULL, " +
                "F VARCHAR(100) NOT NULL, " +
                "G FLOAT NOT NULL, " +
                "H BOOLEAN NOT NULL, " +
                "I BOOLEAN NOT NULL, " +
                "J VARCHAR(100) NOT NULL)");

        int recsReceive=0;
        int recsSuccess=0;
        int recsFail=0;

        SimpleDateFormat sdf = new SimpleDateFormat(TSFORMAT);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

        String timeString = sdf.format(timestamp);

        String badFileName="bad-data-"+timeString+".csv";
        String logFileName="dbstats-"+timeString+".txt";

        FileWriter fw = new FileWriter(badFileName);

        // Iterate through records in csv
        Reader in = new FileReader(fileName);
        Iterable<CSVRecord> records = CSVFormat.EXCEL.withFirstRecordAsHeader()
                                                .withIgnoreSurroundingSpaces()
                                                .withNullString("")
                                                .withIgnoreEmptyLines().parse(in);
        for (CSVRecord record: records)
        {
            recsReceive++;

            // checking if number of columns in csv matches + checking for null columns
            if(!record.isConsistent() || !checkRecord(record))
            {
                recsFail++;
                fw.write(record.toString()+"\r\n");
            }
            else
            {
                recsSuccess++;
                insertIntoDB(record);
            }

        }

        fw.close();

        // log stats
        logStats(logFileName,recsReceive,recsSuccess,recsFail);

        selectAll();

        stmt.close();
        conn.close();

        System.out.println("Database operation complete");
    }

    public static boolean checkRecord(CSVRecord record)
    {
        for(int i=0; i<record.size();i++)
        {
            if(record.get(i)==null)
            {
                return false;
            }
        }

        return true;
    }

    public static void insertIntoDB(CSVRecord record) throws SQLException
    {
        String sql = "INSERT INTO db VALUES(?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement stmt = conn.prepareStatement(sql);

        stmt.setString(1,record.get("A"));
        stmt.setString(2,record.get("B"));
        stmt.setString(3,record.get("C"));
        stmt.setBoolean(4,record.get("D").equals("Male"));
        stmt.setString(5,record.get("E"));
        stmt.setString(6,record.get("F"));
        stmt.setFloat(7, 1.0f); // record.get(6) TODO: fix money
        stmt.setBoolean(8,Boolean.parseBoolean(record.get("H")));
        stmt.setBoolean(9,Boolean.parseBoolean(record.get("I")));
        stmt.setString(10,record.get("J"));

        stmt.executeUpdate();
    }

    public static void logStats(String logFileName,int recsReceive, int recsSuccess, int recsFail) throws IOException
    {
        FileWriter fw = new FileWriter(logFileName);
        fw.write("----Database Statisticss----\r\n");
        fw.write(recsReceive + " records received\r\n");
        fw.write(recsSuccess + " records successful\r\n");
        fw.write(recsFail + " records failed\r\n");
        fw.close();
    }

    // utility method to print database for testing
    public static void selectAll()
    {
        String sql = "SELECT * FROM db";

        try (Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){

            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getString("A") +  "\t" +
                                    rs.getString("B") + "\t" +
                        rs.getString("C") + "\t" +
                        rs.getBoolean("D") + "\t" +
                        rs.getString("E") + "\t" +
                        rs.getString("F") + "\t" +
                        rs.getFloat("G") + "\t" +
                        rs.getBoolean("H") + "\t" +
                        rs.getBoolean("I") + "\t" +
                        rs.getString("J"));

            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}
