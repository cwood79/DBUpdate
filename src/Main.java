import java.util.Scanner;

public class Main {

    public static final String CSVPATTERN=".csv";

    public static void main(String[] args) {

        Scanner fileScanner = new Scanner(System.in);
        System.out.println("----DB Updater----");

        System.out.println("Enter the name of the csv file you want to convert");

        String fileName = fileScanner.nextLine();

        while(!fileName.endsWith(CSVPATTERN))
        {
            System.out.println("Invalid format for csv file name");
            System.out.println("Enter the name of the csv you want to convert");

            fileName = fileScanner.nextLine();
        }


    }
}
