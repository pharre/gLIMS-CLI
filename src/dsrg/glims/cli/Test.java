package dsrg.glims.cli;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.io.FileWriter;

public class Test {
	
	public static void shortenFile() throws IOException {
		Scanner myScanner = new Scanner(new FileReader("collection_All_ANIT_Raw_Merged_10658.txt"));
		FileWriter writer = new FileWriter(new File("short_ANIT.txt"));
		int stopLine = 30;
		int currentLine = 0;
		while (myScanner.hasNextLine() && currentLine < stopLine) {
			String line = myScanner.nextLine();
			writer.write(line + "\n");
			currentLine++;
		}
		writer.close();
	}
	
	public static void readFile(String filename) throws FileNotFoundException {
		Scanner s = new Scanner(new FileReader(filename));
		while (s.hasNextLine()) {
			System.out.println(s.nextLine());
		}
	}
	
	public static void main(String[] args) throws IOException {
		shortenFile();
		readFile("short_ANIT.txt");
	}
}
