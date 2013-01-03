package dsrg.glims.cli;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;

public class RowColumnTransposer {

	public void transpose(File infile, File outfile, String oldSplitSymbol,
			String newSplitSymbol) {

		FileInputStream fStream;

		try {
			fStream = new FileInputStream(infile);
			DataInputStream in = new DataInputStream(fStream);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));

			int numLines = 0;
			int longestLineLen = 0;
			String line;
			ArrayList<String[]> lines = new ArrayList<String[]>();
			while ((line = reader.readLine()) != null) {
				String[] splitLine = line.split(oldSplitSymbol);
				lines.add(splitLine);
				numLines++;
				if (splitLine.length > longestLineLen) {
					longestLineLen = splitLine.length;
				}
			}
			
			//System.out.println("lines.size() " + lines.size());
			//System.out.println("longestLine " + longestLineLen);
			
			for (int i=0; i<lines.size(); i++) {
				if (lines.get(i).length != longestLineLen) {
					String[] replacement = Arrays.copyOf(lines.get(i), longestLineLen);
					for (int j=0; j<replacement.length; j++)
						if (replacement[j]==null)
							replacement[j] = "";
					lines.set(i, replacement);
				}
			}
			
			try {
				Writer out = new OutputStreamWriter(new FileOutputStream(outfile));

				// writes the transposed file
				boolean firstLine = true;
				for (int i = 0; i < longestLineLen; i++) {
					if (firstLine) firstLine = false;
					else out.write("\n");
					boolean firstColumn = true;
					for (int j = 0; j < numLines; j++) {
						if (firstColumn) firstColumn = false;
						else out.write(newSplitSymbol);
						if (j < lines.size() && i < lines.get(j).length)
							out.write(lines.get(j)[i]);
						else {
							System.out.println("index out of bounds problem");
						}
					}
				}
				out.close();
			} catch (Exception e) {
				System.out.println("catch");
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			System.err.println("Error file not found: " + e.getMessage());
		} catch (IOException e) {
			System.err.println("IO exception: " + e.getMessage());
		}
	}

	public String makeTransposeName(File file) {
		String absPath = file.getAbsolutePath();
		int end = absPath.indexOf(".txt");
		String outFileName = absPath.substring(0, end) + "_transposed.txt";
		return outFileName;
	}
	
//	public static void main(String[] args) {
//		File file = new File("C:\\Users\\pharre\\Desktop\\rows2.txt");
//		File outFile = new File("C:\\Users\\pharre\\Desktop\\test.txt_transposed.txt");
//		RowColumnTransposer rct = new RowColumnTransposer();
//		rct.transpose(file, outFile, "\t", "\t");
//	}
}
