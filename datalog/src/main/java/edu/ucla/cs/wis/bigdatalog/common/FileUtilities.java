package edu.ucla.cs.wis.bigdatalog.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class FileUtilities {

	public static String getFileContents(Path filePath) throws IOException {
		StringBuilder data = new StringBuilder();
	
		List<String> lines = Files.readAllLines(filePath, StandardCharsets.UTF_8);
				
		for (String line : lines) {
			data.append(line);
			data.append("\n");
		}
		
		return data.toString();
	}
	
	public static String getFileContents(InputStream inputStream) {
		BufferedReader reader = null;
		InputStreamReader isr = null;
		StringBuilder data = new StringBuilder();
		try {
			isr = new InputStreamReader(inputStream); 
			reader = new BufferedReader(isr);

			String line;
			while ((line = reader.readLine()) != null) {
				data.append(line);
				data.append("\n");
			}
			
		} catch (Exception ex) {
		} finally {
			try { if (isr != null) isr.close();} catch (Exception ex){}
			try { if (reader != null) reader.close();} catch (Exception ex){}
		}
		return data.toString();
	}
}
