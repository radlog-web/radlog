package edu.ucla.cs.wis.bigdatalog.api;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

public class DirectoryVisitor implements FileVisitor<Path> {

	StringBuilder json; 
			
	public DirectoryVisitor() {
		this.json = new StringBuilder();
	}
	
	public String getJson() { return this.json.toString(); }
	
	@Override
	public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
		if (json.length() > 0 && (json.charAt(json.length() - 1) != '['))
			json.append(",");
		json.append("{\"name\":");
		json.append("\"");
		json.append(dir.getFileName());
		json.append("\", \"files\":[");
		return FileVisitResult.CONTINUE;
	}

	@Override
	public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
		if (file.toString().endsWith(".deal") || file.toString().endsWith(".fac")) {
			if (json.length() > 0 && (json.charAt(json.length() - 1) != '['))
				json.append(",");
			
			json.append("{\"name\":\"");
			json.append(file.getFileName());
			json.append("\"}");
		}
		return FileVisitResult.CONTINUE;
	}

	@Override
	public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
		return null;
	}

	@Override
	public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
		json.append("]}");
		return FileVisitResult.CONTINUE;
	}
}
