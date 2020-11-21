package edu.ucla.cs.wis.bigdatalog.system;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.api.CommandResult;
import edu.ucla.cs.wis.bigdatalog.api.CommandWrapper;
import edu.ucla.cs.wis.bigdatalog.api.TimerOption;

public class FileRunner {	
	protected PrintStream output;
	
	public FileRunner() {
		this(System.out);
	}
	
	public FileRunner(PrintStream output) {
		this.output = output;
	}
	
	public void run(Path filePath, int logLevel) {
		/* This method reads in a file, loads the declared objects and facts, and execute all queries
		   1) read file contents into string
		   2) remove all query declarations
		   3) load database objects declared in file
		   4) compile
		   5) run all queries, one by one, displaying results
		*/
		DeALSContext deALSContext = new DeALSContext();
		deALSContext.initialize();
		
		deALSContext.setLogLevel(logLevel);
		deALSContext.setTimer(true);

		// 1) read in file contents, but strip out query calls
		String data = null;
		List<String> queries = new ArrayList<>();
		try {
			StringBuilder sb = new StringBuilder();
			List<String> lines = Files.readAllLines(filePath, StandardCharsets.UTF_8);
			for (String line : lines) {
				if (line.startsWith("query ") ) {
					queries.add(line);
				} else {
					sb.append(line);
					sb.append("\n");
				}
			}
			data = sb.toString();
		} catch (IOException ioex) {
			this.output.println(ioex.toString());
			return;
		}
		
		if (data == null) {
			this.output.println("File was empty");
			return;
		}
		
		ReturnStatus status = ReturnStatus.FAIL;
		SystemCommandResult scr = null;
		//2) load db objects
		try {
			scr = deALSContext.loadDatabaseObjects(data);
			if (scr.getStatus() != ReturnStatus.SUCCESS)
				this.output.println(scr.getMessage());
			status = scr.getStatus();
		} catch (Exception ex) {
			this.output.println("Could not load program file '" + filePath.toString() + "'.");
			ex.printStackTrace(this.output);
			status = ReturnStatus.FAIL;
		}
	
		if (status == ReturnStatus.SUCCESS) {
			try {
				scr = deALSContext.compileAllQueryForms();
				if (scr.getStatus() != ReturnStatus.SUCCESS)
					this.output.println(scr.getMessage());	
				status = scr.getStatus();
			} catch (Exception ex) {
				ex.printStackTrace(this.output);
				status = ReturnStatus.FAIL;
			}
			
			if (status == ReturnStatus.SUCCESS) {	
				if (queries.size() > 0) {
					CommandWrapper commandWrapper = new CommandWrapper(logLevel, deALSContext);
					commandWrapper.getOptions().setTimer(TimerOption.QUERY);
					for (int i = 0; (i < queries.size()) && (status == ReturnStatus.SUCCESS); i++) {
						try {
							if (i > 0) this.output.println();
							CommandResult commandResult = commandWrapper.execute(queries.get(i));
							this.output.println(commandResult.toStringShort(true));
							status = commandResult.getReturnStatus();							
						} catch (Exception ex) {
							ex.printStackTrace(this.output);
							status = ReturnStatus.FAIL;
						}
					}
				}
			}
		} else {
			this.output.println("Errors in '" + filePath.toString() + "'. Fix errors and try again.");
		}
		
		deALSContext.reset();
	}

	public static void main(String[] args) {
		//String filePath = "/home/userguy/workspace/DeALS/src/DeALS/src/test/resources/testcases/system/filerunner/countpaths.deal";
		String filePath = args[0];
		int logLevel = DeALSContext.DEFAULT_LOG_LEVEL;
		if (args.length > 1)
			logLevel += Integer.parseInt(args[1]);
		
		new FileRunner().run(Paths.get(filePath), logLevel);
		System.exit(0);
	}
}
