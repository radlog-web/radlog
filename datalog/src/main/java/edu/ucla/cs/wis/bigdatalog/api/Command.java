package edu.ucla.cs.wis.bigdatalog.api;

import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.exception.APIException;

public class Command {

	protected CommandType commandType;
	protected String originalCommandText;
	protected List<String> commandArguments;
	protected String originalCommandTextWithCommand;
	
	private Command(CommandType commandType, String originalCommandText, List<String> commandArguments) {
		this.commandType = commandType;
		this.originalCommandText = originalCommandText;
		this.commandArguments = commandArguments;
		
		StringBuilder args = new StringBuilder();
		for (String argument : commandArguments) {
			args.append(argument);
			args.append(" ");
		}
		this.originalCommandTextWithCommand = args.toString().trim();
	}
	
	public CommandType getCommandType() { return this.commandType; }
	
	public List<String> getCommandArguments() { return this.commandArguments; }

	public String getOriginalCommandTextWithCommand() { return this.originalCommandTextWithCommand; }
	
	public static Command parseCommand(String commandText) {
		if (commandText == null || commandText.length() == 0)
			throw new APIException("No command given.");
		
		List<String> commandTextTokens = Command.tokenize(commandText);
		if (commandTextTokens == null || commandTextTokens.size() == 0)
			throw new APIException("Command text '" + commandText + "' could not be parsed.");
		
		CommandType commandType = CommandType.fromName(commandTextTokens.get(0));
		
		if (commandType == null)
			throw new APIException("Unrecognized command '" + commandText + "'");
		
		// remove the first since it told us the commandType
		commandTextTokens.remove(0);
		
		if (!commandType.isValidNumberOfArgs(commandTextTokens.size()))
			throw new APIException("Invalid number of arguments for command '" + commandType.getName() + "' with '" + commandText + "'.");
		
		return new Command(commandType, commandText, commandTextTokens);
	}
	
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("CommandType:" + this.commandType + ", Arguments: " + this.commandArguments.toString());
		return retval.toString();
	}
	
	public static List<String> tokenize(String buffer) {
		final char		NL			=	'\n';
		final char		CR			=	'\r';
		
		char			ch; 
		int 			parensCount	= 0;
		boolean 		inQuote		= false;
		boolean 		inEscape	= false;
		boolean 		checkToken	= true;
		List<String> 	argumentList 	= new ArrayList<>();
		
		if ((buffer == null) || (buffer.length() == 0))
			return argumentList;
		
		buffer = buffer.trim();
		int bufferLength = buffer.length();		
		StringBuilder token = new StringBuilder();
		
		// cursor - 1 because we've already gotten the char at position 'cursor' when we reach this outer while loop guard, 
		// so it shouldn't count against us, otherwise a single char last token won't get added
		for (int cursor = 0; cursor < bufferLength; cursor++) {		
			// we have at least 1 char, so get it
			ch = buffer.charAt(cursor);
			
			if ((ch == '\0') || (ch == NL) || (ch == CR))
				break;
			
			while ((ch == ' ') || (ch == '\t')) {
				cursor++;
				ch = buffer.charAt(cursor);
			}
			
			token = new StringBuilder();			
			ch = buffer.charAt(cursor);		
	
		  	while (checkToken) {
		  		if (ch == '\\') {
		  			token.append(ch);
		  			inEscape = true;
		  		}

		  		switch (ch)
		  		{
		  		case '\'':
		  		{
		  			// Escape for single quote only
		  			if (inEscape || inQuote)
		  				inEscape = false;
		  			else
		  				inQuote = true;

		  			token.append(ch);
		  		}
		  		break;

		  		case '\0':
		  		case NL:
		  		case CR:
		  			checkToken = false;
		  			break;

		  		case ' ':
		  		case '\t':
		  		{
		  			if ((parensCount == 0) && !inQuote)
		  				checkToken = false;
		  			else
		  				token.append(ch);
		  		}
		  		break;

		  		case '(':
		  		{
		  			parensCount++;
		  			token.append(ch);
		  		}
		  		break;

		  		case ')':
		  		{
		  			parensCount--;
		  			token.append(ch);
		  		}
		  		break;

		  		default:
		  			token.append(ch);
		  			break;
		  		}
		  		
		  		if (checkToken) {
		  			if (cursor < (bufferLength - 1)) {
		  				cursor++;
		  				ch = buffer.charAt(cursor);
		  			} else {
		  				checkToken = false;
		  			}
		  		}

		  		inEscape = false;
		  	}
			
			if (token.length() > 0) {
				argumentList.add(token.toString());
				checkToken = true;
			}
		}

		return argumentList;
	}
}
