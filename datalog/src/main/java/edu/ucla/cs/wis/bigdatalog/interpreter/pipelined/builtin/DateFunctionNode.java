package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.type.DbDateTime;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbString;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;

public class DateFunctionNode 
	extends OrNode {

	enum DateFunctionType {
		GETDATE(1), // binding: f - sets 0 to DbDateTime 
		DATEPART(3), // binding: bbf - sets 2 to DbInteger
		DATEADD(4), // binding: bbbf - sets 3 to DbDateTime
		DATEDIFF(4); // binding: bbbf - sets 3 to DbInteger
		
		private int numberOfArguments;
		private DateFunctionType(int numberOfArguments) {
			this.numberOfArguments = numberOfArguments;
		}
		
		public static DateFunctionType getDateFunctionType(String functionName, int numberOfArguments) {
			for (DateFunctionType dft : DateFunctionType.values()) {
				if (dft.name().toLowerCase().equals(functionName) && dft.numberOfArguments == numberOfArguments)
					return dft;
			}
			return null;
		}
	}
	
	enum DatePart {
		year, month, dayofyear, day, week, weekday, hour, minute, second, millisecond;
		
		public static DatePart getDatePart(String datePartStr) {
			for (DatePart dp : DatePart.values())
				if (dp.name().equals(datePartStr))
					return dp;
			return null;
		}
	}
	
	protected DateFunctionType dateFunctionType;
	protected Calendar cal;

	public DateFunctionNode(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables) {
		super(predicateName, args, binding, freeVariables);
				
		this.dateFunctionType = DateFunctionType.getDateFunctionType(predicateName, args.size());		
		if (this.dateFunctionType == null)
			throw new InterpreterException("No valid date function can be found for " + predicateName + "|" + args.size());
		
		this.cal = Calendar.getInstance();
	}

	public boolean initialize() { return true; }
	
	public Status getTuple() {
		Status status;
	  
		this.traceGetTupleEntry();

		if (this.isEntry) {
			switch (this.dateFunctionType) {
				case GETDATE: {
					((Variable)this.arguments.get(0)).setValue(DbDateTime.create(new Date()));
					break;
				}
				
				case DATEPART: {
					DbString dbDatePart = (DbString)this.getArgumentAsDbType(0);
					DatePart datePart = DatePart.getDatePart(dbDatePart.getValue().toLowerCase());
					DbDateTime dbDateTime = (DbDateTime) this.getArgumentAsDbType(1);
					Date dateTime = dbDateTime.getValue();
					int partValue = 0;
					this.cal.setTime(dateTime);
					switch (datePart) {
						case year:
							partValue = this.cal.get(Calendar.YEAR);
							break;
						case month:
							partValue = this.cal.get(Calendar.MONTH) + 1;
							break;
						case day:
							partValue = this.cal.get(Calendar.DAY_OF_MONTH);
							break;
						case dayofyear:
							partValue = this.cal.get(Calendar.DAY_OF_YEAR);
							break;
						case week:
							partValue = this.cal.get(Calendar.WEEK_OF_YEAR);
							break;
						case weekday:
							partValue = this.cal.get(Calendar.DAY_OF_WEEK);
							break;
						case hour:
							partValue = this.cal.get(Calendar.HOUR_OF_DAY);
							break;
						case minute:
							partValue = this.cal.get(Calendar.MINUTE);
							break;
						case second:
							partValue = this.cal.get(Calendar.SECOND);
							break;
						case millisecond:
							partValue = this.cal.get(Calendar.MILLISECOND);
							break;
					}
					
					((Variable)this.arguments.get(2)).setValue(DbInteger.create(partValue));
					break;
				}
				
				case DATEADD: {
					DbString dbDatePart = (DbString)this.getArgumentAsDbType(0);
					DatePart datePart = DatePart.getDatePart(dbDatePart.getValue().toLowerCase());

					DbInteger dbNumber = (DbInteger) this.getArgumentAsDbType(1);
					DbDateTime dbDateTime = (DbDateTime) this.getArgumentAsDbType(2);
					
					this.cal.setTime(dbDateTime.getValue());
					switch (datePart) {
						case year:
							this.cal.add(Calendar.YEAR, dbNumber.getValue());
							break;
						case month:
							this.cal.add(Calendar.MONTH, dbNumber.getValue());
							break;
						case day:
							this.cal.add(Calendar.DAY_OF_MONTH, dbNumber.getValue());
							break;
						case dayofyear:
							this.cal.add(Calendar.DAY_OF_YEAR, dbNumber.getValue());
							break;
						case week:
							this.cal.add(Calendar.WEEK_OF_YEAR, dbNumber.getValue());
							break;
						case weekday:
							this.cal.add(Calendar.DAY_OF_WEEK, dbNumber.getValue());
							break;
						case hour:
							this.cal.add(Calendar.HOUR_OF_DAY, dbNumber.getValue());
							break;
						case minute:
							this.cal.add(Calendar.MINUTE, dbNumber.getValue());
							break;
						case second:
							this.cal.add(Calendar.SECOND, dbNumber.getValue());
							break;
						case millisecond:
							this.cal.add(Calendar.MILLISECOND, dbNumber.getValue());
							break;
					}
					
					((Variable)this.arguments.get(3)).setValue(DbDateTime.create(this.cal.getTime()));
					break;
				}
				
				case DATEDIFF: {
					DbString dbDatePart = (DbString)this.getArgumentAsDbType(0);
					DatePart datePart = DatePart.getDatePart(dbDatePart.getValue().toLowerCase());

					DbDateTime dbDateTime1 = (DbDateTime) this.getArgumentAsDbType(1);
					DbDateTime dbDateTime2 = (DbDateTime) this.getArgumentAsDbType(2);
					
					long date1 = dbDateTime1.getValue().getTime();
					long date2 = dbDateTime2.getValue().getTime();
					long diff = date1 - date2;
					long val = 0;
					switch (datePart) {
					case year:
						val = (long) (TimeUnit.MILLISECONDS.toDays(diff) / 365.25);
						break;
					case month:
						val = TimeUnit.MILLISECONDS.toDays(diff) / 30;
						break;
					case day:
						val = TimeUnit.MILLISECONDS.toDays(diff);
						break;
					case hour:
						val = TimeUnit.MILLISECONDS.toHours(diff);
						break;
					case minute:
						val = TimeUnit.MILLISECONDS.toMinutes(diff);
						break;
					case second:
						val = TimeUnit.MILLISECONDS.toSeconds(diff);
						break;
					case millisecond:
						val = diff;
						break;
				}
					
					((Variable)this.arguments.get(3)).setValue(DbInteger.create((int) val));
					break;
				}
			}
						
			status = Status.SUCCESS;
			this.isEntry = false;
	    } else {
	    	this.isEntry = true;
	    	status = Status.ENTRY_FAIL;
	    }

		this.traceGetTupleExit(status);

		return status;
	}

	public void deleteRelationsAndCursors() {  }

	public void cleanUp() {
		this.freeVariableList.makeFree();
		this.baseNodeCleanUp();
	}

	public void partialCleanUp() {
		this.cleanUp();
	}

	public String toString() {
		return toStringNode();
	}
}