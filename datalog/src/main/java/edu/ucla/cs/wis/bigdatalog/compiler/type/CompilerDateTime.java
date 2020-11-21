package edu.ucla.cs.wis.bigdatalog.compiler.type;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbDateTime;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.system.DeALSConfiguration;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CompilerDateTime extends CompilerTypeBase implements DbConvertible {
	private static final long serialVersionUID = 1L;

	protected Date value;
	
	public static SimpleDateFormat dateFormat;
	public static SimpleDateFormat timeFormat;
	public static SimpleDateFormat dateTimeFormat;
	
	static {
		DeALSConfiguration deALSConfiguration = new DeALSConfiguration();
		dateFormat = new SimpleDateFormat(deALSConfiguration.getProperty("deals.dateformat"));
		timeFormat = new SimpleDateFormat(deALSConfiguration.getProperty("deals.timeformat"));
		dateTimeFormat = new SimpleDateFormat(dateFormat.toPattern() + " " + timeFormat.toPattern());
	}
	
	public CompilerDateTime(Date value) {
		super(CompilerType.COMPILER_DATETIME);
		this.value = value;
	}

	public Date getValue() {return this.value;}

	public void setValue(Date value) {
		this.value = value;
	}

	public CompilerDateTime copy() {
		return new CompilerDateTime(this.value);
	}

	public String toString() {
		return CompilerDateTime.dateTimeFormat.format(this.value);
	}

	public boolean equals(CompilerTypeBase other)  {
		if (other == null)
			return false;
		
		if (!(other instanceof CompilerDateTime))
			return false;
		
		CompilerDateTime otherDateTime = (CompilerDateTime)other;		
		return (this.value == otherDateTime.getValue());
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		return copy();
	}
	
	@Override
	public boolean isConstant() { return true; }
	
	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		return DbDateTime.create(this.value);
	}
	
	public static CompilerDateTime create(String value) {
		//example format: yyyy-MM-dd hh:mm:ss
		if (value == null || value.trim().length() == 0)
			return null;
		
		if (CompilerDateTime.dateFormat.toPattern().length() == value.length()) {
			try {
				return new CompilerDateTime(CompilerDateTime.dateFormat.parse(value));
			} catch (ParseException e) {
				return null;
				//throw new CompilerException("Value cannot be converted to CompilerDateTime. " + e.getMessage());
			}
		} else if (CompilerDateTime.dateTimeFormat.toPattern().length() == value.length()) {
			try {
				return new CompilerDateTime(CompilerDateTime.dateTimeFormat.parse(value));
			} catch (ParseException e) {
				return null;
				//throw new CompilerException("Value cannot be converted to CompilerDateTime. " + e.getMessage());
			}
		}
		
		return null;
	}

	@Override
	public DataType getDataType() {
		return DataType.DATETIME;
	}
}
