package edu.ucla.cs.wis.bigdatalog.database;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;

public class Tuple implements MemorySize, Serializable {
	private static final long serialVersionUID = 1L;
	
	public 	DbTypeBase[]	columns;
	
	public Tuple() {}
	
	public Tuple(int arity) {
		this.columns = new DbTypeBase[arity];
	}
	
	public Tuple(DbTypeBase[] columnValues) {		
		this.columns = new DbTypeBase[columnValues.length];
		for (int i = 0; i < this.columns.length; i++)
			this.columns[i] = columnValues[i];
	}
	
	public int getArity() { return this.columns.length; }
	
	public DbTypeBase getColumn(int index) {
		return this.columns[index];
	}
	
	public void setColumn(int index, DbTypeBase dbTypeObject) {
		this.columns[index] = dbTypeObject;
	}
	
	public boolean equals(Tuple otherTuple) {
		if (otherTuple == null)
			return false;
		
		if (this == otherTuple)
			return true;
		
		if (this.columns.length != otherTuple.columns.length)
			return false;
		
		for (int i = 0; i < this.columns.length; i++) {
			if (!this.columns[i].equals(otherTuple.columns[i]))
				return false;
		}
		
		return true;
	}
	
	public boolean equals(DbTypeBase[] otherTuplesColumns, int numberOfColumns) {
		for (int i = 0; i < numberOfColumns; i++)
			if (!this.columns[i].equals(otherTuplesColumns[i]))
				return false;
		
		return true;
	}
	
	public boolean isEmpty() {
		boolean retval = true;
		for (int i = 0; i < this.columns.length; i++) {
			if (this.columns[i] != null) {
				retval = false;
				break;
			}
		}
		return retval;		
	}
	/*
	// efficiently type check the tuple
	public boolean matchesSchema(DataType[] schema) {
		if (this.columns.length != schema.length) 
			return false;

		for (int i = 0; i < this.columns.length; i++) {
			if ((this.columns[i].getDataType() != schema[i])) {
				if (schema[i] == DataType.UNKNOWN)
					continue;
				
				if ((schema[i] == DataType.ANY) && (this.columns[i].getDataType() == DataType.COMPLEX))
					continue;
		
				if ((schema[i] == DataType.FLOAT) && (this.columns[i] instanceof DbInteger)) {
					// promote dbInteger column to dbFloat
					this.columns[i] = DbFloat.create(((DbInteger)this.columns[i]).getValue());
					continue;
				}
					
				return false;
			}
		}
		return true;
	}*/
	
	public Tuple copy() {
		Tuple copy = new Tuple(this.columns.length);
		for (int i = 0; i < this.columns.length; i++)
			copy.columns[i] = this.columns[i].copy();

		return copy;
	}
	
	public static Tuple copy(Tuple tuple) {
		Tuple copy = new Tuple(tuple.columns.length);
		for (int i = 0; i < tuple.columns.length; i++)
			copy.columns[i] = tuple.columns[i].copy();
		return copy;
	}
	
	public Tuple project(int[] ordinals) {
		Tuple tuple = new Tuple(ordinals.length);
		for (int i = 0; i < ordinals.length; i++)
			tuple.setColumn(i, this.columns[ordinals[i]]);
		return tuple;
	}
	
	public String toString() {
		StringBuilder output = new StringBuilder();
		if (this.columns != null) {
			output.append("(");
			for (int i = 0; i < this.columns.length; i++) {
				if (i != 0)
					output.append(", ");
				// this is weak, but just in case
				if ((this.columns[i] == null) || (!this.columns[i].isValid())) {
					output.append("NULL");
				} else {
					output.append(this.columns[i].toString());
				}
			}
				
			output.append(")."/* + "[" + this.address + "]"*/);	
		}
			
		return output.toString();
	}
	
	public String toJson() {
		StringBuilder output = new StringBuilder();
		if (this.columns != null) {
			output.append("[");
			for (int i = 0; i < this.columns.length; i++) {
				if (i > 0)
					output.append(",");

				output.append("\"");
				if (this.columns[i] != null) {
					output.append(this.columns[i].toString());
				} else {
					output.append("NULL");
				}
				output.append("\"");
			}

			output.append("]");
		}		
	
		return output.toString();
	}
	
	public MemoryMeasurement getSizeOf() {
		MemoryMeasurement mm; 
		int used = 0;
		int allocated = 0;
		for (int i = 0; i < this.columns.length; i++) {
			mm = this.columns[i].getSizeOf();
			used += mm.getUsed();
			allocated += mm.getAllocated();
		}
		return new MemoryMeasurement(used, allocated);
	}
	
	public void setValues(Tuple source) {
		//DataType sourceDataType, destinationDataType;
		for (int i = 0; i < source.columns.length; i++)
			this.columns[i] = source.columns[i];/*{
			sourceDataType = source.columns[i].getDataType();
			destinationDataType = this.columns[i].getDataType();
			if (sourceDataType == destinationDataType) {
				this.columns[i].setValue(source.columns[i]);
			} else {
				if (destinationDataType.getOrder() > sourceDataType.getOrder())
					this.columns[i].setValue(source.columns[i].convertTo(destinationDataType));
				else
					throw new DeALSException("Unable to cast to load tuple.");
			}
		}*/
	}
}
