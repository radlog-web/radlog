package edu.ucla.cs.wis.bigdatalog.compiler.predicate;

import java.io.Serializable;

import com.google.gson.Gson;

import edu.ucla.cs.wis.bigdatalog.compiler.CompilerException;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class BasePredicateStructuralAttribute extends CompilerTypeBase implements Serializable {
	private static final long serialVersionUID = 1L;
	protected DataType dataType;
	protected String columnName;	
	protected BasePredicateStructuralAttribute[] subAttributes;
	
	public BasePredicateStructuralAttribute(DataType dataType, String columnName, BasePredicateStructuralAttribute[] subAttributes) {
		super(CompilerType.SCHEMA_COLUMN);
		this.dataType = dataType;

		/*if (this.dataType == DataType.STRING || this.dataType == DataType.INT || this.dataType == DataType.FLOAT)
			if (this.subAttributes != null)*/
		if ((this.subAttributes != null) && (!(this.dataType == DataType.COMPLEX || this.dataType == DataType.LIST)))
			throw new CompilerException("Sub attributes are only allowed for Complex and List types.");
			
		this.columnName = columnName;		
		this.subAttributes = subAttributes;
	}
	
	public BasePredicateStructuralAttribute(DataType dataType) {
		this(dataType, "", null);
	}

	public DataType getDataType() { return this.dataType; }
	
	public void setDataType(DataType dataType) { this.dataType = dataType; }

	public String getColumnName() { return this.columnName; }

	public BasePredicateStructuralAttribute[] getSchemaStructuralAttributes() { return this.subAttributes; }
		
	public String toString() {
		StringBuilder retval = new StringBuilder();
		
		if (this.columnName != null && this.columnName.length() > 0)
			retval.append(this.columnName + ":");
		
		retval.append(this.dataType.toString());
		
		if (this.subAttributes != null) {
			retval.append("[");
			for (int i = 0; i < this.subAttributes.length; i++)
				retval.append(this.subAttributes[i].toString());
			retval.append("]");
		}
		
		return retval.toString();
	}
	
	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}

	public BasePredicateStructuralAttribute copy() {
		return new BasePredicateStructuralAttribute(this.dataType, this.columnName, this.subAttributes);
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		throw new RuntimeException("Column type can not be copied with variable list.");
	}

	@Override
	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof BasePredicateStructuralAttribute))
			return false;
		
		BasePredicateStructuralAttribute otherSchemaStructuralAttribute = (BasePredicateStructuralAttribute)other;
		
		if (this.dataType == null && otherSchemaStructuralAttribute.getDataType() != null)
			return false;
		
		if (!this.dataType.equals(otherSchemaStructuralAttribute.getDataType()))
			return false;
		
		if (this.columnName == null && otherSchemaStructuralAttribute.getColumnName() != null)
			return false;
		
		if (!this.columnName.equals(otherSchemaStructuralAttribute.getColumnName()))
			return false;
		
		if (this.subAttributes.length != otherSchemaStructuralAttribute.subAttributes.length)
			return false;
		
		for (int i = 0; i < this.subAttributes.length; i++)
			if (!this.subAttributes[i].equals(otherSchemaStructuralAttribute.subAttributes[i]))
				return false;

		return true;
	}
}
