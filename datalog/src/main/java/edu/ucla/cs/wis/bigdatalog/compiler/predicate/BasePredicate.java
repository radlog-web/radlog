package edu.ucla.cs.wis.bigdatalog.compiler.predicate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.CompilerException;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNodeChild;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class BasePredicate extends PredicateBase implements PCGOrNodeChild, Serializable {
	private static final long serialVersionUID = 1L;
	protected List<BasePredicateStructuralAttribute> 	basePredicateStructuralAttributes;
	protected List<BasePredicateKey> 					basePredicateKeys;
	protected List<BasePredicateIndex> 				basePredicateIndices;
	
	public BasePredicate(String predicateName, List<BasePredicateStructuralAttribute> columns) {
		super(predicateName, columns.size(), CompilerType.SCHEMA_RELATION);

		// these better be passed in ordinal position order
		this.basePredicateStructuralAttributes = columns;
		this.basePredicateKeys = new ArrayList<>();
		this.basePredicateIndices = new ArrayList<>();
	}

	public boolean addKey(BasePredicateKey key) {
		boolean status = false;
		if (this.verifyOrdinalTerms(key.getOrdinalTerms())) {						
			if (key.getOrdinalTerms().length > this.arity)
				throw new CompilerException("Key " + key.toString() + " has too many columns for relation " + this.toString() + ".");
			
			status = this.basePredicateKeys.add(key);
		}
		return status;
	}

	public boolean addIndex(BasePredicateIndex index) {
		boolean status = false;
		if (this.verifyOrdinalTerms(index.getOrdinalTerms())) {
			if (index.getOrdinalTerms().length > this.arity)
				throw new CompilerException("Index " + index.toString() + " has too many columns for relation " + this.toString() + ".");

			status = this.basePredicateIndices.add(index);
		}
		return status;
	}

	public boolean verifyOrdinalTerms(int[]  ordinalPositions) {
		boolean status = true;

		for (Integer ordinalPosition : ordinalPositions) {
			if (ordinalPosition >= this.basePredicateStructuralAttributes.size())
				throw new CompilerException("Ordinal position " + ordinalPosition + " exceeds the number of columns for relation " + this.predicateName);
		}

		return status;
	}

	public int getNumberOfKeys() {return this.basePredicateKeys.size();}

	public int getNumberOfIndices() {return this.basePredicateIndices.size();}

	public List<BasePredicateKey> getBasePredicateKeys() { return this.basePredicateKeys; }
	
	public BasePredicateKey getBasePredicateKey(int position) { return this.basePredicateKeys.get(position); }

	public List<BasePredicateIndex> getBasePredicateIndexes() { return this.basePredicateIndices; }
	
	public BasePredicateIndex getBasePredicateIndex(int position) { return this.basePredicateIndices.get(position); }
	
	public List<BasePredicateStructuralAttribute> getBasePredicateStructuralAttributes() { return this.basePredicateStructuralAttributes; }

	public BasePredicateStructuralAttribute getBasePredicateStructuralAttribute(int position) { return this.basePredicateStructuralAttributes.get(position); }
	
	public DataType[] getSchema() { 
		DataType[] dataTypes = new DataType[this.basePredicateStructuralAttributes.size()];
		for (int i = 0; i < this.basePredicateStructuralAttributes.size(); i++)
			dataTypes[i] = this.basePredicateStructuralAttributes.get(i).getDataType();

		return dataTypes;
	}

	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(this.predicateName + "(");

		for (int i = 0; i < this.basePredicateStructuralAttributes.size(); i++) {
			if (i > 0)
				output.append(", ");
			output.append(this.basePredicateStructuralAttributes.get(i).toString());
		}

		output.append(")");
		return output.toString();
	}

	public String toJson() {
		StringBuilder output = new StringBuilder();
		output.append("{\"name\":\"");
		output.append(this.predicateName);
		output.append("\", \"columns\":[");
		for (int i = 0; i < this.basePredicateStructuralAttributes.size(); i++) {
			if (i > 0)
				output.append(",");
			output.append(this.basePredicateStructuralAttributes.get(i).toJson());
		}
		output.append("]}");
		return output.toString();				
	}
	
	@Override
	public void unsetChildrenVariables() {
		//do nothing - for PCGNodeChild		
	}

	@Override
	public void unsetVariables() {
		//do nothing - for PCGNodeChild
	}
}
