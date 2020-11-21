package edu.ucla.cs.wis.bigdatalog.compiler.predicate;

import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

public class IfThenPredicate extends BuiltInPredicate {
	protected List<Predicate> ifLiterals;
	protected List<Predicate> thenLiterals;
	
	public IfThenPredicate(List<Predicate> ifLiterals, List<Predicate> thenLiterals) {
		super(BuiltInPredicate.IFTHEN_PREDICATE_NAME, new CompilerTypeList(), CompilerType.IFTHEN_PREDICATE,
				BuiltInPredicateType.IFTHEN);		
		this.ifLiterals = ifLiterals;
		this.thenLiterals = thenLiterals;
	}
	
	public IfThenPredicate(CompilerTypeList ifLiteralsList, CompilerTypeList thenLiteralsList) {
		super(BuiltInPredicate.IFTHEN_PREDICATE_NAME, new CompilerTypeList(), CompilerType.IFTHEN_PREDICATE,
				BuiltInPredicateType.IFTHEN);		
		
		List<Predicate> ifLiterals = null;
		if (ifLiteralsList != null && ifLiteralsList.size() > 0) {
			ifLiterals = new ArrayList<>();
		
			for (int i = 0; i < ifLiteralsList.size(); i++)
				ifLiterals.add((Predicate) ifLiteralsList.get(i));
			this.ifLiterals = ifLiterals;
		}
		
		List<Predicate> thenLiterals = null;
		if (thenLiteralsList != null && thenLiteralsList.size() > 0) {
			thenLiterals = new ArrayList<>();
		
			for (int i = 0; i < thenLiteralsList.size(); i++)
				thenLiterals.add((Predicate) thenLiteralsList.get(i));
			this.thenLiterals = thenLiterals;
		}
	}

	public List<Predicate> getIfLiterals() {return this.ifLiterals;}

	public List<Predicate> getThenLiterals() {return this.thenLiterals;}

	public IfThenPredicate copy() {		
		List<Predicate> ifLiteralsCopy = new ArrayList<>();
		List<Predicate> thenLiteralsCopy = new ArrayList<>();
		
		for (Predicate predicate : this.ifLiterals)
			ifLiteralsCopy.add(predicate.copy());
		
		for (Predicate predicate : this.thenLiterals)
			thenLiteralsCopy.add(predicate.copy());

		return new IfThenPredicate(ifLiteralsCopy, thenLiteralsCopy);
	}

	public IfThenPredicate copy(CompilerVariableList variableList) {
		List<Predicate> ifLiteralsCopy = new ArrayList<>();
		List<Predicate> thenLiteralsCopy = new ArrayList<>();
		
		for (Predicate predicate : this.ifLiterals)
			ifLiteralsCopy.add(predicate.copy(variableList));
		
		for (Predicate predicate : this.thenLiterals)
			thenLiteralsCopy.add(predicate.copy(variableList));
		
		return new IfThenPredicate(ifLiteralsCopy,  thenLiteralsCopy);
	}

	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("if( ");
		if (this.ifLiterals.size() > 0) {
			retval.append(this.ifLiterals.get(0).toString());
			
			for (int i = 1; i < this.ifLiterals.size(); i++) {
				retval.append(", ");
				retval.append(this.ifLiterals.get(i).toString());
			}
		}
		retval.append(" then ");
				
		if (this.thenLiterals.size() > 0) {
			retval.append(this.thenLiterals.get(0).toString());
					
			for (int i = 1; i < this.thenLiterals.size(); i++) {
				retval.append(", ");
				retval.append(this.thenLiterals.get(i).toString());
			}
		}
		
		retval.append(" )");
		return retval.toString();
	}
}