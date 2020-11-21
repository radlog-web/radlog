package edu.ucla.cs.wis.bigdatalog.compiler.predicate;

import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

public class IfThenElsePredicate extends BuiltInPredicate {
	List<Predicate> ifLiterals;
	List<Predicate> thenLiterals;
	List<Predicate> elseLiterals;

	public IfThenElsePredicate(List<Predicate> ifLiterals, List<Predicate> thenLiterals, List<Predicate> elseLiterals) {
		super (BuiltInPredicate.IFTHENELSE_PREDICATE_NAME, new CompilerTypeList(), CompilerType.IFTHENELSE_PREDICATE, 
				BuiltInPredicateType.IFTHENELSE);
		this.ifLiterals = ifLiterals;
		this.thenLiterals = thenLiterals;
		this.elseLiterals = elseLiterals;
	}
	
	public IfThenElsePredicate(CompilerTypeList ifLiteralsList, CompilerTypeList thenLiteralsList, CompilerTypeList elseLiteralsList) {
		super (BuiltInPredicate.IFTHENELSE_PREDICATE_NAME, new CompilerTypeList(), CompilerType.IFTHENELSE_PREDICATE, 
				BuiltInPredicateType.IFTHENELSE);
		
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
		
		List<Predicate> elseLiterals = null;
		if (elseLiteralsList != null && elseLiteralsList.size() > 0) {
			elseLiterals = new ArrayList<>();
		
			for (int i = 0; i < elseLiteralsList.size(); i++)
				elseLiterals.add((Predicate) elseLiteralsList.get(i));
			this.elseLiterals = elseLiterals;
		}
	}
		
	public List<Predicate> getIfLiterals() {return this.ifLiterals;}

	public List<Predicate>getThenLiterals() {return this.thenLiterals;}

	public List<Predicate>getElseLiterals() {return this.elseLiterals;}

	public IfThenElsePredicate copy() {		
		List<Predicate> ifLiteralsCopy = new ArrayList<>();
		List<Predicate> thenLiteralsCopy = new ArrayList<>();
		List<Predicate> elseLiteralsCopy = new ArrayList<>();
		
		for (Predicate predicate : this.ifLiterals)
			ifLiteralsCopy.add(predicate.copy());
		
		for (Predicate predicate : this.thenLiterals)
			thenLiteralsCopy.add(predicate.copy());
		
		for (Predicate predicate : this.elseLiterals)
			elseLiteralsCopy.add(predicate.copy());		
		
		return new IfThenElsePredicate(ifLiteralsCopy,  thenLiteralsCopy,  elseLiteralsCopy);
	}

	public IfThenElsePredicate copy(CompilerVariableList variableList) {
		List<Predicate> ifLiteralsCopy = new ArrayList<>();
		List<Predicate> thenLiteralsCopy = new ArrayList<>();
		List<Predicate> elseLiteralsCopy = new ArrayList<>();
		
		for (Predicate predicate : this.ifLiterals)
			ifLiteralsCopy.add(predicate.copy(variableList));
		
		for (Predicate predicate : this.thenLiterals)
			thenLiteralsCopy.add(predicate.copy(variableList));
		
		for (Predicate predicate : this.elseLiterals)
			elseLiteralsCopy.add(predicate.copy(variableList));	
		
		return new IfThenElsePredicate(ifLiteralsCopy,  thenLiteralsCopy,  elseLiteralsCopy);
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
		retval.append(" else ");
		
		if (this.elseLiterals.size() > 0) {
			retval.append(this.elseLiterals.get(0).toString());

			for (int i = 1; i < this.elseLiterals.size(); i++) {
				retval.append(", ");
				retval.append(this.elseLiterals.get(i).toString());
			}
		}
		retval.append(" )"); 
		return retval.toString();
	}
}
