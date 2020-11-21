package edu.ucla.cs.wis.bigdatalog.compiler;

import java.util.LinkedList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicateIndex;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicateKey;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

public class ModuleDeclaration extends CompilerTypeBase {
	protected String 					moduleName;
	protected List<BasePredicate> 		basePredicates;
	protected List<BasePredicateKey> 			basePredicateKeys;
	protected List<BasePredicateIndex> 		basePredicateIndexes;
	protected List<Export> 			exports;
	protected List<Rule> 				rules;
	protected List<Predicate>			facts;
	
	public ModuleDeclaration(String name) {
		super(CompilerType.MODULE_DECLARATION);
		this.moduleName = name;
		this.basePredicates = new LinkedList<>();
		this.basePredicateKeys= new LinkedList<>();
		this.basePredicateIndexes = new LinkedList<>();
		this.exports = new LinkedList<>();
		this.rules = new LinkedList<>();
		this.facts = new LinkedList<>();
	}
	
	public String getModuleName() { return this.moduleName; }

	public void addBasePredicate(BasePredicate basePredicate) {
		this.basePredicates.add(basePredicate);
	}
	
	public List<BasePredicate> getBasePredicates() { return this.basePredicates; }

	public void addBasePredicateKey(BasePredicateKey basePredicateKey) {
		this.basePredicateKeys.add(basePredicateKey);
	}
	
	public boolean isBasePredicate(String predicateName) {
		for (BasePredicate bp : this.basePredicates)
			if (bp.getPredicateName().equals(predicateName))
				return true;
		return false;
	}
	
	public List<BasePredicateKey> getBasePredicateKeys() { return this.basePredicateKeys; }

	public void addBasePredicateIndex(BasePredicateIndex basePredicateIndex) {
		this.basePredicateIndexes.add(basePredicateIndex);
	}
	
	public List<BasePredicateIndex> getBasePredicateIndexes() { return this.basePredicateIndexes; }
	
	public void addExport(Export export) {
		this.exports.add(export);
	}
	
	public List<Export> getExports() { return this.exports; }
	
	public void addRule(Rule rule) {
		this.rules.add(rule);
	}	
	
	public List<Rule> getRules() { return this.rules; }
	
	public void addFact(Predicate fact) {
		this.facts.add(fact);
	}
	
	public List<Predicate> getFacts() { return this.facts; }
	
	@Override
	public CompilerTypeBase copy() { return null; }

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) { return null; }

	@Override
	public boolean equals(CompilerTypeBase other) { return false; }

	@Override
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("Begin Module Declaration '");
		retval.append(this.moduleName);
		retval.append("' Description\n");
		retval.append(this.toStringBasePredicates());
		retval.append("\n");
		retval.append(this.toStringRules());
		retval.append("\n");
		retval.append(this.toStringExports());
		retval.append("\nEnd Module Declaration '");
		retval.append(this.moduleName);
		retval.append("' Description");
		return retval.toString();
	}
	
	public String toStringBasePredicates() {
		StringBuilder retval = new StringBuilder();
		if (this.basePredicates.size() == 0) {
			retval.append("  No Base Predicates.");
		} else {
			if (this.basePredicates.size() > 0) {
				retval.append("  Base Predicates for Module '" + this.moduleName + "'\n");

				for (BasePredicate basePredicate : this.basePredicates)  {					
					retval.append("    " + basePredicate.toString());
					retval.append("\n");
				}
				
				for (BasePredicateKey basePredicateKey : this.basePredicateKeys) {
					retval.append("    " + basePredicateKey.toString());
					retval.append("\n");				
				}
				
				for (BasePredicateIndex basePredicateIndex : this.basePredicateIndexes) {
					retval.append("    " + basePredicateIndex.toString());
					retval.append("\n");				
				}				
			}
	    }
		return retval.toString();
	}

	public String toStringExports() {
		StringBuilder retval = new StringBuilder();
		if (this.exports.size() > 0) {
			retval.append("  Exports in Module '");
			retval.append(this.moduleName);
			retval.append("'\n");

			for (Export export : this.exports) {
				retval.append("    " + export.toString());

				if (export.getQueryForm().isCompiled())
					retval.append("  [COMPILED]");
				else
					retval.append("  [NOT COMPILED]");
				retval.append("\n");
			}
		} else {
			retval.append("  Module contains no exports.");
		}
		return retval.toString();
	}

	public String toStringRules() {
		StringBuilder retval = new StringBuilder();
		if (this.rules.size() > 0) {
			retval.append("  Rules in Module '");
			retval.append(this.moduleName);
			retval.append("'\n");

			for (Rule rule : this.rules)
				retval.append("   " + rule.toString() + "\n");
			
		} else {
			retval.append("  Module contains no rules.");
		}		

		return retval.toString();
	}
}
