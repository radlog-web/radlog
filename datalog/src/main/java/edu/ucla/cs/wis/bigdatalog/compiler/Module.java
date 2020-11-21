package edu.ucla.cs.wis.bigdatalog.compiler;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.compiler.analysis.BasicClique;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicateIndex;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicateKey;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.DerivedPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.DbConvertible;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.BaseRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreConfiguration;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.exception.RelationManagerException;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.system.ModuleManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class Module {
	private static Logger logger = LoggerFactory.getLogger(Module.class.getName());
	
	protected DeALSContext 			deALSContext;
	protected String 					fileName;
	protected String 					moduleName;

	protected List<BasePredicate> 		basePredicates;
	protected List<Export> 			exports;
	protected List<DerivedPredicate> 	derivedPredicates;
	protected List<Predicate>			facts;
	protected List<BasicClique> 		cliques;
	
	// this is so we can display the original rules to a user, rather than the rewritten rules
	protected List<DerivedPredicate> 	derivedPredicatesSnapshot;
	
	protected boolean 				isModified;
	// APS 8/23/2013 - module needs to be in sync with a single data (for now)
	protected Database					database;
	
	public Module(DeALSContext deALSContext, Database database, String moduleName) {
		this.deALSContext = deALSContext;
		this.moduleName = moduleName;		
		// get an existing database if it exists
		this.database = database;
		this.basePredicates 	= new ArrayList<>();
		this.derivedPredicates 	= new ArrayList<>();
		this.exports 			= new ArrayList<>();
		this.cliques 			= new ArrayList<>();
		this.isModified 		= false;
	}
	
	public void reset() {
		// leave the module name alone
		this.destroyDatabaseRelations();
		
		this.fileName 					= null;		
		this.basePredicates 			= new ArrayList<>();
		this.derivedPredicates 			= new ArrayList<>();
		this.exports 					= new ArrayList<>();
		this.cliques 					= new ArrayList<>();
		this.isModified 				= false;
		this.derivedPredicatesSnapshot 	= null;
	}
	
	public String getModuleName() { return this.moduleName; }
	
	public Database getDatabase() { return this.database; }

	public String getFileName() { return this.fileName; }

	public List<Export> getExports() { return this.exports; }

	public List<DerivedPredicate> getDerivedPredicates() { return this.derivedPredicates; }
	
	public List<BasePredicate> getBasePredicates() { return this.basePredicates; }

	public List<BasicClique> getCliques() { return this.cliques; }

	public boolean isDefaultModule() { return this.moduleName.equals(ModuleManager.DEFAULT_MODULE_NAME); }
	
	public boolean isModified() { return this.isModified; }
	
	public void setIsModified(boolean value) { this.isModified = value; }

	public boolean addBasePredicate(BasePredicate basePredicate) {
		boolean found = false;
		// check for equality before adding new basePredicate
		for (BasePredicate bp : this.basePredicates) {
			if (bp.equals(basePredicate)) {
				this.deALSContext.logWarn(logger, "Attempting to add base predicate '{}' that already exists.", basePredicate.getPredicateName());
			
				found = true;
				break;
			}
		}
		if (!found)
			return this.basePredicates.add(basePredicate);

		return false;
	}

	public boolean addBasePredicateIndex(BasePredicateIndex basePredicateIndex) {
		String predicateName = basePredicateIndex.getBasePredicateName();
		if (predicateName != null) {
			for (BasePredicate basePredicate : this.basePredicates) {		  
				if (basePredicate.getPredicateName().equals(predicateName)) {
					boolean status = basePredicate.addIndex(basePredicateIndex);
				
					if (status)
						this.deALSContext.logInfo(logger, "Relation Index created: {}", basePredicate.getPredicateName());
					
					return status;
				}
			}
	    }

		String message = ""; 
		if (predicateName == null)
			message = "Can not add index " + basePredicateIndex.toString() + ".  Base Predicate name is null";
		else 
			message = "Can not add index " + basePredicateIndex.toString() + ".  Base Predicate name " + predicateName + " does not exist";
		logger.error(message);
		throw new CompilerException(message);
	}
	
	public boolean addBasePredicateKey(BasePredicateKey basePredicateKey) {
		String predicateName = basePredicateKey.getBasePredicateName();
		if (predicateName != null) {	
			for (BasePredicate basePredicate : this.basePredicates) {		  
				if (basePredicate.getPredicateName().equals(predicateName)) {
					boolean status = basePredicate.addKey(basePredicateKey);
					
					if (status)
						this.deALSContext.logInfo(logger, "Relation Key created: {}", basePredicate.getPredicateName());
					
					return status;
				}
			}
	    }

		String message = ""; 
		if (predicateName == null)
			message = "Can not add key " + basePredicateKey.toString() + ".  Base Predicate name is null";
		else 
			message = "Can not add key " + basePredicateKey.toString() + ".  Base Predicate name " + predicateName + " does not exist";
		logger.error(message);
		throw new CompilerException(message);
	}      

	public BasePredicate getBasePredicate(String predicateName) {
		for (BasePredicate	basePredicate : this.basePredicates) {
			if (basePredicate.getPredicateName().equals(predicateName)) 
				return basePredicate;
	    }

		return null;
	}

	public boolean addFact(Predicate fact) {
		if (!fact.isGround())
			throw new CompilerException("Fact is not ground. " + fact.toString());
		
		Relation relation = database.getRelationManager().getRelation(fact.getPredicateName(), fact.getArity());
		if (relation == null)
			throw new CompilerException("No relation found for fact " + fact.toString());	
		
		Tuple tuple = relation.getEmptyTuple();
		DbTypeBase value = null;
		for (int i = 0; i < fact.getArguments().size(); i++) {
			value = ((DbConvertible)fact.getArguments().get(i)).toDbType(this.database.getTypeManager());
			DataType toDataType = relation.getSchema()[i];
			DataType fromDataType = value.getDataType();
			if (fromDataType == toDataType) {
				tuple.setColumn(i, value);
			} else {//if (DataType.isPromotable(fromDataType, toDataType)) {
				// since the data types are not the same, try to promote between numeric types
				// if not numeric, no way to promote safely
				//if (!DataType.assignable(fromDataType, toDataType))
				//	throw new DatabaseException("Cannot load relation.  Data type mismatch [from: " + fromDataType.toString() + " to: " + toDataType.toString());	

				DbNumericType convertedValue = ((DbNumericType)value).convertToAllowNarrowing(toDataType);
				//if ((promotedValue == null)	&& ((fromDataType == DataType.STRING) && (toDataType == DataType.DATETIME)))
				//	promotedValue = new CompilerDateTime(((DbString)value).getValue()).toDbType();					
				if (convertedValue == null)
					throw new DatabaseException("Cannot load relation.  Data type mismatch [from: " + fromDataType.toString() + " to: " + toDataType.toString());
				
				tuple.setColumn(i, convertedValue);
			}
		}
		return (relation.add(tuple) != null);
	}

	public void removeDatabaseTuple(Predicate fact) {
		if (fact.isGround()) {
			Relation relation = database.getRelationManager().getRelation(fact.getPredicateName(), fact.getArity());
			if (relation != null) {
				Tuple tuple = relation.getEmptyTuple();
				for (int i = 0; i < fact.getArguments().size(); i++)
					tuple.setColumn(i, ((DbConvertible)fact.getArguments().get(i)).toDbType(this.database.getTypeManager()));
				
				relation.remove(tuple);
			} else {
				throw new CompilerException("No relation found for fact " + fact.toString());
			}
	    } else {
	    	throw new CompilerException("Fact is not ground. " + fact.toString());
	    }
	}

	public void createDatabaseRelations() {
		for (BasePredicate basePredicate : this.basePredicates) {
			this.database.getRelationManager().createBaseRelation(basePredicate.getPredicateName(), basePredicate.getSchema());
			// install indexes
			// install keys
		}
	}

	public void destroyDatabaseRelations() {
		for (BasePredicate basePredicate : this.basePredicates)
			this.database.getRelationManager().deleteBaseRelation(basePredicate.getPredicateName());
		
		for (DerivedPredicate derivedPredicate : this.derivedPredicates)
			this.database.getRelationManager().deleteDerivedRelation(derivedPredicate.getPredicateName(), derivedPredicate.getArity());
	}

	public boolean addExport(Export export) {
		boolean found = false;
		for (Export existingExport : this.exports) {			
			if (existingExport.getQueryForm() != null && export.getQueryForm() != null) {
				if (!existingExport.getQueryForm().getPredicateName().equals(export.getQueryForm().getPredicateName()))
					continue;
				
				if (existingExport.getQueryForm().getArity() != export.getQueryForm().getArity())
					continue;
				
				int i;
				for (i = 0; i < existingExport.getQueryForm().getArity(); i++) {
					if (existingExport.getQueryForm().getArgument(i).getType() 
							!= export.getQueryForm().getArgument(i).getType()) {
						break;
					}
				}
				
				if (i == existingExport.getQueryForm().getArity()) {
					found = true;
					break;
				}
			}
		}
		if (!found)
			return this.exports.add(export);
		
		return false;
	}
	
	public boolean addRule(Rule rule) {
		this.deALSContext.logTrace(logger, "Entering addRule for [{}|{}]", rule.getHead().getPredicateName(), rule.getHead().getArity());
		
		boolean exists = false;
		boolean status = false;
		
		// first, make sure rule's base predicates are identified
		for (Predicate literal : rule.getBody()) {
			if (literal.isDerived())				
				if (getBasePredicate(literal.getPredicateName()) != null)
					literal.setAsBasePredicate();
		}

		for (DerivedPredicate derivedPredicate : this.derivedPredicates) {
			if (derivedPredicate.getPredicateName().equals(rule.getHead().getPredicateName()) 
					&& (derivedPredicate.getArity() == rule.getHead().getArity())) {
				status = derivedPredicate.addRule(rule);
				exists = true;
				break;
			}
		}

		DerivedPredicate derivedPredicate; 
		for (BasicClique clique : this.cliques) {
			derivedPredicate = clique.getDerivedPredicate(rule.getHead().getPredicateName(), rule.getHead().getArity());
			if (derivedPredicate != null) {
				status = derivedPredicate.addRule(rule);
				exists = true;
				break;
			}
		}

		if (!exists) {
			derivedPredicate = new DerivedPredicate(rule.getHead().getPredicateName(), rule.getHead().getArity());
			derivedPredicate.addRule(rule);
			this.isModified = true;
			status = this.derivedPredicates.add(derivedPredicate);
		}
		
		this.deALSContext.logTrace(logger, "Exiting addRule for [{}|{}] with status = {}", rule.getHead().getPredicateName(), rule.getHead().getArity(), status);
		
		return status;
	}

	public void resetRecursiveDerivedPredicates() {
		BasicClique clique;

		for (int i = this.cliques.size() - 1; i >= 0; i--) {
			clique = this.cliques.remove(i);

			for (int j = clique.getNumberOfDerivedPredicates() - 1; j >= 0; j--)
				this.derivedPredicates.add(clique.removeDerivedPredicate(j));
		}
	}

	public void removeDerivedPredicate(String predicateName, int arity) {
		for (DerivedPredicate derivedPredicate : this.derivedPredicates) {
			if (derivedPredicate.getPredicateName().equals(predicateName) 
					&& arity == derivedPredicate.getArity()) {
				this.isModified = true;
				this.derivedPredicates.remove(derivedPredicate);
				return;
			}
		}
	}
	
	public void removeExport(QueryForm exportQueryFormToRemove) {

		for (Export export : this.exports) {
			QueryForm queryForm = export.getQueryForm();
			if (queryForm.getPredicateName().equals(exportQueryFormToRemove.getPredicateName())
					&& queryForm.getArity() == exportQueryFormToRemove.getArity()) {
				boolean found = true;
				int count = 0;
				// loop through list and compare argument type 
				// can not use .equals() because it does object reference equality
				for (CompilerTypeBase argument : queryForm.getArguments()) {
					if (argument.isVariable() 
							&& exportQueryFormToRemove.getArgument(count).isVariable()) {
						count++;
						continue;
					} else if (argument.isInputVariable() 
							&& exportQueryFormToRemove.getArgument(count).isInputVariable()) {
						count++;
						continue;
					} else {
						found = false;
						break;
					}					
				}
				
				if (found) {
					this.exports.remove(export);
					break;
				}
			}
		}
	}
	
	public boolean resetDerivedObjects() {
		this.derivedPredicates.clear();
		this.cliques.clear();
		this.exports.clear();				
		this.isModified = true;		
		return true;
	}

	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append("Begin Module '");
		output.append(this.moduleName);
		output.append("' Description\n");
		output.append(this.toStringBasePredicates());
		output.append("\n");
		output.append(this.toStringRules());
		output.append("\n");
		output.append(this.toStringExports());
		output.append("\nEnd Module '");
		output.append(this.moduleName);
		output.append("' Description");
		return output.toString();
	}
	
	public String toStringBasePredicates() {
		StringBuilder output = new StringBuilder();
		if (this.basePredicates.size() == 0) {
			output.append("  No Base Predicates.");
		} else {
			if (this.basePredicates.size() > 0) {
				output.append("  Base Predicates in Module '" + this.moduleName + "'\n");

				for (BasePredicate basePredicate : this.basePredicates)  {					
					output.append("    " + basePredicate.toString());
					output.append("\n");

					for (int j = 0; j < basePredicate.getNumberOfKeys(); j++) {						
						output.append("      " + basePredicate.getBasePredicateKey(j).toString());
						output.append("\n");
					}

					for (int j = 0; j < basePredicate.getNumberOfIndices(); j++) {
						output.append("      " + basePredicate.getBasePredicateIndex(j).toString());
						output.append("\n");
					}
				}
			}
	    }
		return output.toString();
	}

	public String toStringExports() {
		StringBuilder output = new StringBuilder();
		if (this.exports.size() > 0) {
			output.append("  Exports in Module '");
			output.append(this.moduleName);
			output.append("'\n");

			for (Export export : this.exports) {
				output.append("    " + export.toString());

				if (export.getQueryForm().isCompiled())
					output.append("  [COMPILED]");
				else
					output.append("  [NOT COMPILED]");
				output.append("\n");
			}
		} else {
			output.append("  Module contains no exports.");
		}
		return output.toString();
	}

	public String toStringRules() {
		StringBuilder output = new StringBuilder();
		if (this.derivedPredicates.size() > 0 || this.cliques.size() > 0) {
			output.append("  Rules in Module '");
			output.append(this.moduleName);
			output.append("'\n");

			for (DerivedPredicate derivedPredicate : this.derivedPredicates)
				output.append("   " + derivedPredicate.toString() + "\n");
	
			for (BasicClique clique : this.cliques) {
				for (DerivedPredicate derivedPredicate : clique.getDerivedPredicates())
					output.append("   " + derivedPredicate.toString() + "\n");
			}
			
		} else {
			output.append("  Module contains no rules.");
		}		

		return output.toString();
	}
	
	public String toJson() {
		StringBuilder output = new StringBuilder();
		
		output.append("{\"moduleName\":\"");
		output.append(this.moduleName);
		output.append("\",\"basePredicates\":[");
		
		for (int i = 0; i < this.basePredicates.size(); i++) {
			if (i > 0)
				output.append(",");
			output.append(this.basePredicates.get(i).toJson());
		}		
		
		output.append("],\"exports\":[");
		for (int i = 0; i < this.exports.size(); i++) {
			if (i > 0)
				output.append(",");
			output.append(this.exports.get(i).toJson());
		}

		output.append("],\"derivedPredicates\":[");
		// use a snapshot if it exists - this protects us from displaying rewritten rules in the json
		if (this.derivedPredicatesSnapshot == null) {
			for (int i = 0; i < this.derivedPredicates.size(); i++) {
				if (i > 0)
					output.append(",");
				output.append(this.derivedPredicates.get(i).toJson());
			}
		} else {			
			for (int i = 0; i < this.derivedPredicatesSnapshot.size(); i++) {
				if (i > 0)
					output.append(",");
				output.append(this.derivedPredicatesSnapshot.get(i).toJson());
			}
		}
		output.append("]}");
		return output.toString();
	}
	
	public DerivedPredicate getDerivedPredicate(String predicateName, int arity) {
		for (DerivedPredicate derivedPredicate : this.derivedPredicates) {
			if (derivedPredicate.getPredicateName().equals(predicateName)  
					&& arity == derivedPredicate.getArity())
				return derivedPredicate;
		}

		return null;
	}

	public BasicClique getBasicClique(String predicateName, int arity) {
		for (BasicClique clique : this.cliques) {
			if (clique.getDerivedPredicate(predicateName, arity) != null)
				return clique;
		}

		return null;
	}

	public QueryForm getQueryForm(Predicate query) {
		QueryForm queryForm = null;

		for (Export export : this.exports) {
			queryForm = export.getQueryForm();
			if ((queryForm != null) && queryForm.matchQuery(query))
				return queryForm.copy();
		}

		return null;
	}
	
	public List<Rule> getRules(String predicateName, int arity) {
		List<Rule> list = new LinkedList<>();
		
		DerivedPredicate dp = this.getDerivedPredicate(predicateName, arity);
		if (dp != null) {
			list.addAll(dp.getRules());
		} else {
			BasicClique clique = this.getBasicClique(predicateName, arity);
			dp = clique.getDerivedPredicate(predicateName, arity);
			if (dp != null)
				list.addAll(dp.getRules());
		}

		return list;
	}
	
	public boolean equals(Module other) {
		if (other == null)
			return false;
	
		if (this.fileName != null)
			return (this.fileName.equals(other.fileName) && this.moduleName.equals(other.moduleName));
		
		return this.moduleName.equals(other.moduleName);
	}
	
	public Module merge(ModuleDeclaration module) {
		if (module == null)
			return null;
		
		for (BasePredicate bp : module.getBasePredicates())
			this.addBasePredicate(bp);

		for (BasePredicateIndex bpi : module.getBasePredicateIndexes()) {
			BasePredicate bp = this.getBasePredicate(bpi.getBasePredicateName());
			if (bp == null)
				throw new CompilerException("No base predicate matching index : " + bpi.toString());
			
			if (bp.addIndex(bpi))
				this.deALSContext.logInfo(logger, "Relation Index created: {}", bp.getPredicateName());				
		}
		
		for (BasePredicateKey bpk : module.getBasePredicateKeys()) {
			BasePredicate bp = this.getBasePredicate(bpk.getBasePredicateName());
			if (bp == null)
				throw new CompilerException("No base predicate matching key : " + bpk.toString());
			
			if (bp.addKey(bpk))
				this.deALSContext.logInfo(logger, "Relation Key created: {}", bp.getPredicateName());
		}
		
		for (Export export : module.getExports())
			this.addExport(export);

		for (Rule rule : module.getRules())
			this.addRule(rule);
		
		if (module.getFacts() != null) {
			if (this.facts == null)
				this.facts = new LinkedList<>();
			
			for (Predicate fact : module.getFacts())
				this.facts.add(fact);
		}
		
		return this;
	}
	
	public void takeSnapshotOfDerivedPredicates() {
		this.derivedPredicatesSnapshot = new ArrayList<>();
		for (DerivedPredicate derivedPredicate : this.derivedPredicates)
			this.derivedPredicatesSnapshot.add(derivedPredicate.copy());
	}
	
	public boolean install(DeALSContext deALSContext) {
		
		for (BasePredicate basePredicate : this.getBasePredicates())
			this.install(basePredicate);
						
		// APS added 5/9/2014 so facts could be included in .deal file
		for (Predicate fact : this.facts) {
			// if the fact belongs to a base predicate, we add it, otherwise, we move it to a derived predicate
			BasePredicate basePredicate = this.getBasePredicate(fact.getPredicateName());
			if (basePredicate == null) {
				fact.setAsDerivedPredicate();
				this.addRule(new Rule(String.valueOf(deALSContext.getParser().getRuleIndex()), fact, null));
			} else {
				this.addFact(fact);
			}	
		}
		this.facts = null;
				
		return true;	
	}
	
	public BaseRelation install(BasePredicate basePredicate) {
		return this.install(basePredicate, null);
	}
	
	public BaseRelation install(BasePredicate basePredicate, TupleStoreConfiguration tsc) {
		BaseRelation baseRelation;
		if (tsc == null)
			baseRelation = database.getRelationManager().createBaseRelation(basePredicate.getPredicateName(), basePredicate.getSchema());
		else
			baseRelation = database.getRelationManager().createBaseRelation(basePredicate.getPredicateName(), basePredicate.getSchema(), tsc);
		
		if (baseRelation == null)
			throw new RelationManagerException("Relation can not be created for base predicate : " + basePredicate.toString());
		
		for (BasePredicateKey key : basePredicate.getBasePredicateKeys())
			if (baseRelation.addKeyIndex(key.getOrdinalTerms()) == null)
				throw new RelationManagerException("Key can not be created for relation : " + baseRelation.toString());

		for (BasePredicateIndex index : basePredicate.getBasePredicateIndexes())
			if (baseRelation.addSecondaryIndex(index.getOrdinalTerms()) == null)
				throw new RelationManagerException("Index can not be created for relation : " + baseRelation.toString());
		
		return baseRelation;
	}
}
