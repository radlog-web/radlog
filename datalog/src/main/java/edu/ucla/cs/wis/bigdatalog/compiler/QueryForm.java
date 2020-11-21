package edu.ucla.cs.wis.bigdatalog.compiler;

import java.util.ArrayList;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.DbConvertible;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerFunctor;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerInputVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.Program;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InputVariable;

public class QueryForm extends Predicate {	
	protected Program<?> 			program;
	protected Relation 				queryFormRelation;
	protected Cursor 				queryFormCursor;
	protected ArrayList<Integer> 	freeColumnIndex;
	public 	CompilerTypeList		variableList;
	protected String				persistedResultsRelationName;
	protected boolean				isUsingSurrogateRelation;

	public QueryForm(String predicateName, CompilerTypeList arguments) {
		super(predicateName, arguments, CompilerType.QUERY_FORM);
		this.initialize(arguments);
	}
	
	private void initialize(CompilerTypeList arguments) {
		this.program 					= null;
		this.queryFormRelation 			= null;
		this.freeColumnIndex 			= new ArrayList<>(arguments.size());
		this.isUsingSurrogateRelation 	= false;
	}	
	
	public void setAsBuiltInPredicate() {
		// TODO - fix this so query forms can be built in predicates
		throw new RuntimeException("fix this later");
	}
	
	public void setAsGenericPredicate() {
		// TODO - fix this so query forms can be built in predicates
		throw new RuntimeException("fix this later");
	}

	public boolean isCompiled() { return (this.program != null); }

	public void setProgram(Program<?> compiledProgram) {
		this.program = compiledProgram;
	}

	public Program<?> getProgram() { return this.program; }
	
	public void clearProgram() { this.program = null; }
	
	public Relation getQueryFormRelation() { return this.queryFormRelation; }
		
	public void setQueryFormRelation(Relation queryFormRelation) {
		this.queryFormRelation = queryFormRelation;
	}	

	public Cursor getQueryFormCursor(Database database) {
		if (this.queryFormCursor == null)
			this.queryFormCursor = database.getCursorManager().createScanCursor(this.queryFormRelation);
				
		return this.queryFormCursor; 
	}
	
	public void setQueryFormCursor(Cursor queryFormCursor) {
		this.queryFormCursor = queryFormCursor;
	}
		
	public String getPersistedResultsRelationName() { return this.persistedResultsRelationName; }
	
	public void setPersistedResultsRelationName(String persistedResultsRelationName) {
		this.persistedResultsRelationName = persistedResultsRelationName;
	}
	
	public void setIsUsingSurrogateRelation(boolean isUsingSurrogateRelation) { this.isUsingSurrogateRelation = isUsingSurrogateRelation; }
	
	public boolean isUsingSurrogateRelation() { return isUsingSurrogateRelation; }
	
	public QueryForm copy() {
		QueryForm queryForm = new QueryForm(this.predicateName, this.arguments.copy());
		queryForm.setProgram(this.program);
		return queryForm;
	}

	public QueryForm copy(CompilerVariableList variableList) {
		QueryForm queryForm = new QueryForm(this.predicateName, this.arguments.copy(variableList));
		queryForm.setProgram(this.program);
		return queryForm;
	}
	
	public QueryForm copyForResults() {
		QueryForm queryForm = new QueryForm(this.predicateName, this.arguments.copy());
		queryForm.setProgram(this.program);
		queryForm.setQueryFormRelation(this.queryFormRelation);
		queryForm.setQueryFormCursor(this.queryFormCursor);
		return queryForm;
	}

	public boolean matchQueryForm(QueryForm queryForm) {
		boolean status = false;
		if (this.getPredicateName().equals(queryForm.getPredicateName()) 
				&& (this.getArity() == queryForm.getArity()))
			status = this.matchTerms(this.getArguments(), queryForm.getArguments());
		this.unsetQueryFormBinding();
		return status;
	}

	public boolean matchQuery(Predicate query) {
		boolean status = false;
		if (this.getPredicateName().equals(query.getPredicateName())
				&& (this.getArity() == query.getArity()))
			status = this.matchTerms(this.getArguments(), query.getArguments());
		
		this.unsetQueryFormBinding();
		return status;
	}

	public boolean matchTerms(CompilerTypeList terms1, CompilerTypeList terms2) {
		for (int i = 0; i < terms1.size(); i++)
			if (!this.matchTerm(terms1.get(i), terms2.get(i)))
				return false;

		return true;
	}

	public boolean matchTerm(CompilerTypeBase term1, CompilerTypeBase term2) {
		boolean status = false;

		if (term1.isInputVariable()) {
			if (term2.isGround()) {
				if (term2.isInputVariable())
					((CompilerInputVariable)term1).setValue(((CompilerInputVariable)term2).getValue());
				else
					((CompilerInputVariable)term1).setValue(term2);
				status = true;
			}
		} else if (term1.isVariable() && term2.isVariable()) {
			((CompilerVariable)term1).setValue(term2);
			status = true;
		} else if (term1.isFunctor() && term2.isFunctor()) {
			CompilerFunctor functor1 = (CompilerFunctor) term1;
			CompilerFunctor functor2 = (CompilerFunctor) term2;

			if ((functor1.getFunctorName().equals(functor2.getFunctorName())) 
					&& (functor1.getArity() == functor2.getArity()))
				status = this.matchTerms(functor1.getArguments(), functor2.getArguments());
		} else if (term1.isList() && term2.isList()) {
			CompilerList list1 = (CompilerList) term1;
			CompilerList list2 = (CompilerList) term2;

			if (list1.isEmpty() && list2.isEmpty()) {
				status = true;
			} else { 
				if (!list1.isEmpty() && !list2.isEmpty() 
						&& this.matchTerm(list1.getHead(), list2.getHead())) {
					if ((list1.getTail() == null) && (list2.getTail() == null))
						status = true;
					else
						status = this.matchTerm(list1.getTail(), list2.getTail());
				}
			}
		} else if (term1.isConstant() && term2.isConstant()) {
			status = term1.equals(term2);
		}

		return status;
	}	

	public void unsetTerms(CompilerTypeList terms) {
		for (CompilerTypeBase term : terms)
			this.unsetTerm(term);
	}

	public void unsetQueryFormBinding() {
		this.unsetTerms(this.getArguments());
	}
	
	public void unsetTerm(CompilerTypeBase term) {
		if (term.isVariable())
			((CompilerVariable)term).makeFree();
		else if (term.isInputVariable())
			((CompilerInputVariable)term).makeFree();
		else if (term.isFunctor())
			this.unsetTerms(((CompilerFunctor)term).getArguments());
		else if (term.isList() && !((CompilerList)term).isEmpty()) {
			CompilerList list = (CompilerList)term;
			this.unsetTerm(list.getHead());
			if (list.getTail() != null)
				this.unsetTerm(list.getTail());
		}
	}
	/*
	public void bindProgramArguments(TypeManager typeManager) {
		if (this.program != null) {
			for (int i = 0; i < this.arguments.size(); i++) {
				if (this.program.getArgument(i) instanceof InputVariable) {
					DbTypeBase value = null;
					if (this.arguments.get(i).isInputVariable())
						value = ((DbConvertible)((CompilerInputVariable)this.arguments.get(i)).getValue()).toDbType(typeManager);
					else if (this.arguments.get(i).isConstant())
						value = (((DbConvertible)this.arguments.get(i)).toDbType(typeManager));
					
					((InputVariable)this.program.getArgument(i)).setValue(value);
				}
			}
			
			this.program.compressBoundArguments();
		}
	}*/
	
	public String toString() {
		return super.toString() + ".";
	}
	
	public String toString(boolean withBoundArgumentValues) {
		if (!withBoundArgumentValues)
			return toString();

		StringBuilder retval = new StringBuilder();
		switch (this.predicateOperatorType)
		{
			case POSITIVE:
				break;
			case NEGATIVE:
				retval.append("~");
				break;
			//case INSERT:				retval.append("+");				break;
			//case DELETE:				retval.append("-");				break;
		}

		retval.append(this.predicateName.toString());
		retval.append("(");
		
		if (this.arity > 0) {
			for (int i = 0; i < this.arguments.size(); i++) {
				if (i > 0)
					retval.append(", ");
				if (this.arguments.get(i).isInputVariable()) {
					if (this.program != null && this.program.isValid())
						retval.append(this.program.getArgument(i).toString());
					else
						retval.append(this.arguments.get(i).toString());	
				} else {
					retval.append(this.arguments.get(i).toString());
				}				
			}
		}
		retval.append(")");
		
		
		retval.append(".");
		return retval.toString();
	}
	
	// query form needs this because the arguments are typed, but it might be using a surrogate relationa nd cursor with different (i.e. bplustree) typed columns
	public Tuple getEmptyTuple() {
		Tuple tuple = new Tuple(this.getArity());
		for (int i = 0; i < this.getArity(); i++) {
			// APS 7/16/2014 using loadFrom and defaulting all values to 0
			if (this.getArgument(i).isVariable())
				tuple.columns[i] = DbTypeBase.loadFrom(((CompilerVariable)this.getArgument(i)).getDataType(), 0);
			else if (this.getArgument(i).isInputVariable())
				tuple.columns[i] = DbTypeBase.loadFrom(((CompilerInputVariable)this.getArgument(i)).getDataType(), 0);
			else 
				// if all else fails....
				tuple.columns[i] = DbTypeBase.loadFrom(this.queryFormRelation.getSchema()[i], 0);
		}
		
		return tuple;
	}
}
