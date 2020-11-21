package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.list;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;

public class CardinalityNode 
	extends OrNode {
	
	public CardinalityNode(NodeArguments args, Binding binding, VariableList freeVariables) {
		super(BuiltInPredicate.CARDINALITY_PREDICATE_NAME, args, binding, freeVariables);
	}

	@Override
	public boolean initialize() { return true; }

	@Override
	public void deleteRelationsAndCursors() {	}

	@Override
	public void cleanUp() {
		this.baseNodeCleanUp();
	}

	@Override
	public void partialCleanUp() {
		this.cleanUp();
	}
	
	@Override
	public String toString() {
		return toStringNode();
	}

	@Override
	public Status getTuple() {
		Status status;

		this.traceGetTupleEntry();
		//this.unsetFreeVariables();
		this.freeVariableList.makeFree();
		
		if (this.isEntry) {
			DbTypeBase dbList = null;
			DbTypeBase cardinality = null;

			if ((dbList = this.getArgumentAsDbType(0)) != null) {
				if (dbList instanceof DbList) {
					cardinality = DbInteger.create(((DbList)dbList).getLength());
					status = Status.SUCCESS;
				} else {
					throw new InterpreterException("Invalid argument type.  Only list types allowed for cardinality predicate");
				}
		  
				if ((status == Status.SUCCESS) && this.getArgument(1).match(cardinality)) 
						//&& this.matchDbTypeToArgument(cardinality, this.getArgument(1)) == true)
					this.isEntry = false;
				else
					status = Status.ENTRY_FAIL;
			} else {
				status = Status.ENTRY_FAIL;
			}
	    } else {
	    	status = Status.FAIL;
	    }

		if (status != Status.SUCCESS)
			this.cleanUp();

		this.traceGetTupleExit(status);

		return status;
	}
	
	@Override
	public CardinalityNode copy(ProgramContext programContext) {
		CardinalityNode copy = new CardinalityNode(programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		
		programContext.getNodeMapping().put(this, copy);
		
		return copy;
	}
}
