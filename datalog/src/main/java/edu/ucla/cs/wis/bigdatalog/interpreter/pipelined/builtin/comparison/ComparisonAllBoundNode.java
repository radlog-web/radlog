package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.comparison;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.ProgramGeneratorException;
import edu.ucla.cs.wis.bigdatalog.interpreter.ComparisonOperation;
import edu.ucla.cs.wis.bigdatalog.interpreter.Interpreter;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;

public class ComparisonAllBoundNode 
	extends ComparisonNode {	
	protected DbTypeBase[] boundArgumentValues = new DbTypeBase[2];
	protected ComparisonOperation comparisonOperation;
	protected Interpreter interpreter;
	protected final boolean hasArithmeticArguments;

	public ComparisonAllBoundNode(String predicateName, NodeArguments args, VariableList freeVariables) {
		super(predicateName, args, new Binding(2, BindingType.BOUND), freeVariables);

		this.comparisonOperation = ComparisonOperation.getOperation(predicateName);
		if (this.comparisonOperation == ComparisonOperation.NONE)
			new ProgramGeneratorException("Unknown built-in comparison predicate");
		
		this.hasArithmeticArguments = this.hasArithmeticArguments();
	}
	
	@Override
	public void cleanUp() {
		this.baseNodeCleanUp();
	}
	
	@Override
	public Status getTuple() {
		Status status;
	  
		this.traceGetTupleEntry();

		if (this.isEntry) {
			boolean retval = true;
			//if (this.hasArithmeticArguments()) {
			if (this.hasArithmeticArguments) {
				this.boundArgumentValues[0] = this.getArgument(0).reduce().toDbType(this.typeManager);
				this.boundArgumentValues[1] = this.getArgument(1).reduce().toDbType(this.typeManager);
			} else {
				this.boundArgumentValues[0] = this.getArgumentAsDbType(0);
				this.boundArgumentValues[1] = this.getArgumentAsDbType(1);				
			}

			switch (this.comparisonOperation) {
				case EQUALITY:
					retval = this.boundArgumentValues[0].equals(this.boundArgumentValues[1]);
					break;
				case INEQUALITY:
					retval = this.boundArgumentValues[0].notEquals(this.boundArgumentValues[1]);
					break;
				case GREATER_THAN:
					retval = this.boundArgumentValues[0].greaterThan(this.boundArgumentValues[1]);
					break;
				case GREATER_THAN_OR_EQUAL:
					retval = this.boundArgumentValues[0].greaterThanOrEqualsTo(this.boundArgumentValues[1]);
					break;
				case LESS_THAN:
					retval = this.boundArgumentValues[0].lessThan(this.boundArgumentValues[1]);						
					break;
				default:
					retval = this.boundArgumentValues[0].lessThanOrEqualsTo(this.boundArgumentValues[1]);
			}
			
			if (retval) {
				status = Status.SUCCESS;
				this.isEntry = false;
			} else {
				status = Status.FAIL;
			}	
	    } else {
	    	this.isEntry = true;
	    	status = Status.ENTRY_FAIL;
	    }

		this.traceGetTupleExit(status);

		return status;
	}
	
	@Override
	public ComparisonAllBoundNode copy(ProgramContext programContext) {
		ComparisonAllBoundNode copy = new ComparisonAllBoundNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				programContext.copyVariableList(this.freeVariableList));
		
		programContext.getNodeMapping().put(this, copy);
		
		return copy;
	}
}
