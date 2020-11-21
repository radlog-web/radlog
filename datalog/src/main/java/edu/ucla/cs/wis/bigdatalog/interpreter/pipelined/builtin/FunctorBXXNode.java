package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.type.DbComplex;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;

public class FunctorBXXNode 
	extends FunctorNode {
	
	public FunctorBXXNode(NodeArguments args, Binding binding, VariableList freeVariables) {
		super(args, binding, freeVariables);
	}
	
	public Status getTuple()	{
		Status status;

		this.traceGetTupleEntry();

		if (this.isEntry) {
			if ((this.complexObject = (DbComplex) this.getArgumentAsDbType(0)) != null) {
				boolean isComplexObjectValid = false;

				if (this.complexObject instanceof DbComplex) {
					this.functor = this.complexObject.getName();
					this.argumentList = this.typeManager.createList(this.complexObject.getArguments());					
					isComplexObjectValid = true;
				} /*else if (this.complexObject.isString()) {
					this.functor = this.complexObject;
					this.argumentList = this.typeManager.createNilList();
					isComplexObjectValid = true;
				}*/

				if (isComplexObjectValid
						&& this.getArgument(1).match(this.functor)
						&& this.getArgument(2).match(this.argumentList)) {
						//&& (this.matchDbTypeToArgument(this.functor, this.getArgument(1)))
						//&& (this.matchDbTypeToArgument(this.argumentList, this.getArgument(2)))) {
					status = Status.SUCCESS;
					this.isEntry = false;
				} else {
					status = Status.ENTRY_FAIL;
				}
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
	public FunctorBXXNode copy(ProgramContext programContext) {
		FunctorBXXNode copy = new FunctorBXXNode(programContext.copyArguments(this.arguments),  
				this.bindingPattern.copy(),  
				programContext.copyVariableList(this.freeVariableList));
		
		programContext.getNodeMapping().put(this, copy);
		
		return copy;
	}
}
