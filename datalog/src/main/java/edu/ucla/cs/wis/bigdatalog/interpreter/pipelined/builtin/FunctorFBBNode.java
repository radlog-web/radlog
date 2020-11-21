package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.type.DbComplex;
import edu.ucla.cs.wis.bigdatalog.database.type.DbList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbString;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.system.DeALSConfiguration;

public class FunctorFBBNode 
	extends FunctorNode {

	public FunctorFBBNode(NodeArguments args, Binding binding, VariableList freeVariables) {
		super(args, binding, freeVariables);
	}
	
	@Override
	public Status getTuple() {
		Status status;

		this.traceGetTupleEntry();

		if (this.isEntry) {
			if (((this.functor = this.getArgumentAsDbType(1)) != null) 
					&& ((this.argumentList = this.getArgumentAsDbType(2)) != null) 
					&& (this.functor instanceof DbString) 
					&& (this.argumentList instanceof DbList)) {
				
				int	numberOfArguments = ((DbList)this.argumentList).getLength();
				boolean isComplexObject = false;
		
				if (numberOfArguments == 0){
					this.complexObject = (DbComplex) this.functor;
					isComplexObject = true;
				} else {
					DbTypeBase[] dbTypeArray = new DbTypeBase[numberOfArguments];
					DbList dbList = (DbList)this.argumentList;
					
					int i = 0;
					while (i < numberOfArguments && !dbList.isEmpty()) {
						dbTypeArray[i] = dbList.getHead();
						dbList = dbList.getTail();
						i++;
					}					
					
					this.complexObject = this.typeManager.createComplex(this.functor.toString(), dbTypeArray);

					isComplexObject = true;
				}

				if (isComplexObject && this.getArgument(0).match(this.complexObject)) {
						//this.matchDbTypeToArgument(this.complexObject, this.getArgument(0))) {
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
	public FunctorFBBNode copy(ProgramContext programContext) {
		FunctorFBBNode copy = new FunctorFBBNode(programContext.copyArguments(this.arguments),  
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		programContext.getNodeMapping().put(this, copy);
		return copy;
	}
}
