package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.list;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.type.DbList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;

public class SubsetNode 
	extends MemberNode {
	/*APS 3/2/2014 - this class is to compare two ordered lists for subseted-ness*/
	public SubsetNode(NodeArguments args, Binding binding, VariableList freeVariables) {
		super("subset", args, binding, freeVariables);
	}
	
	@Override
	public void cleanUp() {
		this.baseNodeCleanUp();
	}
	
	@Override
	public Status getTuple() {
		Status status;

		this.traceGetTupleEntry();  

		//this.unsetFreeVariables();
		this.freeVariableList.makeFree();
		
		if (this.isEntry) {
			DbList dbList1 = null;
			DbList dbList2 = null;

			status = Status.ENTRY_FAIL;

			if (((dbList1 = (DbList)this.getArgumentAsDbType(0)) != null) && 
					((dbList2 = (DbList)this.getArgumentAsDbType(1)) != null)) {
				
				DbTypeBase dbList1Head = null;
				DbTypeBase dbList2Head = null;
				while (!dbList1.isEmpty() && !dbList2.isEmpty()) {
					dbList1Head = dbList1.getHead();
					dbList2Head = dbList2.getHead();
					
					// lists are ordered, so we can compare
					if (dbList1Head.isNumber() && dbList2Head.isNumber()) {
						if (dbList2Head.lessThan(dbList1Head)) {
							dbList2 = dbList2.getTail();
						} else if (dbList1Head.equals(dbList2Head)) {
							dbList1 = dbList1.getTail();
							dbList2 = dbList2.getTail();
						} else {
							status = Status.FAIL;
							break;
						}
					}
				}
				
				if (status != Status.FAIL)
					status = Status.SUCCESS;
				
			} else {
				throw new InterpreterException("Invalid argument type.  Only list types allowed for subset predicate");
			}
			
			

			if (status == Status.SUCCESS)
				this.isEntry = false;

		} else {
			this.cleanUp();
			status = Status.FAIL;
	    }

		this.traceGetTupleExit(status);

		return status;
	}

	@Override
	public SubsetNode copy(ProgramContext programContext) {
		SubsetNode copy = new SubsetNode(programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		programContext.getNodeMapping().put(this, copy);
		return copy;
	}
}
