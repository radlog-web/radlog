package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.list;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.type.DbList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.system.DeALSConfiguration;

public class MemberBBNode 
	extends MemberNode {

	public MemberBBNode(NodeArguments args, Binding binding, VariableList freeVariables) {
		super("member_bb", args, binding, freeVariables);
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
			DbTypeBase dbTypeObject = null;
			DbTypeBase dbList = null;

			status = Status.ENTRY_FAIL;

			if (((dbTypeObject = this.getArgumentAsDbType(0)) != null) &&
					((dbList = this.getArgumentAsDbType(1)) != null)) {
				if (dbList instanceof DbList) {
					if (((DbList)dbList).contains(dbTypeObject))
						status = Status.SUCCESS;
				} else {
					throw new InterpreterException("Invalid argument type.  Only list types allowed for member predicate");
				}
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
	public MemberBBNode copy(ProgramContext programContext) {
		MemberBBNode copy = new MemberBBNode(programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		
		programContext.getNodeMapping().put(this, copy);
		
		return copy;
	}
}
