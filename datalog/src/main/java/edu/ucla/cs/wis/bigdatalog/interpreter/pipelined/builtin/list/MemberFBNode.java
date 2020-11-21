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

public class MemberFBNode 
	extends MemberNode {
	protected DbTypeBase dbList;
	protected DbList listNode;
	protected int memberIndex;

	public MemberFBNode(NodeArguments args, Binding binding, VariableList freeVariables) {
		super("member_fb", args, binding, freeVariables);

		this.memberIndex = 0;
		this.listNode = null;
	}
	
	@Override
	public void cleanUp() {
		this.freeVariableList.makeFree();
		this.baseNodeCleanUp();
		this.memberIndex = 0;
		this.listNode = null;
	}

	public Status getListTuple() {
		if ((this.listNode != null) && !this.listNode.isEmpty() &&
				this.getArgument(0).match(this.listNode.getHead())) {
				//this.matchDbTypeToArgument(this.listNode.getHead(), this.getArgument(0))) {
			this.listNode = this.listNode.getTail();
			return Status.SUCCESS;
		}
		return Status.FAIL;
	}

	@Override
	public Status getTuple()  {
		Status status = Status.ENTRY_FAIL;

		this.traceGetTupleEntry();  

		this.freeVariableList.makeFree();

		if (this.isEntry) {
			status = Status.ENTRY_FAIL;

			if ((this.dbList = this.getArgumentAsDbType(1)) != null) {
				if (this.dbList instanceof DbList) {
					this.listNode = ((DbList)this.dbList);
					status = this.getListTuple();
				} else {
					throw new InterpreterException("Invalid argument type.  Only list types allowed for member predicate");
				}
			}

			if (status == Status.SUCCESS)
				this.isEntry = false;
			else
				status = Status.ENTRY_FAIL;
		} else {
			if (this.dbList instanceof DbList)
				status = this.getListTuple();
		}

		if (status != Status.SUCCESS)
			this.cleanUp();

		this.traceGetTupleExit(status);

		return status;
	}
	
	@Override
	public MemberFBNode copy(ProgramContext programContext) {
		MemberFBNode copy = new MemberFBNode(programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		programContext.getNodeMapping().put(this, copy);
		return copy;
	}
}
