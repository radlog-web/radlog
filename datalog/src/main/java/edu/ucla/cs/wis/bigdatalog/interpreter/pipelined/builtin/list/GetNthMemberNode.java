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
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.system.DeALSConfiguration;

public class GetNthMemberNode 
	extends MemberNode {
	protected DbTypeBase memberObject;

	public GetNthMemberNode(NodeArguments args, Binding binding, VariableList freeVariables) {
		super(BuiltInPredicate.GET_NTH_MEMBER_PREDICATE_NAME, args, binding, freeVariables);
	}

	@Override
	public void cleanUp() {
		this.baseNodeCleanUp();
		this.freeVariableList.makeFree();
		//this.unsetFreeVariables();
	}

	@Override
	public Status getTuple() {
		Status status = Status.ENTRY_FAIL;

		this.traceGetTupleEntry();
		//this.unsetFreeVariables();
		this.freeVariableList.makeFree();

		if (this.isEntry) {
			DbTypeBase dbList = null;
			DbTypeBase position = null;

			if (((position = this.getArgumentAsDbType(1)) != null)
					&& (position instanceof DbInteger)) {
				int index = ((DbInteger)position).getValue();
				boolean isSuccess = false;

				if ((dbList = this.getArgumentAsDbType(0)) != null) {
					if (dbList instanceof DbList) {
						if ((index <= ((DbList)dbList).getLength()) && (index > 0)) {
							this.memberObject = ((DbList)dbList).get(index-1);
							isSuccess = true;
						}
					} else {
						throw new InterpreterException("Invalid argument type.  Only list types allowed for nth predicate");
					}
					//this.matchDbTypeToArgument(this.memberObject, this.getArgument(2))) {
					if (isSuccess && this.getArgument(2).match(this.memberObject)) {
						status = Status.SUCCESS;
						this.isEntry = false;
					}
				}
			}
		} else {
			status = Status.FAIL;
			this.isEntry = true;
		}

		this.traceGetTupleExit(status);

		return status;
	}

	@Override
	public GetNthMemberNode copy(ProgramContext programContext) {
		GetNthMemberNode copy = new GetNthMemberNode(programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		programContext.getNodeMapping().put(this, copy);
		return copy;
	}
}
