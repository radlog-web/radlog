package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.fs;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;

public class FSMaxManyFunction extends OrNode {
	public FSMaxManyFunction(NodeArguments args, Binding binding, VariableList freeVariables) {		  
		super(BuiltInPredicate.FS_MAX_PREDICATE_NAME, args, binding, freeVariables);
	}

	@Override
	public boolean initialize() { return true; }

	@Override
	public void partialCleanUp() {
		this.cleanUp();
	}

	@Override
	public String toString() { return toStringNode(); }

	@Override
	public Status getTuple() {
		Status status = Status.FAIL;
		DbTypeBase dbArg1, dbArg2;

		this.traceGetTupleEntry();

		this.freeVariableList.makeFree();
		
		if (this.isEntry) {
			this.isEntry = false;

			// if old value is nil, we return the new value
			if ((dbArg2 = this.getArgumentAsDbType(1)) == DbList.create()) {
				//we will always have a new max in this case
				if (this.getArgument(2).matchByFree(this.getArgument(0))
					&& this.getArgument(3).matchByFree(DbInteger.create(1))) {
					status = Status.SUCCESS;
				} else {
					status = Status.ENTRY_FAIL;
					this.cleanUp();
				}
			} else {
				boolean retval = false;
				dbArg1 = this.getArgumentAsDbType(0);
				// both arg1 and arg2 should have values for MULTI
				// IsNewMaxValue_N is set to 1 if we have a new max value, 0 otherwise
				// Ym=max(Yold,Ys)
				if (dbArg1.greaterThan(dbArg2)) {
					retval = (this.getArgument(2).match(dbArg1) && 
							this.getArgument(3).matchByFree(DbInteger.create(1)));
				} else {
					retval = (this.getArgument(2).match(dbArg2) && 
							this.getArgument(3).matchByFree(DbInteger.create(0)));
				}

				if (retval) {
					status = Status.SUCCESS;
				} else {
					status = Status.ENTRY_FAIL;
					this.cleanUp();
				}
			}
		} else {
			status = Status.FAIL;
			this.cleanUp();
		}

		this.traceGetTupleExit(status);

		return status;
	}

}
