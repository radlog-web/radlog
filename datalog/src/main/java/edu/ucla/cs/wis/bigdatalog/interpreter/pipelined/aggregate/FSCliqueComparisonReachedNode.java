package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.type.DbSet;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class FSCliqueComparisonReachedNode extends OrNode {

	protected boolean 					initialized;
	private DbSet						tracker; // keys that have successfully passed comparison
	private DbTypeBase[] 				keys;	
	
	public FSCliqueComparisonReachedNode(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables) {
		super(predicateName, args, binding, freeVariables);
		this.keys = new DbTypeBase[args.size()];
	}
		
	public DbSet getTracker() { return this.tracker; }

	public boolean initialize() {
		if (!this.initialized) {
			this.initializeTracker();				
			this.initialized = true;
		}
		return true; 
	}
	
	private void initializeTracker() {		
		DataType[] keyTypes = new DataType[this.getArity()];
		for (int i = 0; i < this.getArity(); i++)
			keyTypes[i] = this.arguments.get(i).getDataType();
		
		this.tracker = this.typeManager.createSet(keyTypes);	
	}
	
	public Status getTuple() {
		Status status = Status.FAIL;
	  
		this.traceGetTupleEntry();

		if (this.isEntry) {
			status = Status.SUCCESS;
			
			if (this.getArity() == 1) {
				this.tracker.put(this.getArgumentAsDbType(0));
				//System.out.println("adding " + this.getArgumentAsDbType(0));
			} else {
				for (int i = 0; i < this.getArity(); i++)
					this.keys[i] = this.getArgumentAsDbType(i);
				
				this.tracker.put(this.keys);
				//System.out.println("adding " + Arrays.toString(this.keys));
			}			
			if (status == Status.SUCCESS)
				this.isEntry = false;
			else
				this.isEntry = true;
		} else {
			status = Status.FAIL;
			this.isEntry = true;
		}
		this.traceGetTupleExit(status);

		return status;
	}
	
	public void deleteRelationsAndCursors() {  }

	public void cleanUp() {
		this.freeVariableList.makeFree();
		this.baseNodeCleanUp();
	}

	public void partialCleanUp() {
		this.cleanUp();
	}

	@Override
	public String toString() {
		return toStringNode();
	}	
}
