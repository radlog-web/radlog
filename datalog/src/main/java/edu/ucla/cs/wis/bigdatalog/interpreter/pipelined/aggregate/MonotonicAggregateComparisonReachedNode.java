package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate;

import java.util.Arrays;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.type.DbSet;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class MonotonicAggregateComparisonReachedNode extends OrNode {

	private boolean 		isRead;
	private MonotonicAggregateComparisonReachedNode companion; 
	private DbSet 			tracker;
	private DbTypeBase[] 	keyArr;
	protected boolean 		initialized;
	
	public MonotonicAggregateComparisonReachedNode(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables, boolean isRead) {
		super(predicateName, args, binding, freeVariables);
		this.isRead = isRead;
		if (args.size() > 1)
			this.keyArr = new DbTypeBase[args.size()];
	}
	
	public void setCompanion(MonotonicAggregateComparisonReachedNode companion) {
		this.companion = companion;
	}
	
	public DbSet getTracker() { return this.tracker; }

	public boolean initialize() {
		if (!this.initialized) {
			if (this.isRead) {
				this.initializeTracker();				
			} else {
				if (this.companion == null)
					this.initializeTracker();
				else
					this.tracker = this.companion.tracker;
			}
			
			this.initialized = true;
		}
		return true; 
	}
	
	private void initializeTracker() {
		if (this.arguments.size() == 1) {
			this.tracker = this.typeManager.createSet(this.arguments.get(0).getDataType());
		} else {
			DataType[] keyType = new DataType[this.arguments.size()];
			for (int i = 0; i < this.arguments.size(); i++)
				keyType[i] = this.arguments.get(0).getDataType();
			this.tracker = this.typeManager.createSet(keyType);
		}
	}
	
	public Status getTuple() {
		Status status = Status.FAIL;
	  
		this.traceGetTupleEntry();

		if (this.keyArr != null)
			for (int i = 0; i < this.arguments.size(); i++)
				this.keyArr[i] = this.getArgumentAsDbType(i);
		
		if (this.isEntry) {		
			if (this.isRead) {			
				if (this.keyArr != null) {
					if (this.tracker.get(this.keyArr) == null)
						status = Status.SUCCESS;	
				} else {
					if (this.tracker.get(this.getArgumentAsDbType(0)) == null)
						status = Status.SUCCESS;
				}
			} else {
				status = Status.SUCCESS;
				
				if (this.keyArr == null) {
					this.tracker.put(this.getArgumentAsDbType(0));
					System.out.println("adding " + this.getArgumentAsDbType(0));
				} else {
					this.tracker.put(this.keyArr);
					System.out.println("adding " + Arrays.toString(this.keyArr));
				}
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

	public String toString() {
		return toStringNode();
	}	
}
