package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.choice;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.FilteredScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.RelationNode;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class ChoicePredicate 
	extends RelationNode {
	
	public enum XYStageArgumentType {OLD, NEW, None}
	
	protected Tuple tuple;	
	private Database database;
	private XYStageArgumentType xyStageArgumentType = XYStageArgumentType.None;
	
	public ChoicePredicate(String name, NodeArguments args, Binding bindings) {
		super(name, args, bindings, new VariableList());
	}

	public void setXYStageArgumentType(XYStageArgumentType xyStageArgumentType) {
		this.xyStageArgumentType = xyStageArgumentType;
	}
	
	public XYStageArgumentType getXYStageArgumentType() { return this.xyStageArgumentType; }
	
	public boolean initialize() {
		// default case - index on 1st column
		int[] indexedColumns = new int[]{0};

		this.relation = this.relationManager.createDerivedRelation(this.predicateName, this.getSchema());
				
		if (this.relation != null) {
			this.relation.addSecondaryIndex(indexedColumns);
			this.cursor = this.database.getCursorManager().createCursor(this.relation, indexedColumns);
			this.tuple = this.relation.getEmptyTuple();
			return true;		
		}

		return false;				
	}

	public void deleteRelationsAndCursors() {
		this.relationManager.deleteBaseRelation(this.predicateName);
	  	super.deleteRelationsAndCursors();
	}

	public Status getTuple() {
		Status status;

		this.traceGetTupleEntry();

		if (this.isEntry) {
			//Tuple tuple = this.cursor.getEmptyTuple();

			this.setBoundColumnValues();
			((FilteredScanCursor)this.cursor).reset(new DbTypeBase[] {this.boundColumnValues[0]});

			if (this.cursor.getTuple(this.tuple) > 0) {
				if (this.tuple.getColumn(1).equals(this.boundColumnValues[1]))
					status = Status.SUCCESS;
				else
					status = Status.ENTRY_FAIL;
			}
			else {
				status = Status.SUCCESS;
			}
	    } else {
	    	status = Status.FAIL;
	    }

		if (status != Status.SUCCESS)
			this.cleanUp();
		else
			this.isEntry = false;

		this.traceGetTupleExit(status);
	  
		return status;
	}

	public void memoValue() {
		if (!this.isEntry) {
			for (int i = 0; i < this.boundColumnValues.length; i++)
				this.tuple.columns[i] = this.boundColumnValues[i];

			this.relation.add(this.tuple);
		}
	}
	
	@Override
	public ChoicePredicate copy(ProgramContext programContext) {
		ChoicePredicate copy = new ChoicePredicate(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy());
		programContext.getNodeMapping().put(this, copy);
		return copy;
	}
	
	@Override
	public void attachContext(DeALSContext deALSContext) {
		super.attachContext(deALSContext);
		this.database = deALSContext.getDatabase();
	}
}
