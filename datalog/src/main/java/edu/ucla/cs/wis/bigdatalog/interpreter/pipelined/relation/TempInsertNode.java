package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;

public class TempInsertNode 
	extends RelationNode {	
	protected Tuple tuple;
	
	public TempInsertNode(String relationName, NodeArguments args, Binding binding) {
		super(relationName, args, binding, new VariableList());
	}

	@Override
	public boolean initialize() {
		//if ((this.relation = relationManager.getDerivedRelation(this.predicateName, this.arity)) == null)
		this.relation = this.relationManager.createDerivedRelation(this.predicateName, this.getSchema());
		//if ((this.relation = relationManager.getBaseRelation(this.predicateName, this.arity)) == null)
		//	this.relation = relationManager.createBaseRelation(this.predicateName, this.arity);

		if (this.relation != null)
			this.tuple = this.relation.getEmptyTuple();
		
		return (this.relation != null);
	}

	public String toString() {
		return "+" + this.toStringNode();
	}

	public void deleteRelationsAndCursors() {
		this.clearRelation();		
		this.relationManager.deleteDerivedRelation((DerivedRelation) this.relation);
	  	super.deleteRelationsAndCursors();
	}
	
	@Override
	public Status getTuple() {
		Status status;

		this.traceGetTupleEntry();

		if (this.isEntry) {
			this.setBoundColumnValues();
			for (int i = 0; i < this.boundColumnValues.length; i++)
				this.tuple.columns[i] = this.boundColumnValues[i];
			this.relation.add(this.tuple);
			//this.deleteFilterValuesOnBackTrack();
			this.isEntry = false;
			status = Status.SUCCESS;
	    } else {
	    	this.isEntry = true;
	    	status = Status.FAIL;
	    }

		this.traceGetTupleExit(status);

		return status;
	}
	
	public void cleanUp() {
		this.baseNodeCleanUp();
	}

	public void partialCleanUp() {
		this.cleanUp();
	}
	
	public void clearRelation() {
		if (this.relation != null)
			this.relation.cleanUp();
	}
	
	@Override
	public TempInsertNode copy(ProgramContext programContext) {
		TempInsertNode copy = new TempInsertNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy());
		copy.executionMode = this.executionMode;
		programContext.getNodeMapping().put(this, copy);
		return copy;
	}
}
