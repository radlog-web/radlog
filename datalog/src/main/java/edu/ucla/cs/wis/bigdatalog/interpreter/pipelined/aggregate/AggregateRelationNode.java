package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.interpreter.ArithmeticExpression;
import edu.ucla.cs.wis.bigdatalog.interpreter.ArithmeticOperation;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.Utilities;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.AndNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ExecutionMode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.QueryFormNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.RelationNode;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

abstract public class AggregateRelationNode 
	extends RelationNode 
	implements QueryFormNode {
	
	protected int 						numberOfAggregateColumns;  
	protected int 						numberOfKeyColumns;
	protected boolean 					allKeyColumnsBound;
	protected AggregateInfo[]			aggregateInfos;
	protected int						numberOfDistinctAggregates;
	protected Tuple						returnTuple;
	protected ArithmeticExpression[]	expressions;
	protected AggregateStoreType		aggregateStoreType;
	protected boolean					isMaterialized;
	protected Database					database;
	
	public AggregateRelationNode(String predicateName, NodeArguments args, Binding binding, 
			VariableList freeVariables, AggregateInfo[] aggregateInfos, AggregateStoreType aggregateStoreType) {
		super(predicateName, args, binding, freeVariables);

		this.aggregateInfos = aggregateInfos;
		
		this.expressions = new ArithmeticExpression[this.aggregateInfos.length];
		for (int i = 0; i < this.aggregateInfos.length; i++) {
			if (this.aggregateInfos[i].aggregateType == AggregateFunctionType.SUM)
				this.expressions[i] = new ArithmeticExpression(ArithmeticOperation.ADDITION);
		}
		this.aggregateStoreType = aggregateStoreType;
		this.executionMode = ExecutionMode.Pipelined;
		this.isMaterialized = false;
	}
	
	public AggregateInfo[] getAggregateInfos() { return this.aggregateInfos; }
	
	public void setExecutionModeMaterialized() { 
		this.executionMode = ExecutionMode.Materialized; 
	}
	
	@Override
	public Status getOrNodeTuple() {
		Status status = Status.FAIL;
		
		//if (!this.isEntry && this.hasAllArgumentsBound) {
		//	this.partialCleanUp();
		//} else {
		while (this.currentChildIndex < this.children.length && status != Status.SUCCESS) {
			depth++;
			status = this.children[this.currentChildIndex++].getTuple();
			depth--;
		}
		
		if (DEBUG && this.deALSContext.isDerivationTrackingEnabled() && (this.currentChildIndex > 0)) {
			StringBuilder retval = new StringBuilder();
			for (int i = 0; i < depth; i++) retval.append(" ");
			retval.append(this.getChild(this.currentChildIndex-1).toStringWithAssignments());
			this.logDerivationTracking(retval.toString());
		}
		
		if (status != Status.SUCCESS) {
			this.currentChildIndex = 0;

			// It was like this but this is wrong because if this or node is not
			// on entry and yet one of the indexed rule fails on entry, the status
			// will inherit the status of the entry-failed rule. As a result, this
			// or node will get entry fail instead of normal fail - KL Ong /10/15/92
			if (this.isEntry)
				status = Status.ENTRY_FAIL;

			this.isEntry = true;
		} else {
			this.isEntry = false;
			// If we succeeded, we undo the increment of the rule index
			this.currentChildIndex--;	
		}
	    //}

		return status;
	}
	
	public void cleanUpData() {
		if (this.relation != null)
			this.relation.cleanUp();
	}
	
	public void deleteRelationsAndCursors() {
		if (this.relation != null) {
			this.relation.removeAllTuples();
			this.relation.commit();

			this.relationManager.deleteDerivedRelation((DerivedRelation)this.relation);
			this.relation = null;
		}

		super.deleteRelationsAndCursors();

		if (this.hasChildren())
			for (AndNode andNode : this.getChildren())
				andNode.deleteRelationsAndCursors();
	}
	
	public void partialCleanUp() {
		this.cleanUp();
	}
	
	public void getVariables(VariableList variableList) {
		for (int i = this.getNumberOfChildren() - 1; i >= 0; i--) {
			this.getChild(i).getVariables(variableList);
			this.getChild(i).getBodyVariables(variableList);
		}		
		Utilities.getVariables(this.getArguments(), variableList);
	}
	
	public String toString() { return toStringNode(); }

	protected boolean isDistinctAggregate(int index) {
		return (this.aggregateInfos.length > index 
				&& this.aggregateInfos[index].aggregateType == AggregateFunctionType.COUNT_DISTINCT);
	}
	
	@Override
	public Cursor<?> getQueryFormCursor() {
		return this.database.getCursorManager().createAggregateCursor(this.relation, this.numberOfKeyColumns, null, this.aggregateInfos, this.typeManager);
	}
	
	@Override
	public void attachContext(DeALSContext deALSContext) {
		super.attachContext(deALSContext);
		this.database = deALSContext.getDatabase();
	}
}
