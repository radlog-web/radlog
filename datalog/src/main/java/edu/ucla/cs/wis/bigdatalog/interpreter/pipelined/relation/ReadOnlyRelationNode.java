package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class ReadOnlyRelationNode 
	extends RelationNode {

	protected boolean isRecursive;
	protected Database database;
	
	public ReadOnlyRelationNode(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables, 
			boolean isRecursivePredicate) {
		super(predicateName, args, binding, freeVariables);

		this.isRecursive = isRecursivePredicate;
	}

	@Override
	public boolean initialize() {				
		String relationName = this.predicateName;
		if (this.isRecursive) {
			this.relation = this.relationManager.getRecursiveRelation(relationName, this.arity);
		} else {
			//if (relationName.startsWith(AggregateRewriter.STRATIFIED_AGGREGATE_NODE_NAME_PREFIX))
			//	relationName = relationName.substring(0, relationName.lastIndexOf("_"));			
			this.relation = this.relationManager.getDerivedRelation(relationName, this.arity);
		}
		
		if (this.relation != null) {
			this.cursor = this.database.getCursorManager().createCursor(this.relation, this.boundColumns);
			this.capturedTuple = this.relation.getEmptyTuple();
			this.determineMatchFreeColumns();
		}
		
		return (this.cursor != null);
	}

	public Status getTuple() {
		return super.getTuple();	
	}

	public void partialCleanUp() {
		this.baseNodeCleanUp();
		this.freeVariableList.makeFree();
	}
	
	@Override
	public ReadOnlyRelationNode copy(ProgramContext programContext) {
		ReadOnlyRelationNode copy = new ReadOnlyRelationNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList), 
				this.isRecursive);
		programContext.getNodeMapping().put(this, copy);
		return copy;
	}
	
	@Override
	public void attachContext(DeALSContext deALSContext) {
		super.attachContext(deALSContext);
		this.database = deALSContext.getDatabase();
	}
}
