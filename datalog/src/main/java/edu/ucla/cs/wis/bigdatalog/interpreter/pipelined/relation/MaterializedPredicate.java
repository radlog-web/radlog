package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.Node;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class MaterializedPredicate extends RelationNode {
	protected MaterializedRule materializedRule;
	protected String localRelationName;
	protected Database database;
	
	public MaterializedPredicate(String name, NodeArguments args, Binding binding, VariableList freeVariables) {
		super(name, args, binding, freeVariables);
		this.materializedRule = null;
		this.localRelationName = null;		
	}

	public MaterializedRule getMaterializedRule() { return this.materializedRule; }

	public Status getTuple() {
		if (this.isEntry)
			this.materializedRule.materializeRelation();

		return super.getTuple();
	}

	@Override
	public boolean initialize() {
		if (!super.initialize())
			return false;
		
		if ((this.materializedRule != null)
				&& this.materializedRule.initialize()
				&& ((this.relation = this.relationManager.getRelation(this.localRelationName, this.arity)) != null)
				&& ((this.boundColumns == null) || 
						(this.boundColumns.length == 0) || 
						(this.relation.addSecondaryIndex(this.boundColumns) != null))
						&& ((this.cursor = this.database.getCursorManager().createCursor(this.relation, this.boundColumns)) != null)) {
			this.determineMatchFreeColumns();
			return true;
		}

		return false;
	}

	public void setMaterializedRule(MaterializedRule rule) {
		if (this.materializedRule != null)
			throw new InterpreterException("Materialized rule already exists " + this.materializedRule.getPredicateName());

		this.materializedRule = rule;
		this.localRelationName = this.materializedRule.getRelationName();
	}

	public void deleteRelationsAndCursors() {
		super.deleteRelationsAndCursors();

		if (this.materializedRule != null)
			this.materializedRule.deleteRelationsAndCursors();
	}

	public void cleanUp() {
		super.cleanUp();

		if (this.materializedRule != null)
			this.materializedRule.cleanUp();
	}

	public void partialCleanUp() {
		//super.partialCleanUp();
		this.baseNodeCleanUp();
		//this.unsetFreeVariables();
		this.freeVariableList.makeFree();
		//this.deleteFilterValuesOnBackTrack();

		if (this.materializedRule != null)
			this.materializedRule.partialCleanUp();
	}

	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(super.toString());
		displayIndentLevel++;

		if (this.materializedRule != null) {
			output.append(Node.toStringIndent());
			output.append(this.materializedRule.toString());
		}

		displayIndentLevel--;
		return output.toString();
	}

	@Override
	public String toStringTree() {
		StringBuilder output = new StringBuilder();
		output.append(Node.toStringIndent());
		output.append(super.toString());
		displayIndentLevel++;

		if (this.materializedRule != null) {
			output.append(Node.toStringIndent());
			output.append(this.materializedRule.toStringTree());
		}

		displayIndentLevel--;
		return output.toString();
	}
	
	@Override
	public MaterializedPredicate copy(ProgramContext programContext) {
		MaterializedPredicate copy = new MaterializedPredicate(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		
		programContext.getNodeMapping().put(this, copy);
		
		//programContext.pushArgumentContext();
		
		copy.materializedRule = this.materializedRule.copy(programContext);
		copy.localRelationName = new String(this.localRelationName);
		return copy;
	}
	
	@Override
	public void attachContext(DeALSContext deALSContext) {
		super.attachContext(deALSContext);
		this.database = deALSContext.getDatabase();
		this.materializedRule.attachContext(deALSContext);
	}
}
