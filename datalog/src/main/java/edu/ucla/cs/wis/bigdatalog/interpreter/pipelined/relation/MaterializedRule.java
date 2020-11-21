package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.database.RelationManager;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.Node;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.AndNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

/*******************************************************************************
 * COMMENTS:	This is a materialized RULE node to be used when we perform a supplementary magic rewrite.
 *		This class is pointed to by a MaterializedPredicate and used exclusively by it. No one else should
 *		use it. Also the function getTuple should NEVER be invoked. What should be invoked instead is
 *		materializeRelation. The calling node, MaterializedPredicate, has a pointer to the exact
 *		same relation which is materialized by this node.  We can do this because we use the argument 'name'
 *		as the name of the materialized relation. 
 * AUTHOR:	Natraj Vidur Arni
 ********************************************************************************/
public class MaterializedRule 
	extends AndNode {

	protected TempInsertNode 	memoNode;
	protected boolean 			isMaterialized;
	protected boolean 			initialized;
	protected boolean 			isCleanedUp;
	protected RelationManager 	relationManager;

	public MaterializedRule(String predicateName, NodeArguments args, Binding binding, String relationName) {
		super(predicateName, args, binding);

		this.initialized = false;
		this.isCleanedUp = true;

		Binding allBoundBinding = new Binding(this.getArity(), BindingType.BOUND);
		this.memoNode = new TempInsertNode(relationName, args.copyExceptVariables(), allBoundBinding);
		this.isMaterialized = false;
	}

	public void materializeRelation() {
		if (DEBUG)
			this.logTrace("Entering MaterializedRule.materializeRelation");
		
		if (!this.isMaterialized) {
			if (DEBUG)
				this.logInfo("Begin Materializing the relation in MaterializedRule {}", this.getPredicateName());			
		
			while (this.getAndNodeTuple() == Status.SUCCESS) {
				this.memoNode.getTuple();
				this.memoNode.getTuple(); // Simulate a backtrack
			}
	
			this.isMaterialized = true;
			this.isCleanedUp = false;
	
			if (DEBUG) {
				this.logInfo("End Materializing the relation in MaterializedRule, {}", this.getPredicateName());
				this.logInfo("The materialized relation is");
				this.logInfo(this.memoNode.getRelation().toString());
			}
		}
		this.logTrace("Exiting MaterializedRule.materializeRelation");
	}

	@Override
	public boolean initialize() {
		if (!this.initialized
				&& (this.memoNode != null)
				&& this.memoNode.initialize()
				&& super.initialize()) {
			this.isMaterialized = false;
			this.initialized = true;
			this.isCleanedUp = true;
		} else {
			this.initialized = false;
		}

		return this.initialized;
	}

	public void deleteRelationsAndCursors() {
		if (this.initialized) {
			this.initialized = false;

			if (this.memoNode != null)
				this.memoNode.deleteRelationsAndCursors();

			super.deleteRelationsAndCursors();
		}
	}

	public Status getTuple() {
		this.logError("MaterializedRule.getTuple() should not be called. implement in subclass.");
		throw new InterpreterException("MaterializedRule.getTuple() should not be called. implement in subclass.");
	}

	public void cleanUp() {
		if (!this.isCleanedUp) {
			super.cleanUp();
			this.memoNode.clearRelation();
			this.memoNode.cleanUp();
			this.isMaterialized = false;
			this.isCleanedUp = true;
		}
	}

	public void partialCleanUp() { }

	public String getRelationName() {
		return this.memoNode.getPredicateName();
	}

	@Override
	public String toStringTree() {
		StringBuilder output = new StringBuilder();
		output.append(this.toString());

		displayIndentLevel++;

		if (this.memoNode != null) {
			output.append(Node.toStringIndent());
			output.append(this.memoNode.toString());
		}

		for (int i = 0; i < this.getNumberOfChildren(); i++)
			output.append(this.getChild(i).toStringTree());

		displayIndentLevel--;
		return output.toString();
	}
	
	@Override
	public MaterializedRule copy(ProgramContext programContext) {
		MaterializedRule copy = new MaterializedRule(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				new String(this.memoNode.getPredicateName()));
		
		programContext.getNodeMapping().put(this, copy);
		
		copy.backtrackMap = this.backtrackMap;
		copy.ruleBacktrackPoint = this.ruleBacktrackPoint;
		
		for (int i = 0; i < this.getNumberOfChildren(); i++)
			copy.addChild(this.getChild(i).copy(programContext));
		
		return copy;
	}
	
	@Override
	public void attachContext(DeALSContext deALSContext) {
		super.attachContext(deALSContext);
		this.relationManager = deALSContext.getDatabase().getRelationManager();
		this.memoNode.attachContext(deALSContext);
	}
}
