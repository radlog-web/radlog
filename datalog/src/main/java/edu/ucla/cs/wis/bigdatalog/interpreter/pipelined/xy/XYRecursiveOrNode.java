package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.xy;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.compiler.CompilerException;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYPredicateType;
import edu.ucla.cs.wis.bigdatalog.database.cursor.XYCursor;
import edu.ucla.cs.wis.bigdatalog.interpreter.Interpreter;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.RecursiveOrNode;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class XYRecursiveOrNode 
	extends RecursiveOrNode {
	protected BindingType xyStageBinding;
	protected XYStageVariableNode xyStageVariableNode;
	protected Interpreter interpreter;
	
	public XYRecursiveOrNode(String predicateName, NodeArguments args, Binding binding, 
			BindingType xyStageBinding, VariableList freeVariables) {
		super(predicateName, args, binding, freeVariables);

		//nodes pointing to a XYMutualClique have no XYStageVariableNode
		this.xyStageVariableNode = null;

		//xy stage variable is eliminated, but its binding info is retained here. This value is used in getTuple() to decide the
		//highest stage a clique can ascend to.
		this.xyStageBinding = xyStageBinding;
	}

	public XYStageVariableNode getXYStageNode() { return this.xyStageVariableNode; }

	public void setXYStageNode(XYStageVariableNode node) {
		// APS 8/1/2013 - XY Mutual Cliques arent' supposed to have stagevariables
		// we leave them anyways for the case of rules like 'floyd_results(A,B,C).
		this.xyStageVariableNode = node;
		if (node != null)
			this.xyStageBinding = node.getBinding(0);
	}

	@Override
	public boolean initialize() {
		boolean initialized = true;

		if (this.clique != null)
			initialized = this.clique.initialize();

		if (initialized) {
			//this.relation = Runtime.getDatabaseManager().getActiveDatabase().getRelationManager()
			//		.createDerivedRelation(this.recursiveRelationName, this.getSchema());
			this.relation = relationManager	.createRecursiveRelation(this.recursiveRelationName, this.getXYSchema(), true);
			this.cursor = this.database.getCursorManager().createXYCursor(this.relation, this.boundColumns, this.deALSContext, this.database);

			this.capturedTuple = this.relation.getEmptyTuple();
			this.interpreter.getXYLiteralListManager().addXYLiteralList(this, this.getRecursiveRelationName());
		}

		return initialized;
	}

	@Override
	public String toString() {
		StringBuilder retval = new StringBuilder();	
		retval.append(super.toString());
		retval.append("\n");
		for (int i = 0; i < displayIndentLevel; i++)
			retval.append(" ");
		retval.append("/////XY Stage Node[");
		if (this.xyStageVariableNode != null) {			
			retval.append(this.xyStageVariableNode.toString());			
		} else {
			retval.append("NULL");
		}
		retval.append("]/////");
		return retval.toString();
	}

	@Override
	public Status getTuple() {
		Status status = Status.FAIL;

		this.traceGetTupleEntry();

		this.freeVariableList.makeFree();
		
		if (this.isEntry) {
			this.clique.incrementBacktrackCount();
			this.resetCursor();
		}

		if (!this.isEntry && this.hasAllArgumentsBound) {
			// We should not enforce clean-up unless
			// the clique is unsharable - KayLiang Ong
			this.partialCleanUp();
			//status = Status.FAIL;
		} else {
			int getStatus = 0;

			//decide expected-stage-value for XY Clique, i.e., the highest stage a
			//clique/mutual clique can ascend to, according to the following criteria:

			//1. stage var unbound: -1 (no limit)
			//2. stage var bound : bound value in xyStageNode

			//expected stage value for XY Mutual Clique is set in beginNextPhaseForYRules() of xy clique.
			//if (this.clique.getType() == NodeType.XY_CLIQUE_NODE) {
			if (this.clique instanceof XYCliqueNode && !(this.clique instanceof MutualXYCliqueNode)) {
				// APS added on 3/13/2013 - the binding is always free due to removing the XY Variable before adornment
				// therefore check the binding pattern on the stagenode
				if (this.xyStageBinding == BindingType.FREE/* && this.xyStageVariableNode.getBindingPattern().allFree()*/)
					((XYCliqueNode)this.clique).setExpectedStage(-1);
				else
					((XYCliqueNode)this.clique).setExpectedStage(this.xyStageVariableNode.getBoundStageValue()); 
			}

			((XYCliqueNode)this.clique).setProducer(this);

			/*while ((tuple = this.clique.getAnyTuple(this.cursor, this.parentClique)) != null) {
				if ((this.postSelectTuple(tuple.columns) == Status.SUCCESS) 
						&& this.isExpectedStage())
					break;
				this.unsetFreeVariables();
			}*/
			//Tuple tuple = this.cursor.getEmptyTuple();
			int i;
			while ((getStatus = this.clique.getAnyTuple(this.cursor, this.parentClique, this.capturedTuple)) > 0) {
				for (i = 0; i < this.arity; i++)
					if (!this.arguments.innerArguments[i].match(this.capturedTuple.columns[i]))
						break;

				//System.out.println(this.isExpectedStage());				
				if ((i == this.arity) && this.isExpectedStage())
					break;

				this.freeVariableList.makeFree();
			}

			//status = this.setEntryFlagAndGetStatus(tuple != null);
			if (getStatus == 0) {
				this.freeVariableList.makeFree();
				
				if (this.isEntry)
					status = Status.ENTRY_FAIL;

				this.isEntry = true;
				//this.deleteFilterValuesOnBackTrack();
			} else {
				status = Status.SUCCESS;
				this.isEntry = false;
			}
		}

		if (status != Status.SUCCESS)
			this.clique.decrementBacktrackCount();

		this.traceGetTupleExit(status);

		return status;
	}

	public boolean isExpectedStage() {
		if (DEBUG)
			this.logTrace("Entering isExpectedStage");

		boolean status = false;
		int relationStage = ((XYCursor)this.getCursor()).XYStage;
		int expectedStage = 0; // APS initialized 3/13/2013

		if (this.xyStageVariableNode != null) {
			expectedStage = this.xyStageVariableNode.xyStage();
			if ((expectedStage == -1) || (expectedStage == relationStage))
				status = true;
			//} else if (this.clique.getType() == NodeType.XY_CLIQUE_NODE) {
		} else if ((this.clique instanceof XYCliqueNode) && !(this.clique instanceof MutualXYCliqueNode)) {
			status = true;
		} else {
			// MUTUAL_XY_CLIQUE
			expectedStage = ((XYCliqueNode)this.clique).getExpectedStage();

			if ((expectedStage < relationStage) ||
					((expectedStage == 0) && (this.getXYPredicateType() == XYPredicateType.OLD))) {
				this.logError("isExpectedStage() failed.");
				throw new CompilerException("isExpectedStage() failed.");
			}

			if (this.getXYPredicateType() == XYPredicateType.NEW)
				status = (relationStage == expectedStage);
			else
				status = (relationStage == expectedStage - 1);
		}

		if (DEBUG) {
			if (!status) {
				this.logDebug("***Unexpected XY Clique Stage Fail.****");
				this.logDebug("relationStage = {} expectedStage = {}", relationStage, expectedStage);
			}
		}

		if (DEBUG)
			this.logTrace("Exiting isExpectedStage with status = {}", status);

		return status;
	}

	//This function is called at the beginning of generateTuple(), i.e., we've
	//reached the end of the reading relation, and we are trying to get a new tuple
	//from the underlying rules. Before that, we need to look at the sibling
	//relation to see if the tuples we intend to generate are already there. If
	//that's the case, we swap the reading relation to the sibling relation instead
	//of churning the rules.
	public Pair<Boolean, Status> isXYCursorResetRequired(int expectedStageOfNew, int cliqueStage, Status status) {
		if (DEBUG)
			this.logTrace("Entering isXYCursorResetRequired for expectedStageOfNew = {}, cliqueStage = {}", expectedStageOfNew, cliqueStage);

		int currentRelationStage = ((XYCursor)this.cursor).XYStage;
		int siblingRelationStage = ((XYCursor)this.cursor).siblingXYStage;
		int expectedStage;

		//adjust expected stage value for mutual xy clique. 
		//for xy clique, expected-stage-value is either -1 or a bound value.
		//if ((this.clique.getType() == NodeType.MUTUAL_XY_CLIQUE_NODE)
		if ((this.clique instanceof MutualXYCliqueNode)
				&& (this.getXYPredicateType() == XYPredicateType.OLD))
			expectedStage = expectedStageOfNew - 1;
		else
			expectedStage = expectedStageOfNew;

		if (DEBUG)
			this.logDebug("currentRelationStage = {}, siblingRelationStage = {}, expectedStage = {}, cliqueStage = {}",
					currentRelationStage, siblingRelationStage, expectedStage, cliqueStage);
		
		//System.out.println("currentRelationStage = "+currentRelationStage+", siblingRelationStage = "+siblingRelationStage+", expectedStage = "+expectedStage+", cliqueStage = "+cliqueStage);

		//if the reading relation is the same as the last writing relation, and the
		//reading relation is ranked as same as or below the expected stage, we don't
		//have to do anything here.
		if ((currentRelationStage == cliqueStage) && 
				(expectedStage < 0 || currentRelationStage <= expectedStage))
			return new Pair<>(false, status);

		//if we've already reached the end of a relation whose stage is expected, we
		//fail. There's no need to go up or down, because their stages won't match.
		if (currentRelationStage == expectedStage) {
			status = Status.FAIL;
		} else if (siblingRelationStage == expectedStage || 
				expectedStage < 0 && siblingRelationStage > currentRelationStage) {
			//for mutual recursive node, if the expected stage can be found in sibling
			//relation, swap; or for xy recursive node, if the sibling relation is of a
			//higher stage, go up.

			((XYCursor)this.cursor).switchCursor();

			status = Status.SUCCESS;
		} else {
			this.logError("isXYCursorResetRequired(): unexpected situation");
			throw new CompilerException("isXYCursorResetRequired(): unexpected situation");
		}

		if (DEBUG)
			this.logTrace("Exiting isXYCursorResetRequired");

		return new Pair<>(true, status);
	}

	private DataType[] getXYSchema() {
		DataType[] schema = this.getSchema();
		DataType[] realSchema = new DataType[schema.length + 1];
		int i;
		for (i = 0; i < schema.length; i++)
			realSchema[i] = schema[i];

		realSchema[i] = DataType.INT;  // for stage

		return realSchema;
	}

	@Override
	public XYRecursiveOrNode copy(ProgramContext programContext) {
		if (programContext.getNodeMapping().containsKey(this))
			return (XYRecursiveOrNode) programContext.getNodeMapping().get(this);
		
		XYRecursiveOrNode copy = new XYRecursiveOrNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				this.xyStageBinding, 
				programContext.copyVariableList(this.freeVariableList));
		
		programContext.getNodeMapping().put(this, copy);
		
		copy.clique = this.clique.copy(programContext);
		if (this.clique == this.parentClique)
			copy.parentClique = copy.clique;
		else
			copy.parentClique = this.parentClique.copy(programContext);
		
		copy.executionMode = this.executionMode;
		copy.recursiveRelationName = this.recursiveRelationName;
		copy.xyPredicateType = this.xyPredicateType;
		
		if (this.xyStageVariableNode != null)
			copy.xyStageVariableNode = this.xyStageVariableNode.copy(programContext);
			
		return copy;
	}
	
	@Override
	public void attachContext(DeALSContext deALSContext) {
		super.attachContext(deALSContext);
		this.interpreter = deALSContext.getInterpreter();
		if (this.xyStageVariableNode != null)
			this.xyStageVariableNode.attachContext(deALSContext);
	}
}
