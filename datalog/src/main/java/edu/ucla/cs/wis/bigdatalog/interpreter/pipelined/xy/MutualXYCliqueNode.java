package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.xy;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.FixpointCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.XYCursor;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.RecursiveLiteral;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueBaseNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueRuleType;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.IMutualClique;

public class MutualXYCliqueNode 
	extends XYCliqueNode 
	implements IMutualClique {

	public XYCliqueNode mainClique;

	public MutualXYCliqueNode(String predicateName, NodeArguments args, Binding binding, 
			VariableList freeVariables, String recursiveRelationName) {
		super(predicateName, args, binding, freeVariables, recursiveRelationName, true);
	}
	
	public void cleanUp() {
		if (!this.isCleanedUp) {
			this.cliqueCleanUp();
			this.isCleanedUp = true;
		}
	}

	public void partialCleanUp() {	}

	@Override
	public Status generateTuple() {	
		if (DEBUG)
			this.logTrace("Entering MutualXYClique.generateTuple for {}", this.toStringNode());

		Status status = Status.FAIL;

		this.isCleanedUp = false;

		//If we are not materializing, see if we can swap cursor.
		if (this.producer != null) { 
			Pair<Boolean, Status> retvalPair = this.producer.isXYCursorResetRequired(this.expectedStage, this.stage, status);
			if (retvalPair.getFirst())
				return retvalPair.getSecond(); // this returns the status
		}

		if (this.currentRuleType == CliqueRuleType.EXIT_RULE) {
			status = this.getExitTuple();

			if (status != Status.SUCCESS)
				this.currentRuleType = CliqueRuleType.X_RULE;
		}    

		if (this.currentRuleType == CliqueRuleType.COPY_RULE ||
				this.currentRuleType == CliqueRuleType.DELETE_RULE ||
				this.currentRuleType == CliqueRuleType.Y_RULE) {
			status = this.getYTuple();
			if (status != Status.SUCCESS) 
				this.currentRuleType = CliqueRuleType.RULE_TRANSITION;
		}

		if (this.currentRuleType == CliqueRuleType.X_RULE) {
			status = this.getXTuple();
			if (status != Status.SUCCESS) 
				this.currentRuleType = CliqueRuleType.RULE_TRANSITION;
		}

		if (DEBUG)
			this.logTrace("Exiting MutualXYClique.generateTuple for {} with status = {}", this.toStringNode(), status);

		return status;
	}

	@Override
	public int getAnyTuple(Cursor readCursor, CliqueBaseNode parentClique, Tuple tuple) {
		if (DEBUG)
			this.logTrace("Entering MutualXYClique.getAnyTuple for {}", readCursor.toString());

		int getStatus = readCursor.getTuple(tuple); 
		if (getStatus == 0) {
			XYRecursiveOrNode   	producer    = this.getProducer();
			XYStageVariableNode 	stageNode  = producer.getXYStageNode();
			XYCliqueNode           	mainClique = this.mainClique;
			Status              	status      = Status.SUCCESS;

			if (stageNode != null) {
				if (stageNode.xyStage() > mainClique.getNumberOfIterations()) {
					while (status == Status.SUCCESS && getStatus == 0) {
	
						if (DEBUG)
							this.logInfo("Trying to materialize main clique.");
	
						status = mainClique.generateTuple();
	
						if (DEBUG)
							this.logInfo("Materialing main clique ends.");
	
						if (status == Status.SUCCESS)
							getStatus = readCursor.getTuple(tuple);
					}
					
					this.setProducer(producer);
					// APS 8/2/2013 - this case is necessary to produce results when this mutual clique
					// is new, but the stage variable is old
				} else if (stageNode.xyStage() < this.expectedStage) {
					// switch cursor to read from old stage
					((XYCursor)readCursor).switchCursor();
					
					XYCursor xyCursor = (XYCursor)readCursor;
					if (xyCursor.getIndexCursor() != null) {
						if (xyCursor.isFixedPointReached())
							xyCursor.getIndexCursor().resetCurrentMatches();
					}

					getStatus = readCursor.getTuple(tuple);
				}
			}
		}

		if (DEBUG) {
			if (getStatus > 0)
				this.logTrace("Exiting MutualXYClique.getAnyTuple with SUCCESS (status = {} and tuple = {})", getStatus, tuple);
			else
				this.logTrace("Exiting MutualXYClique.getAnyTuple with FAIL (status = {})", getStatus);
		}

		return getStatus;
	}

	public void materialize(CliqueRuleType ruleType) {
		if (DEBUG)
			this.logTrace("Entering MutualXYClique.materialize for {}", this.toStringNode());
		
		// set the producer as NULL because we are not calling from getTuple of recursive ornode.
		this.producer = null;

		while (this.generateTuple() == Status.SUCCESS);

		this.currentRuleType = (ruleType == CliqueRuleType.X_RULE) ? CliqueRuleType.COPY_RULE : CliqueRuleType.X_RULE;

		if (DEBUG)
			this.logTrace("Exiting MutualXYClique.materialize with currentRuleType = {}", this.currentRuleType.name());
	}
	
	public FixpointCursor getFixpointCursor() { return this.fixpointCursor; }
	
	@Override
	public MutualXYCliqueNode copy(ProgramContext programContext) {
		if (programContext.getCliqueMapping().containsKey(this))
			return (MutualXYCliqueNode) programContext.getCliqueMapping().get(this);
		
		MutualXYCliqueNode copy = new MutualXYCliqueNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList), 
				new String(this.recursiveRelationName));
		
		programContext.getCliqueMapping().put(this, copy);
		
		copy.exitRulesOrNode = this.exitRulesOrNode.copy(programContext);
		copy.xRulesOrNode = this.xRulesOrNode.copy(programContext);
		copy.yRulesOrNode = this.yRulesOrNode.copy(programContext);
		copy.copyRulesOrNode = this.copyRulesOrNode.copy(programContext);
		copy.deleteRulesOrNode = this.deleteRulesOrNode.copy(programContext);
		
		copy.stage = this.stage;
		copy.identicalLiteralIndex = this.identicalLiteralIndex;
		copy.orderedCliqueIndex = this.orderedCliqueIndex;
		copy.expectedStage = this.expectedStage;
		copy.xyPredicateType = this.xyPredicateType;

		if (this.producer != null)
			copy.producer = this.producer.copy(programContext);
		
		if (this.mainClique != null)
			copy.mainClique = this.mainClique.copy(programContext);
		
		for (IMutualClique mc : this.mutualCliqueList)
			copy.mutualCliqueList.add((IMutualClique) programContext.getCliqueMapping().get(mc));
		
		for (RecursiveLiteral rl : this.recursiveLiteralList)
			copy.recursiveLiteralList.add((RecursiveLiteral) programContext.getNodeMapping().get(rl));
				
		//copy.recursiveLiteralList
		//copy.mutualCliqueList
		//copy.producer
		//copy.mainClique
		return copy;
	}
}
