package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.xy;

import java.util.LinkedList;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.compiler.type.TypeInferrer;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYPredicateType;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.RecursiveCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.XYCursor;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.exception.ProgramGeneratorException;
import edu.ucla.cs.wis.bigdatalog.interpreter.EvaluationType;
import edu.ucla.cs.wis.bigdatalog.interpreter.Interpreter;
import edu.ucla.cs.wis.bigdatalog.interpreter.Node;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.Utilities;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.AndNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.RecursiveLiteral;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueBaseNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueRuleType;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.IMutualClique;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.statistics.StageStatisticsManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.RelationNode;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class XYCliqueNode extends CliqueBaseNode {	

	public int identicalLiteralIndex;
	public int orderedCliqueIndex;   

	protected OrNode xRulesOrNode;
	protected OrNode yRulesOrNode;
	protected OrNode copyRulesOrNode;
	protected OrNode deleteRulesOrNode;
	
	protected int expectedStage;
	protected XYRecursiveOrNode producer;
	protected Tuple returnedTuple;
	protected Database database;
	protected Interpreter interpreter;

	public XYCliqueNode(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables, 
			String recursiveRelationName, boolean isSharable) {
		super(predicateName, args, binding, freeVariables, recursiveRelationName, isSharable);
		this.evaluationType = EvaluationType.XY;
	}
	
	public void setExpectedStage(int stage) {
		this.expectedStage = stage;
	}

	public int getExpectedStage() { return this.expectedStage; }

	public void setProducer(XYRecursiveOrNode xyRecursiveOrNode) {
		this.producer = xyRecursiveOrNode;
	}

	public XYRecursiveOrNode getProducer() { return this.producer; }
	
	public OrNode getXRulesOrNode() { return this.xRulesOrNode; }
	
	public OrNode getYRulesOrNode() { return this.yRulesOrNode; }
	
	public OrNode getCopyRulesOrNode() { return this.copyRulesOrNode; }
	
	public OrNode getDeleteRulesOrNode() { return this.deleteRulesOrNode; }
	
	protected void initializeIndex() {
		AndNode deleteAndNode = this.deleteRulesOrNode.getChild(0);
		int[] indexedColumns = new int[this.identicalLiteralIndex + 1];
		// delete_rule <- delete1, ..., deleteN, identicalliteral, rest-literals.

		// for each negated literal, map its arg to the arg of the clique, and create index.
		NodeArguments xyArgs = deleteAndNode.getChild(this.identicalLiteralIndex).getArguments();
		for (int i = 0; i < this.identicalLiteralIndex; i++) {
			int count = 0;
			NodeArguments negatedArgs = deleteAndNode.getChild(i).getArguments();

			for (int j = 0; j < negatedArgs.size(); j++) {
				for (int k = 0; k < xyArgs.size(); k++) {
					if (Utilities.isMatch(negatedArgs.get(j), xyArgs.get(k))) {
						indexedColumns[count++] = k;
						break;
					}
				}
			}
			this.recursiveRelation.addSecondaryIndex(indexedColumns);
		}
	}
	
	public void addXRulesOrNode(OrNode node) {
		if (this.xRulesOrNode != null) {
			this.logError("XY Clique's X rules already exist for " + this.predicateName);
			throw new ProgramGeneratorException("XY Clique's X rules already exist for " + this.predicateName);
		}

		this.xRulesOrNode = node;
	}
	
	public void addYRulesOrNode(OrNode node) {
		if (this.yRulesOrNode != null) {
			this.logError("XY Clique's Y rules already exist for " + this.predicateName);
			throw new ProgramGeneratorException("XY Clique's Y rules already exist for " + this.predicateName);
		}
		
		this.yRulesOrNode = node;
	}
	
	public void addCopyRulesOrNode(OrNode node) {
		if (this.copyRulesOrNode != null) {
			this.logError("XY Clique's Copy rules already exist for {}", this.predicateName);
			throw new ProgramGeneratorException("XYClique's Copy rules already exist for " + this.predicateName);
		}
		
		this.copyRulesOrNode = node;
	}
	
	public void addDeleteRulesOrNode(OrNode node) {
		if (this.deleteRulesOrNode != null) {
			this.logError("XY Clique's Delete rules already exist for {}", this.predicateName);
			throw new ProgramGeneratorException("XYClique's Delete rules already exist for " + this.predicateName);
		}
		
		this.deleteRulesOrNode = node;
	}
	
	@Override
	public DataType[] getSchema() {
		DataType[] schema = TypeInferrer.getSchema(this.getArguments());
		DataType[] realSchema = new DataType[schema.length + 1];
		int i;
		for (i = 0; i < schema.length; i++)
			realSchema[i] = schema[i];
		
		realSchema[i] = DataType.INT;  // for stage
		
		return realSchema;
	}

	@Override
	public boolean initialize() {
		if (DEBUG)
			this.logTrace("Entering initialize");
		
		boolean status = false;

		if (!this.initialized) {
			if (this.recursiveRelationName != null) {			
				this.recursiveRelation = this.database.getRelationManager().
						createRecursiveRelation(this.recursiveRelationName, this.getSchema(), true);

				if (this.deleteRulesOrNode.getNumberOfChildren() == 0)
					super.initializeIndex();
				else 
					this.initializeIndex();

				this.fixpointCursor = this.database.getCursorManager().createXYCursor(this.recursiveRelation, null, 
						this.deALSContext, this.database);
				
				//   Recursive literals will be initialized to point to the right relation.
				if ((this.exitRulesOrNode != null)
						&& ((status = this.exitRulesOrNode.initialize()))
						&& (this.xRulesOrNode != null)
						&& ((status = this.xRulesOrNode.initialize()))
						&& (this.yRulesOrNode != null)
						&& ((status = this.yRulesOrNode.initialize()))
						&& (this.copyRulesOrNode != null)
						&& ((status = this.copyRulesOrNode.initialize()))
						&& (this.deleteRulesOrNode != null)
						&& ((status = this.deleteRulesOrNode.initialize()))) {
					if (status)
						this.initialized = true;
				}

				// Step 3. initialize stage variable - stage variable always starts from 0. 
				this.stage = 0;
				this.expectedStage = 0;
				this.numberOfDeltaFactsByIteration = new LinkedList<>();
				this.numberOfGeneratedFactsByIteration = new LinkedList<>();
				this.numberOfGeneratedFactsThisIteration			= 0;
				this.producer = null;
				this.isCleanedUp = false;

				//Part 4. sort mutual clique list according to primed predicate order
				MutualXYCliqueNode mutualXYClique1, mutualXYClique2;

				for (int i = 0; i < this.mutualCliqueList.size() - 1; i++) {
					mutualXYClique1 = (MutualXYCliqueNode)this.mutualCliqueList.get(i);
					for (int j = i + 1; j < this.mutualCliqueList.size(); j++) {
						mutualXYClique2 = (MutualXYCliqueNode)this.mutualCliqueList.get(j);
						if (mutualXYClique1.orderedCliqueIndex > mutualXYClique2.orderedCliqueIndex) {
							IMutualClique temp = this.mutualCliqueList.get(i);
							this.mutualCliqueList.set(i, this.mutualCliqueList.get(j));
							this.mutualCliqueList.set(j, temp);
						}
					}
				}

				for (IMutualClique mutualClique : this.mutualCliqueList)
					((MutualXYCliqueNode)mutualClique).mainClique = this;
				
				this.returnedTuple = this.recursiveRelation.getEmptyTuple();
				
				if (DEBUG && this.deALSContext.isStatisticsEnabled()) {
					StageStatisticsManager.getInstance().startTracking(this.predicateName, this.getArity(), -1);
				}				
			} else {
				throw new InterpreterException("Can not initialize clique.  No relation " + this.predicateName);
			}
		} else {
			status = true;
		}

		if (DEBUG)
			this.logTrace("Exiting initialize with status = {}", status);
		
		return status;
	}

	public void deleteRelationsAndCursors() {
		if (DEBUG)
			this.logTrace("Entering deleteRelationsAndCursors");
		
		if (this.initialized) {
			if (this.exitRulesOrNode != null)
				this.exitRulesOrNode.deleteRelationsAndCursors();

			if (this.xRulesOrNode != null)
				this.xRulesOrNode.deleteRelationsAndCursors();

			if (this.yRulesOrNode != null)
				this.yRulesOrNode.deleteRelationsAndCursors();
				
			if (this.copyRulesOrNode != null)
				this.copyRulesOrNode.deleteRelationsAndCursors();
			
			if (this.deleteRulesOrNode != null)
				this.deleteRulesOrNode.deleteRelationsAndCursors();
			
			this.database.getRelationManager().deleteRecursiveRelation(this.recursiveRelation);

			this.recursiveRelation	= null;
			this.fixpointCursor		= null;
			this.initialized		= false;
		}
		
		if (DEBUG)
			this.logTrace("Exiting deleteRelationsAndCursors");
	}

	public void cleanUp() {
		if (DEBUG) {
			this.logTrace("Entering cleanUp");			
			this.logTrace(this.toStringNode());
		}
		
		if (DEBUG)
			this.logInfo("******** Performing a Cleanup on the XY clique ********");

		if (!this.isCleanedUp) {
			this.cliqueCleanUp();

			this.backtrackCount = 0;
			//this.stage = 0;
			//this.expectedStage = 0;
					
			// Init the various SemiNaive Cursors that are used
			for (RecursiveLiteral recursiveLiteral : this.recursiveLiteralList)
				((RecursiveCursor)recursiveLiteral.getCursor()).initialize();
			
			for (IMutualClique mutualClique : this.mutualCliqueList)
				mutualClique.getFixpointCursor().initialize();
			
			this.fixpointCursor.initialize();
			this.isCleanedUp = true;
		}
		
		if (DEBUG)
			this.logTrace("Exiting cleanUp");
	}
	
	protected void cliqueCleanUp() {
		super.cliqueCleanUp();
		this.exitRulesOrNode.cleanUp();
		this.xRulesOrNode.cleanUp();		
		this.yRulesOrNode.cleanUp();
		this.copyRulesOrNode.cleanUp();
		this.deleteRulesOrNode.cleanUp();

		this.recursiveRelation.cleanUp();

		this.currentRuleType = CliqueRuleType.EXIT_RULE;
	}
	
	public void materialize(CliqueRuleType cliqueRuleType) {
		if (DEBUG)
			this.logTrace("Entering XYCliqueNode.materialize for {}", cliqueRuleType.name());
		
		MutualXYCliqueNode 	mutualXYClique;
		CliqueRuleType  	nextCliqueRuleType;
		
		this.currentRuleType = CliqueRuleType.RULE_TRANSITION;

		int index = this.orderedCliqueIndex;
		if (this.mutualCliqueList.size() < this.orderedCliqueIndex)
			index = this.mutualCliqueList.size();			
		
		//APS - 1/25/2014 - some rules break this because the mutualcliquelist is too short for the lookup 
		// because the 0 position is often an aggregate clique - therefore, we must reduce the index size so we don't throw exceptions
		// materialize all cliques before this clique for cliqueRuleType if not EXIT_RULE type
		//for (int i = 0; i < this.orderedCliqueIndex /*&& cliqueRuleType != CliqueRuleType.EXIT_RULE*/; i++) {
		for (int i = 0; i < index/* && cliqueRuleType != CliqueRuleType.EXIT_RULE*/; i++) {
			mutualXYClique = (MutualXYCliqueNode)this.mutualCliqueList.get(i);

			if (DEBUG)
				this.logInfo("XY Mutual Clique Materializing: ruletype = {}", cliqueRuleType.name());

			mutualXYClique.materialize(cliqueRuleType);
		}

		nextCliqueRuleType = (cliqueRuleType == CliqueRuleType.X_RULE) ? CliqueRuleType.COPY_RULE : CliqueRuleType.X_RULE;

		if (nextCliqueRuleType == CliqueRuleType.COPY_RULE) 
			this.beginNextPhaseForYRules();
		
		int numberOfCliques = this.mutualCliqueList.size();
		
		// materialize all cliques after this clique for 'nextCliqueRuleType'
		for (int i = numberOfCliques - 1; i >= this.orderedCliqueIndex; i--) {
			mutualXYClique = (MutualXYCliqueNode)this.mutualCliqueList.get(i);

			if (DEBUG)
				this.logInfo("XY Mutual Clique Materializing: ruletype = {}", nextCliqueRuleType.name());
			
			mutualXYClique.materialize(nextCliqueRuleType);
		}

		this.currentRuleType = nextCliqueRuleType;
		
		if (DEBUG)
			this.logTrace("Exiting XYCliqueNode.materialize for {}", cliqueRuleType.name());
	}

	/*********************************************************************************
	1) Before we can change to Y rule from X rule, every mutual clique must reach fixpoint on their X rules.  
	2) Before we can change to X rule from Y rule, every mutual clique must reach fixpoint on their Y rules. 
	 ********************************************************************************/
	public Status generateTuple() {
		if (DEBUG) {
			this.logTrace("Entering XYCliqueNode.generateTuple for {}", this.toStringNode());		
			this.logDebug("XYClique generateTuple for currentRuleType = {}", this.currentRuleType.name());
		}

		Status status = Status.FAIL;
		this.isCleanedUp = false;

		// before churning the rules, see if the tuples we need are already in the sibling relation.
		Pair<Boolean, Status> retvalPair = this.producer.isXYCursorResetRequired(this.expectedStage, this.stage, status);		
		if (retvalPair.getFirst())
			return retvalPair.getSecond(); // this returns the status

		if (this.currentRuleType == CliqueRuleType.EXIT_RULE) {
			status = this.getExitTuple();
			if (status != Status.SUCCESS)
				this.materialize(CliqueRuleType.EXIT_RULE);
		}

		if (this.currentRuleType == CliqueRuleType.COPY_RULE ||
				this.currentRuleType == CliqueRuleType.DELETE_RULE ||
				this.currentRuleType == CliqueRuleType.Y_RULE) {
			status = this.getYTuple();

			if (status != Status.SUCCESS)
				this.materialize(CliqueRuleType.COPY_RULE);
		}

		if (this.currentRuleType == CliqueRuleType.X_RULE) {
			status = this.getXTuple();
			if (status != Status.SUCCESS) {
				if (this.expectedStage == -1 || this.stage < this.expectedStage) {
					this.materialize(CliqueRuleType.X_RULE);
					//this.beginNextPhaseForYRules();
					status = this.getYTuple();
				}
			}
		}

		if (DEBUG)
			this.logTrace("Exiting XYCliqueNode.generateTuple for {} with status = {}", this.toStringNode(), status);
		
		return status;
	}

	public void beginNextPhaseForYRules() {
		if (DEBUG) {
			this.logTrace("Entering XYCliqueNode.beginNextPhaseForYRules");
			this.logInfo("XYClique Beginning next phase for Y rules:  " + this.toStringNode());

			if (this.deALSContext.isStatisticsEnabled())
				StageStatisticsManager.getInstance().stageComplete(this.predicateName);
		}
		
		XYCursor xyCursor;
		XYPredicateType	phase;
		
		//Increase Stage Variable of clique
		this.stage++;
		this.numberOfGeneratedFactsByIteration.add(this.numberOfGeneratedFactsThisIteration);
		this.numberOfGeneratedFactsThisIteration = 0;
		
		// APS 12/5/2013
		// clear aggregate relations, since they are now stateful
		int numberOfTuplesDeleted = 0;
		
		// clear previous stage tuples
		if (this.stage >= 2) {
			numberOfTuplesDeleted = ((XYCursor)this.fixpointCursor).deleteTuplesInStage(this.stage - 2);

			if (DEBUG)
				this.logInfo("Deleted {} tuples.", numberOfTuplesDeleted);
		}
		
		((XYCursor)this.fixpointCursor).beginYPhaseForXYLiteral(XYPredicateType.NEW, this.stage);

		this.numberOfDeltaFactsByIteration.add(((XYCursor)this.fixpointCursor).getIterationSize());

		// For each literal and recursive-or-node (i.e., producer) that points to this.recursiveRelation, reinitialize it.
		NodeList<RelationNode> recursiveLiteralList = this.interpreter.getXYLiteralListManager()
				.getXYLiteralList(this.getRecursiveRelationName(), this.getArity());

		for (RelationNode relationNode : recursiveLiteralList) {
			xyCursor = (XYCursor)relationNode.getCursor();
			
			if (relationNode.getXYPredicateType() == XYPredicateType.NEW)
				phase = XYPredicateType.NEW;
			else
				phase = XYPredicateType.OLD;

			if (relationNode instanceof XYRecursiveOrNode)
				xyCursor.beginYPhaseForXYOrNode(phase, this.stage);
			else
				xyCursor.beginYPhaseForXYLiteral(phase, this.stage);
		}

		// For each mutual clique, set its expected stage value. This is necessary
		// because stage variable is eliminated and xy-recursive-node has no way to
		// tell if the tuple it returned is of the stage value it is asked for. Here
		// we set the expected stage value of a mutual clique as the stage value of the clique.
		//if (this.getType() == NodeType.XY_CLIQUE_NODE) {
		if (!(this instanceof MutualXYCliqueNode)) {
			for (IMutualClique mutualClique : this.mutualCliqueList) {
				((MutualXYCliqueNode)mutualClique).setExpectedStage(this.stage);
				((MutualXYCliqueNode)mutualClique).beginNextPhaseForYRules();
			}
		}

		if (DEBUG)
			this.logTrace("Exiting XYCliqueNode.beginNextPhaseForYRules");
	}

	public Status getYTuple() {
		if (DEBUG)
			this.logTrace("Entering XYCliqueNode.getYTuple");
		
		Status status = Status.FAIL;

		if (this.currentRuleType == CliqueRuleType.COPY_RULE) {
			status = this.getCopyTuple(this.copyRulesOrNode);

			if (status == Status.SUCCESS) 
				this.currentRuleType = CliqueRuleType.Y_RULE;
			else 
				this.currentRuleType = CliqueRuleType.DELETE_RULE;
		}

		if (status != Status.SUCCESS 
				&& this.currentRuleType == CliqueRuleType.DELETE_RULE) {
			if (this.deleteRulesOrNode.getNumberOfChildren() > 0)
				status = this.getDeleteTuple(this.deleteRulesOrNode);
			this.currentRuleType = CliqueRuleType.Y_RULE;
		}

		if (status != Status.SUCCESS 
				&& this.currentRuleType == CliqueRuleType.Y_RULE) {
			status = this.getSubCliqueTuple(this.yRulesOrNode);
		}

		if (DEBUG) {
			if (status == Status.SUCCESS) 
				this.logTrace("Exiting XYCliqueNode.getYTuple with status = SUCCESS");
			else
				this.logTrace("Exiting XYCliqueNode.getYTuple with status = FAIL");
		}
		
		return status;
	}

	@Override
	public int getAnyTuple(Cursor readCursor, CliqueBaseNode parentClique, Tuple tuple) {
		if (DEBUG)
			this.logTrace("Entering XYCliqueNode.getAnyTuple for {}", this.toStringNode());

		int status = 0;
		if ((status = readCursor.getTuple(tuple)) == 0) {
			while (this.generateTuple() == Status.SUCCESS) {
				((XYCursor)readCursor).refresh();
				if ((status = readCursor.getTuple(tuple))> 0)
					break;
			}
		}
		
		if (DEBUG) {
			if (status > 0)
				this.logTrace("Exiting XYCliqueNode.getAnyTuple with SUCCESS (status = {}, tuple = {})", status, tuple);
			else
				this.logTrace("Exiting XYCliqueNode.getAnyTuple with FAIL (status = {})", status);
		}

		return status;
	}

	protected Status getCopyTuple(OrNode copyOrNode) {
		if (DEBUG)
			this.logTrace("Entering XYCliqueNode.getCopyTuple for {}", copyOrNode);

		Status status = Status.FAIL;

		if ((status = copyOrNode.getTuple()) == Status.SUCCESS)
			this.resetCursorAfterCopyDeleteRule(CliqueRuleType.COPY_RULE);

		if (DEBUG)
			this.logTrace("Exiting XYCliqueNode.getCopyTuple for {} with status = {}", copyOrNode, status);
		
		return status;
	}

	protected Status getDeleteTuple(OrNode deleteOrNode) {
		if (DEBUG)
			this.logTrace("Entering XYCliqueNode.getDeleteTuple for {}", deleteOrNode.toStringNode());

		Status status = Status.SUCCESS;
		OrNode currentLiteral;
		OrNode negatedNode;

		// delete_rule <- delete_literal, identical_literal, rest-literals.
		if (deleteOrNode.hasChildren()) {
			for (AndNode deleteAndNode : deleteOrNode.getChildren()) {
				// evaluate boolean literals -- should like normal and_node.getTuple()
				for (int i = 2; i < deleteAndNode.getNumberOfChildren() 
						&& status == Status.SUCCESS; i++) {
					currentLiteral = deleteAndNode.getChild(i);
					status = currentLiteral.getTuple();
				}
	
				if (status != Status.SUCCESS) {
					// boolean literals fail, so all tuples in relation are to be deleted.
					this.recursiveRelation.cleanUp();
					break;
				}
	
				negatedNode = deleteAndNode.getChild(0);
				while (negatedNode.getTuple() == Status.SUCCESS) 
					this.deleteTuple(deleteAndNode.getArguments());
			}
		}
		
		// APS 12/7/2013 - make it faster
		//this.recursiveRelation.commit();

		this.resetCursorAfterCopyDeleteRule(CliqueRuleType.DELETE_RULE);

		if (DEBUG)
			this.logTrace("Exiting XYCliqueNode.getDeleteTuple for {} with status = {}", deleteOrNode.toStringNode(), status);

		return status;
	}

	protected Status getXTuple() {
		if (DEBUG)
			this.logTrace("Entering getXTuple for {}", this.toStringNode());
				
		Status status = this.getSubCliqueTuple(this.xRulesOrNode);

		if (DEBUG)
			this.logTrace("Exiting getXTuple for {} with status = {}", this.toStringNode(), status);
		
		return status;
	}
	
	protected Status getSubCliqueTuple(OrNode subClique) {
		if (DEBUG)
			this.logTrace("Entering XYCliqueNode.getSubCliqueTuple for {}", subClique.toStringNode());
		
		Status status = Status.FAIL;

		while (((status = subClique.getTuple()) == Status.SUCCESS) 
				&& (!this.addTuple()));

		if (DEBUG)
			this.logTrace("Exiting XYCliqueNode.getSubCliqueTuple for {}", subClique.toStringNode());
				
		return status;
	}

	protected int deleteTuple(NodeArguments arguments) {
		if (DEBUG)
			this.logTrace("Entering XYCliqueNode.deleteTuple");
		
		int					numberDeleted = 0;
		Argument		 	argument;
		Tuple 				tempTuple = new Tuple(this.arguments.size());
		
		for (int i = 0; i < arguments.size(); i++) {
			argument = ((Variable)arguments.get(i)).dereference();
			if (argument.isGround())
				tempTuple.setColumn(i, argument.toDbType(this.typeManager));
		}
		
		if (DEBUG) {
			if (this.arguments.size() > 0)
				this.logDebug("////////// [deleteTuple: ({}) from {}] //////////", tempTuple, this.recursiveRelation.getName());	
		}		
		
		numberDeleted = this.recursiveRelation.removeSimilarTuples(tempTuple);

		if (DEBUG)
			this.logTrace("Exiting XYCliqueNode.deleteTuple with numberDeleted = {}", numberDeleted);
		
		return numberDeleted;
	}

	protected boolean addTuple() {
		if (DEBUG)
			this.logTrace("Entering XYCliqueNode.addTuple");
		
		boolean status;
		// APS 8/18/2013 refactoring for performance
		// adding + 1 and removing Tuple.addColumn() method
		for (int i = 0; i < this.arguments.size(); i++)
			this.returnedTuple.columns[i] = this.getArgumentAsDbType(i);

		this.returnedTuple.columns[this.arguments.size()] = DbInteger.create(this.stage);
		
		this.numberOfGeneratedFactsThisIteration++;
		this.numberOfRecursiveFactsDerived++;
		
		if (((XYCursor)this.fixpointCursor).isDuplicateTupleInSameStage(this.returnedTuple)) {
			status = false;
		} else {
			if (DEBUG) {	
				if (this.arguments.size() > 0)
					this.logDebug("////////// [addTuple: ({}) to {}] //////////", this.returnedTuple, this.recursiveRelation.getName());
			}			

			status = (this.recursiveRelation.add(this.returnedTuple) != null);
			if (status) {
				if (DEBUG && this.deALSContext.isStatisticsEnabled())				
					StageStatisticsManager.getInstance().markTupleAdded(this.predicateName, this.returnedTuple);				
			}
		}

		if (DEBUG) {
			if (status)
				this.logInfo("Tuple {} added to the XY recursive relation '{}'", this.returnedTuple, this.recursiveRelation.getName());
			else
				this.logInfo("Tuple {} not added", this.returnedTuple);
		}
		
		if (DEBUG)
			this.logTrace("Exiting XYCliqueNode.addTuple with status = {}", status);

		return status;
	}

	protected void resetCursorAfterCopyDeleteRule(CliqueRuleType cliqueRuleType) {
		if (DEBUG)
			this.logTrace("Entering XYCliqueNode.resetCursorAfterCopyDeleteRule for {}", cliqueRuleType.name());
		
		XYCursor xyCursor;
		XYPredicateType phase;

		if (cliqueRuleType == CliqueRuleType.COPY_RULE)
			((XYCursor)this.fixpointCursor).resetCursorAfterCopyRule(XYPredicateType.NEW);
		else 
			((XYCursor)this.fixpointCursor).resetCursorAfterDeleteRule(XYPredicateType.NEW);

		NodeList<RelationNode> recursiveLiteralList = this.interpreter.getXYLiteralListManager()
				.getXYLiteralList(this.getRecursiveRelationName(), this.getArity());

		for (RelationNode relationNode : recursiveLiteralList) {
			xyCursor = (XYCursor)relationNode.getCursor();

			if (relationNode.getXYPredicateType() == XYPredicateType.NEW  || relationNode == this.producer)
				phase = XYPredicateType.NEW;
			else
				phase = XYPredicateType.OLD;

			if (cliqueRuleType == CliqueRuleType.COPY_RULE) 
				xyCursor.resetCursorAfterCopyRule(phase); 
			else
				xyCursor.resetCursorAfterDeleteRule(phase);
		}
		
		if (DEBUG)
			this.logTrace("Exiting XYCliqueNode.resetCursorAfterCopyDeleteRule for {}", cliqueRuleType.name());
	}
	
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append(Node.toStringIndent() /*+ "[" + this.hashCode() + "]"*/);
		retval.append(this.toStringNode());

		displayIndentLevel++;

		retval.append(Node.toStringIndent());
		retval.append("Exit Rules: ");
		
		if (this.exitRulesOrNode != null) {
			displayIndentLevel++;
			retval.append(this.exitRulesOrNode.toStringTree());
			displayIndentLevel--;
		}

		retval.append(Node.toStringIndent());
		retval.append("X Rules: ");
		
		if (this.xRulesOrNode != null) {
			displayIndentLevel++;
			retval.append(this.xRulesOrNode.toStringTree());
			displayIndentLevel--;
		}
	
		if (this.copyRulesOrNode != null) {
			retval.append(Node.toStringIndent());	
			retval.append("Copy Rules: ");
			
			displayIndentLevel++;
			retval.append(this.copyRulesOrNode.toStringTree());
			displayIndentLevel--;
		}

		if (this.deleteRulesOrNode != null) {
			retval.append(Node.toStringIndent());	
			retval.append("Delete Rules: ");
			
			displayIndentLevel++;
			retval.append(this.deleteRulesOrNode.toStringTree());
			displayIndentLevel--;
		}

		if (this.yRulesOrNode != null) {
			retval.append(Node.toStringIndent());
			retval.append("Y Rules:");
			
			displayIndentLevel++;
			retval.append(this.yRulesOrNode.toStringTree());
			displayIndentLevel--;
		}

		displayIndentLevel--;
		return retval.toString();
	}
	
	@Override
	public XYCliqueNode copy(ProgramContext programContext) {
		if (programContext.getCliqueMapping().containsKey(this))
			return (XYCliqueNode) programContext.getCliqueMapping().get(this);
		
		XYCliqueNode copy = new XYCliqueNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList), 
				new String(this.recursiveRelationName),
				this.isSharable);
		
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

		//copy.recursiveLiteralList
		//copy.mutualCliqueList

		if (this.producer != null)
			copy.producer = this.producer.copy(programContext);
		
		for (IMutualClique mc : this.mutualCliqueList)
			copy.mutualCliqueList.add((IMutualClique) programContext.getCliqueMapping().get(mc));
		
		for (RecursiveLiteral rl : this.recursiveLiteralList)
			copy.recursiveLiteralList.add((RecursiveLiteral) programContext.getNodeMapping().get(rl));

		return copy;
	}
	
	@Override
	public void attachContext(DeALSContext deALSContext) {
		super.attachContext(deALSContext);
		this.copyRulesOrNode.attachContext(deALSContext);
		this.xRulesOrNode.attachContext(deALSContext);
		this.yRulesOrNode.attachContext(deALSContext);
		this.deleteRulesOrNode.attachContext(deALSContext);
		this.database = deALSContext.getDatabase();
		this.interpreter = deALSContext.getInterpreter();
	}
	
}
