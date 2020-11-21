package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive;

import java.util.LinkedList;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.FixpointCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.NaiveCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.IndexCursor;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreConfiguration;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreManager.TupleStoreType;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.EvaluationType;
import edu.ucla.cs.wis.bigdatalog.interpreter.Node;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.RecursiveLiteral;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.fs.EMSNCliqueNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.fs.FSCliqueNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.fs.MutualEMSNCliqueNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.statistics.StageStatisticsManager;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class CliqueNode 
	extends CliqueBaseNode 
	implements IClique {	
		
	protected OrNode 	recursiveRulesOrNode;
	protected boolean	isFixpointReached;
	protected Database  database;
		
	public CliqueNode(String predicateName, NodeArguments args, Binding binding,
			VariableList freeVariables, String recursiveRelationName, boolean isSharable) {
		super(predicateName, args, binding, freeVariables, recursiveRelationName, isSharable);

		this.exitRulesOrNode			= null;
		this.recursiveRulesOrNode		= null;
		this.isFixpointReached 			= false;
		this.evaluationType 			= EvaluationType.SemiNaive;
	}

	public OrNode getRecursiveRulesOrNode() { return this.recursiveRulesOrNode; }
	
	public void addRecursiveRulesOrNode(OrNode node) {
		this.recursiveRulesOrNode = node;
	}

	@Override
	public boolean initialize() {
		boolean status = false;

		if (!this.initialized) {
			if (this.recursiveRelationName != null) {
				TupleStoreConfiguration tsc = new TupleStoreConfiguration(TupleStoreType.UnorderedHeap);
				this.recursiveRelation = this.database.getRelationManager().createRecursiveRelation(this.recursiveRelationName, 
						this.getSchema(), tsc, true);
				
				this.initializeIndex();

				this.fixpointCursor = this.database.getCursorManager().createSemiNaiveCursor(this.recursiveRelation, null);

				if ((this.exitRulesOrNode != null) 
						&& (status = this.exitRulesOrNode.initialize())
						&& (this.recursiveRulesOrNode != null) 
						&& (status = this.recursiveRulesOrNode.initialize())) {						 
					this.initialized = true;
				}

				this.isCleanedUp = false;
				this.stage = 0;
				this.numberOfDeltaFactsByIteration = new LinkedList<>();
				this.numberOfGeneratedFactsByIteration = new LinkedList<>();
				this.numberOfGeneratedFactsThisIteration	= 0;
				this.returnedTuple = this.recursiveRelation.getEmptyTuple();
			} else {
				this.logError("Can not initialize clique no relation {}", this.predicateName);
				throw new InterpreterException("Can not initialize clique no relation " + this.predicateName);
			}
		} else {
			status = true;
		}

		return status;
	}

	protected boolean addTuple() {
		if (DEBUG)
			this.logTrace("Entering addTuple");
		
		boolean	status = false;

		for (int i = 0; i < this.arity; i++)
			this.returnedTuple.columns[i] = this.getArgumentAsDbType(i);

		if (DEBUG) {
			if (this.arguments.size() > 0) 
				this.logDebug("////////// [addTuple: {}] //////////", this.returnedTuple);			
		}
		
		this.numberOfGeneratedFactsThisIteration++;
		
		status = (this.recursiveRelation.add(this.returnedTuple) != null);

		if (DEBUG) {
			if (status)
				this.logDebug("{}{} added to the recursive relation.", this.recursiveRelation.getName(), this.returnedTuple);
			else
				this.logDebug("{} not added to {}", this.returnedTuple, this.recursiveRelation.getName());
		}
		
		/*if (!status)
			//System.out.println(this.recursiveRelation.getName() + " " + this.returnedTuple + " added to the recursive relation.");
		//else
			System.out.println(this.returnedTuple + " not added to " + this.recursiveRelation.getName());
		*/
		if (status) {
			if (DEBUG && this.deALSContext.isStatisticsEnabled() && StageStatisticsManager.isValidPredicate(this.predicateName))			
				StageStatisticsManager.getInstance().markTupleAdded(this.predicateName, this.returnedTuple);
			
			this.numberOfRecursiveFactsDerived++;
		}
		
		if (DEBUG)
			this.logTrace("Exiting addTuple with status = {}", status);

		return status;
	}

	@Override
	public int getAnyTuple(Cursor readCursor, CliqueBaseNode clique, Tuple tuple) {
		if (DEBUG)
			this.logTrace("Entering getAnyTuple for {}", this.toStringNode());

		int status = readCursor.getTuple(tuple);
		if (status == 0) {
			// If we are here, then it implies that we have reached the end of the
			// local recursive relation of clique_ptr. Let us see if we can generate
			// a new local tuple by using our exit and recursive rules. Okay, "grunt" ...

			// Here, if the actual clique is the parent clique, we continue only if we have not reached a fixpoint
			if ((this != clique) || (!this.isFixpointReached)) {
				// Note that generateTuple for mutual clique does not depend on the 'this.fixedpoint' flag
				// we also know that if we have reached a fixed-point on the main parent clique
				// no more new tuples can be generated for the parent predicate even though
				// we may generate new tuples for the mutual clique
				while (clique.generateTuple() == Status.SUCCESS) {
					if (readCursor instanceof IndexCursor)
						((IndexCursor)readCursor).refresh();
					
					status = readCursor.getTuple(tuple);
					if (status > 0)		
						break;
				}
				
				// If we didn't generate any tuple and the getAnyTuple request was originally directed
				// at a subclique (this would be true if (this != clique_ptr)), then we try to
				// generate local tuples hoping that this would generate tuples in the subclique "clique_ptr"
				if (this != clique) {
					while ((status == 0) && !this.isFixpointReached) {
						// It does not matter if the generateTuple on the parent clique will succeed or not
						// because we might generate some tuples for the mutual clique
						this.generateTuple();

						while (true) {
							status = readCursor.getTuple(tuple);
							if (status > 0)
								break;
							if (clique.generateTuple() != Status.SUCCESS)
								break;								
						}
					}
				}
			}
		}

		if (DEBUG) {	
			if (status > 0)
				this.logTrace("Exiting getAnyTuple {} with tuple {} with SUCCESS (status = {})", this.getPredicateNameWithBinding(), tuple, status);				
			else
				this.logTrace("Exiting getAnyTuple {} with FAIL (status = {})", this.getPredicateNameWithBinding(), status);			
		}

		return status;
	}	

	protected Status getRecursiveTuple() {
		if (DEBUG)
			this.logTrace("Entering getRecursiveTuple for {}", this.toStringNode());
				
		Status status = Status.FAIL;
		
		// If we generated a tuple, we better add it in our local recursive
		// relation. If it was a duplicate, we try again, else we are done
		while (((status = this.recursiveRulesOrNode.getTuple()) == Status.SUCCESS) 
				&& (!this.addTuple()));
		
		if (DEBUG)
			this.logTrace("Exiting getRecursiveTuple for {} with status = {}", this.toStringNode(), status);
		
		return status;
	}

	public Status generateTuple() {
		if (DEBUG)
			this.logTrace("Entering generateTuple for {}", this.toStringNode());
		
		Status status = Status.FAIL;

		this.isCleanedUp = false;

		if (this.currentRuleType == CliqueRuleType.EXIT_RULE) {
			status = this.getExitTuple();

			if (status != Status.SUCCESS) {
				this.currentRuleType = CliqueRuleType.RECURSIVE_RULE;
				this.prepareForNextStage();
			}
		}

		if (this.currentRuleType == CliqueRuleType.RECURSIVE_RULE) {
			while (true) {
				status = this.getRecursiveTuple();

				if (status == Status.SUCCESS) {
					break;
				} else if (this.isFixedpointReached()) {
					break;
				} else {
					this.prepareForNextStage();	
				}
			}
		}

		if (DEBUG)
			this.logTrace("Exiting generateTuple for {} with status = {}", this.toStringNode(), status);

		return status;
	}

	protected boolean isFixedpointReached() {
		if (DEBUG)
			this.logTrace("Entering isFixedpointReached");
		
		boolean status = false;

		if (this.fixpointCursor.isFixedPointReached())  {
			boolean fixpointReached = true;
			for (IMutualClique mutualClique : this.getMutualCliqueList()) {
				if (!mutualClique.getFixpointCursor().isFixedPointReached()) {
					fixpointReached = false;
					break;
				}
			}

			if (fixpointReached)
				status = true;
		}

		this.isFixpointReached = status;

		if (DEBUG && this.deALSContext.isInfoEnabled()) {
			if (status) {
				if (this.recursiveRelation != null)
					this.logInfo("****** Fixed Point reached for relation '{}' ******", this.recursiveRelation.getName());
				else
					this.logInfo("****** Fixed Point reached for recursive relation ******");
			} else {
				if (this.recursiveRelation != null)
					this.logInfo("****** Fixed Point NOT reached for relation '{}' ******", this.recursiveRelation.getName());
				else
					this.logInfo("****** Fixed Point NOT reached for recursive relation ******");
			}
		}
		
		if (DEBUG && this.deALSContext.isDebugEnabled())
			if (this.recursiveRelation != null)
				this.logDebug(this.recursiveRelation.toString());
				
		if (DEBUG && this.deALSContext.isDerivationTrackingEnabled() && status) {
			if (this.recursiveRelation != null)
				this.logDerivationTracking("****** Fixed Point reached for relation '{}' ******", this.recursiveRelation.getName());
			else
				this.logDerivationTracking("****** Fixed Point reached for recursive relation ******");
		}
	
		if (DEBUG)
			this.logTrace("Exiting isFixpointReached with status = {}", status);

		return status;
	}

	// added so all subclasses in class hierarchy follow a pattern
	// really needed because cliques with FS aggregates must do more at stage 
	// boundaries than basic cliques
	public void prepareForNextStage() {
		if (DEBUG)
			this.logTrace("Entering prepareForNextStage");
			
		this.beginNextStageForRecursiveCursors();
		
		if (DEBUG && this.deALSContext.isStatisticsEnabled() && StageStatisticsManager.isValidPredicate(this.predicateName))
			StageStatisticsManager.getInstance().stageComplete(this.predicateName);
				
		for (IMutualClique mutualClique : this.mutualCliqueList) {
			if (mutualClique instanceof MutualEMSNCliqueNode)
				((MutualEMSNCliqueNode)mutualClique).prepareForNextStage();
			
			if (DEBUG && this.deALSContext.isDebugEnabled()) {
				this.logDebug("Mutual clique {} contains:", mutualClique.getPredicateName());
				if (mutualClique.getRecursiveRelation() != null)
					this.logDebug(mutualClique.getRecursiveRelation().toString());
				else if (mutualClique instanceof MutualEMSNCliqueNode)
					this.logDebug(((MutualEMSNCliqueNode)mutualClique).aggregateRelation.toString());
			}
		}
		
		if (!(this instanceof FSCliqueNode) && !(this instanceof EMSNCliqueNode))
			this.numberOfDeltaFactsByIteration.add(this.fixpointCursor.getIterationSize());
				
		if (DEBUG) {
			this.logInfo("stage {} [{} - {}]{}", 
					this.stage,
					((NaiveCursor)this.fixpointCursor).baseTupleAddress,
					((NaiveCursor)this.fixpointCursor).endTupleAddress,
					System.currentTimeMillis());
			this.logTrace("Exiting prepareForNextStage");
		}
	}
	
	protected void beginNextStageForRecursiveCursors() {
		if (DEBUG)
			this.logTrace("Entering beginNextStageForRecursiveCursors");

		//System.out.println("total derived tuples this iteration : " + this.numberOfGeneratedFactsThisIteration);
		//System.out.println("total recursive tuples so far: " + this.numberOfRecursiveFactsDerived);
		
		//APS 3/19/2013
		this.stage++;
		this.numberOfGeneratedFactsByIteration.add(this.numberOfGeneratedFactsThisIteration);
		this.numberOfGeneratedFactsThisIteration = 0;
		//this.numberOfRecursiveFactsDerived = 0;
		if (DEBUG)
			this.logInfo("Beginning next stage for cursors for " + this.predicateName);

		Cursor<?> cursor;
		for (RecursiveLiteral recursiveLiteral : this.recursiveLiteralList) {
			cursor = recursiveLiteral.getCursor();
			if (cursor instanceof FixpointCursor)
				((FixpointCursor)cursor).beginNextStage(this.deALSContext, this.stage);
		}

		// APS 2/12/2014 - using fifo queue of eager monotonic
		for (IMutualClique mutualClique : this.mutualCliqueList)
			if (mutualClique.getFixpointCursor() != null)
				mutualClique.getFixpointCursor().beginNextStage(this.deALSContext, this.stage);

		// APS 2/12/2014 - using fifo queue of eager monotonic
		if (this.fixpointCursor != null)
			this.fixpointCursor.beginNextStage(this.deALSContext, this.stage);
		
		if (DEBUG)
			this.logTrace("Exiting beginNextStageForRecursiveCursors");
	}

	public void cleanUp() {
		if (DEBUG)
			this.logTrace("Entering cleanUp for {}", this.toStringNode());

		if (!this.isCleanedUp) {
			this.cliqueCleanUp();

			this.backtrackCount = 0;

			// Init the various SemiNaive Cursors that are used
			for (RecursiveLiteral recursiveLiteral : this.recursiveLiteralList) {
				if (recursiveLiteral.getCursor() instanceof FixpointCursor)
					((FixpointCursor)recursiveLiteral.getCursor()).initialize();
			}			
			
			for (IMutualClique mutualClique : this.mutualCliqueList)
				mutualClique.getFixpointCursor().initialize();
			
			this.fixpointCursor.initialize();
			this.isCleanedUp = true;
		}
				
		if (DEBUG)
			this.logTrace("Exiting cleanUp for {}", this.toStringNode());
	}
	
	protected void cliqueCleanUp() {
		super.cliqueCleanUp();

		if (this.exitRulesOrNode != null)
			this.exitRulesOrNode.cleanUp();

		if (this.recursiveRulesOrNode != null)
			this.recursiveRulesOrNode.cleanUp();

		// APS 2/5/2014 - eager monotonic won't have a recursive relation
		if (this.recursiveRelation != null)
			this.recursiveRelation.cleanUp();

		this.isFixpointReached = false;
		this.currentRuleType = CliqueRuleType.EXIT_RULE;
	}

	public void deleteRelationsAndCursors() {
		if (this.initialized) {
			if (this.exitRulesOrNode != null)
				this.exitRulesOrNode.deleteRelationsAndCursors();

			if (this.recursiveRulesOrNode != null)
				this.recursiveRulesOrNode.deleteRelationsAndCursors();

			// APS 2/5/2014 - eager monotonic won't have a recursive relation
			if (this.recursiveRelation != null)
				this.database.getRelationManager().deleteRecursiveRelation(this.recursiveRelation);

			this.recursiveRelation	= null;
			this.fixpointCursor		= null;
			this.initialized		= false;
		}
	}

	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(Node.toStringIndent());
		output.append(this.toStringNode());

		//displayIndentLevel++;

		//output.append(Node.toStringIndent());
		if ((this.exitRulesOrNode.getClass() != OrNode.class) 
				|| this.exitRulesOrNode.hasChildren()) {
		//if (this.exitRulesOrNode.hasChildren()) {
			output.append("\n");			
			for (int i = 0; i < displayIndentLevel; i++)
				output.append(" ");
		
			output.append("Exit Rules: ");
	
			if (this.exitRulesOrNode != null) {
				displayIndentLevel++;
				output.append(this.exitRulesOrNode.toStringTree());
				displayIndentLevel--;
			}
		}
		
		//output.append(Node.toStringIndent());
		output.append("\n");			
		for (int i = 0; i < displayIndentLevel; i++)
			output.append(" ");
		output.append("Recursive Rules: ");
		
		if (this.recursiveRulesOrNode != null) {
			displayIndentLevel++;
			output.append(this.recursiveRulesOrNode.toStringTree());
			displayIndentLevel--;
		}

		//displayIndentLevel--;
		
		return output.toString();
	}
		
	@Override
	public CliqueNode copy(ProgramContext programContext) {		
		if (programContext.getCliqueMapping().containsKey(this))
			return (CliqueNode) programContext.getCliqueMapping().get(this);
		
		CliqueNode copy = new CliqueNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList), 
				new String(this.recursiveRelationName), 
				this.isSharable);
		
		programContext.getCliqueMapping().put(this, copy);
		
		copy.exitRulesOrNode = this.exitRulesOrNode.copy(programContext);
		copy.recursiveRulesOrNode = this.recursiveRulesOrNode.copy(programContext);
		
		copy.evaluationType = this.evaluationType;
		copy.stage = this.stage;
		
		for (IMutualClique mc : this.mutualCliqueList)
			copy.mutualCliqueList.add((IMutualClique) programContext.getCliqueMapping().get(mc));
		
		for (RecursiveLiteral rl : this.recursiveLiteralList)
			copy.recursiveLiteralList.add((RecursiveLiteral) programContext.getNodeMapping().get(rl));

		return copy;
	}
	
	@Override
	public void attachContext(DeALSContext deALSContext) {
		super.attachContext(deALSContext);
		this.database = deALSContext.getDatabase();
		this.recursiveRulesOrNode.attachContext(deALSContext);
	}
}
