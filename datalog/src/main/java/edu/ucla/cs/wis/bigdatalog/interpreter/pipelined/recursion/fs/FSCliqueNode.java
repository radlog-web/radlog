package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.fs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.AggregateRewriter;
import edu.ucla.cs.wis.bigdatalog.compiler.type.ArgumentType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.RelationManager;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.IndexCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.NaiveCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.EvaluationType;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.AndNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.fs.FSAggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.fs.FSCountKeyValueAggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.AggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.RecursiveLiteral;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueBaseNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueRuleType;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.IMutualClique;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.statistics.StageStatisticsManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.MaterializedPredicate;

// Class for semi-naive evaluation of fs aggregates
// also the base class for all fs evaluation methods
// recursive relation - heap tuplestore
// append only to recursive relation
public class FSCliqueNode 
	extends CliqueNode {
	//APS 3/26/2013 - to track already visited tuples so we can correct counts
	protected ArrayList<FSAggregateRelationNode> fsAggregates;
	
	protected final int[]				keyColumns;			
	protected Tuple						addedTuple;
	protected Tuple						updatedTuple;
	protected int						addedTupleCount;
	protected int 						updatedTupleCount;
	protected int						addUpdatedThisStage;
	protected long						time;
	protected ArgumentType[]			argumentTypeAdornment;
	public	 Relation<AddressedTuple>	aggregateRelation;

	public FSCliqueNode(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables,
			String recursiveRelationName, boolean isSharable, List<ArgumentType> argumentTypeAdornment) {
		super(predicateName, args, binding, freeVariables, recursiveRelationName, isSharable);
						
		// now setup the key columns so we can use them to build an index
		List<Integer> tempKeyColumns = new ArrayList<>();
		
		this.argumentTypeAdornment = new ArgumentType[argumentTypeAdornment.size()];
		for (int i = 0; i < argumentTypeAdornment.size(); i++) {
			this.argumentTypeAdornment[i] = argumentTypeAdornment.get(i);
			if (this.argumentTypeAdornment[i] == ArgumentType.VARIABLE 
					|| this.argumentTypeAdornment[i] == ArgumentType.CONSTANT)
				tempKeyColumns.add(i);
		}
		
		this.keyColumns = new int[tempKeyColumns.size()];
		for (int i = 0; i < tempKeyColumns.size(); i++)
			this.keyColumns[i] = tempKeyColumns.get(i);				
		
		this.evaluationType = EvaluationType.SemiNaive;
	}
	
	public List<ArgumentType> getArgumentTypeAdornment() {
		return Arrays.asList(this.argumentTypeAdornment);
	}

	@Override
	public boolean initialize() {
		boolean status = false;

		if (!this.initialized) {
			if (this.recursiveRelationName != null) {
				this.initializeBookkeepingInfo();
				this.fsAggregates = this.getFSAggregates();			
				
				RelationManager relationManager = this.database.getRelationManager();
				this.recursiveRelation = relationManager.createRecursiveRelation(this.recursiveRelationName, this.getSchema(), false);	

				if ((this.exitRulesOrNode != null) 
						&& (status = this.exitRulesOrNode.initialize())
						&& (this.recursiveRulesOrNode != null) 
						&& (status = this.recursiveRulesOrNode.initialize())) {						 
					this.initialized = true;
				}
							
				String relationName = AggregateRewriter.FS_AGGREGATE_NODE_NAME_PREFIX + this.getPredicateName().substring(0, this.getPredicateName().lastIndexOf("_"));
				int numberOfColumns = this.arity; 
				
				// fscnt + omsn/emsn requires fooling the clique into using a table with extra +1 arity
				for (FSAggregateRelationNode fsAggr : this.fsAggregates) {
					if (fsAggr.matchesRelationName(relationName)) {
						if ((fsAggr instanceof FSCountKeyValueAggregateRelationNode)
							/*|| (fsAggr instanceof FSCountSingleKeyValueFastAggregateRelationNode2)*/) {
							numberOfColumns++;
							break;
						} /*else if (fsAggr instanceof FSManyAggregateRelationNode) {
							FSAggregateType[] aggregateTypes = ((FSManyAggregateRelationNode)fsAggr).getFSAggregateTypes();
							for (FSAggregateType aggregateType : aggregateTypes) {
								if (aggregateType == FSAggregateType.FSCNT)
									numberOfColumns++;
							}							
						}*/
					}
				}
				
				this.aggregateRelation = (Relation<AddressedTuple>) relationManager.getRelation(relationName, numberOfColumns);
				if (this.aggregateRelation == null)
					this.aggregateRelation = (Relation<AddressedTuple>) relationManager.getRelation(relationName, numberOfColumns + 1);
				
				this.fixpointCursor = this.database.getCursorManager().createSemiNaiveFixpointCursor(this.recursiveRelation, null);
				
				this.returnedTuple = this.recursiveRelation.getEmptyTuple();
			} else {
				throw new InterpreterException("Can not initialize clique.  No relation " + this.predicateName);
			}
		} else {
			status = true;
		}

		return status;
	}

	protected void initializeBookkeepingInfo() {
		this.isCleanedUp = false;
		this.stage = 0;
		this.updatedTupleCount = 0;
		this.addedTupleCount = 0;
		this.addUpdatedThisStage = 0;
		this.numberOfDeltaFactsByIteration = new LinkedList<>();
		this.numberOfGeneratedFactsByIteration = new LinkedList<>();
		this.numberOfGeneratedFactsThisIteration = 0;
		this.time = System.currentTimeMillis();
	}
	
	@Override
	protected boolean addTuple() {
		if (DEBUG)
			this.logTrace("Entering addTuple");
		
		for (int i = 0; i < this.arity; i++)
			this.returnedTuple.columns[i] = this.getArgumentAsDbType(i);

		if (DEBUG) {
			if (this.arguments.size() > 0) 
				this.logDebug("////////// [addTuple: {}] //////////", this.returnedTuple);			
		}
		
		this.numberOfRecursiveFactsDerived++;
		this.numberOfGeneratedFactsThisIteration++;
		
		boolean status = (this.recursiveRelation.add(this.returnedTuple, true) != null);
		if (status) {				
			this.addedTupleCount++;
			this.updatedTuple = null;
			this.addedTuple = this.returnedTuple;

			if (DEBUG)
				this.logDebug("{}{} added to the recursive relation", this.recursiveRelation.getName(), this.returnedTuple);
			
			this.addUpdatedThisStage++;
		}	
				
		if (DEBUG) {
			if (!status)
				this.logDebug("Tuple {} not added", this.returnedTuple);				
		
			this.logTrace("Exiting addTuple with status = {}", status);
		}

		return status;
	}
		
	@Override
	public int getAnyTuple(Cursor readCursor, CliqueBaseNode clique, Tuple tuple) {
		if (DEBUG)
			this.logTrace("Entering getAnyTuple for {}", this.toStringNode());

		int status = readCursor.getTuple(tuple);
		if (status == 0) {
			// If we are here, then it implies that we have reached the end of the
			// local recursive relation of the clique. Let us see if we can generate
			// a new local tuple by using our exit and recursive rules. Okay, "grunt" ...

			// Here, if the actual clique is the parent clique, we continue only if we have not reached a fixpoint
			if ((this != clique) || (!this.isFixpointReached)) {
				// Note that generateTuple for mutual clique does not depend on the 'this.isFixpointReached' flag
				// we also know that if we have reached a fixed-point on the main parent clique no more new tuples can be 
				// generated for the parent predicate even though we may generate new tuples for the mutual clique
				
				// APS added 8/7/2013 - when updating tuple in local relation, we cannot get next tuple from readCursor.getTuple() 
				// if it is a ScanCursor, which it needs to be in order to read the tuple that was just generated.
				// Therefore, we need to avoid the cursor and directly select the tuple just updated.
				// APS added 11/20/2013 - when updating tuple in local relation with staged bplustree tuple store, tuples are not in 
				// order, so we need to grab it manually, rather than through the cursor
				while (clique.generateTuple() == Status.SUCCESS) {
					if (this.updatedTuple != null) {
						tuple.setValues(this.updatedTuple);
						this.updatedTuple = null;
						status = 1;
						break;
					} else if (this.addedTuple != null) {
						tuple.setValues(this.addedTuple);
						this.addedTuple = null;
						readCursor.moveNext();
						status = 1;
						break;
					} else {
						if (readCursor instanceof IndexCursor)
							((IndexCursor)readCursor).refresh();
						
						status = readCursor.getTuple(tuple);
						if (status > 0)
							break;
					}
				}

				// If we didn't generate any tuple and the getAnyTuple request was originally directed
				// at a subclique (this would be true if (this != clique_ptr)), then we try to
				// generate local tuples hoping that this would generate tuples in the subclique "clique"
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
	
	@Override
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

				if (status == Status.SUCCESS)
					break;
				
				if (this.isFixedpointReached()) {
					this.prepareStatCountersForNextStage(this.stage);
					break;
				}
				this.prepareForNextStage();	
			}
		}

		if (DEBUG)
			this.logTrace("Exiting generateTuple for {} with status = {}", this.toStringNode(), status);

		return status;
	}
	
	protected int getIterationSize() {
		return this.fixpointCursor.getIterationSize();
	}
	
	// APS added 4/12/2013
	@Override
	public void prepareForNextStage() {
		if (DEBUG)
			this.logTrace("Entering prepareForNextPhase");
		
		this.beginNextStageForRecursiveCursors();
		
		this.numberOfDeltaFactsByIteration.add(this.getIterationSize());
				
		this.prepareStatCountersForNextStage(this.stage - 1);
		
		if (DEBUG && this.deALSContext.isStatisticsEnabled())
			StageStatisticsManager.getInstance().stageComplete(this.predicateName);

		int count = 0;
		for (IMutualClique mutualClique : this.mutualCliqueList) {
			if (mutualClique instanceof MutualEMSNCliqueNode)
				((MutualEMSNCliqueNode)mutualClique).prepareForNextStage();
			
			if (DEBUG) {
				this.logInfo("Mutual clique {} contains:", count);
				if (mutualClique.getRecursiveRelation() != null)
					this.logInfo(mutualClique.getRecursiveRelation().toString());
				else if (mutualClique instanceof MutualEMSNCliqueNode)
					this.logInfo(((MutualEMSNCliqueNode)mutualClique).aggregateRelation.toString());
			}
			count++;
		}
		
		// this ensures non-recursive usage monotonic aggregates functions properly.
		// aggregates are not re-entrant, so without a recursion relation, such as when 
		// a monotonic aggregate is used outside of recursion, the results need to be recalculated.
		for (FSAggregateRelationNode node : this.fsAggregates) {	
			if (!node.isInClique())
				node.cleanUpData();

			//if (node instanceof FSMaxBased2SingleAggregateRelationNode)
			//	((FSMaxBased2SingleAggregateRelationNode)node).incrementStage();
		}

		if (DEBUG)
			this.logTrace("Exiting prepareForNextPhase");
	}
	
	protected void prepareStatCountersForNextStage(int stage) {
		if (DEBUG) {
			if (this.fixpointCursor instanceof NaiveCursor) {
				this.logInfo("Stage {} [{} - {}] added: {}  updated: {} [total this stage: {}] time: {} + # of delta tuples:{}", 
						stage, 
						((NaiveCursor)this.fixpointCursor).baseTupleAddress, 
						((NaiveCursor)this.fixpointCursor).endTupleAddress, 
						this.addedTupleCount, 
						this.updatedTupleCount, 
						this.addUpdatedThisStage, 
						(System.currentTimeMillis() - time), 
						this.numberOfDeltaFactsByIteration);
			} else {
				this.logInfo("Stage {} [{}] added: {}  updated: {} [total this stage: {}] time: {} # of delta tuples:{}", 
						stage, 
						this.recursiveRelation.getTupleStore().getNumberOfTuples(),
						this.addedTupleCount,
						this.updatedTupleCount,
						this.addUpdatedThisStage,
						(System.currentTimeMillis() - time),
						this.numberOfDeltaFactsByIteration);
			}
			this.logInfo("***************************Stage {} Complete***************************", stage);				
		}
		this.time = System.currentTimeMillis();
		this.addUpdatedThisStage = 0;
		this.addedTupleCount = 0;
		this.updatedTupleCount = 0;
	}
	
	// APS added 3/21/2013
	protected ArrayList<FSAggregateRelationNode> getFSAggregates() {
		ArrayList<FSAggregateRelationNode> aggregates = new ArrayList<>();
		this.getFSAggregates(this.recursiveRulesOrNode, aggregates);
		return aggregates;
	}
	
	// APS added 3/21/2013
	private void getFSAggregates(OrNode orNode, ArrayList<FSAggregateRelationNode> aggregates) {
		if (orNode == null)				
			return;
		
		if (orNode instanceof FSAggregateRelationNode) {
			// only grab read nodes
			if (orNode instanceof AggregateRelationNode) {
				if (((AggregateRelationNode)orNode).isReadAggregate())
					aggregates.add((FSAggregateRelationNode)orNode);
			} else if (orNode instanceof FSAggregateRelationNode) {
				aggregates.add((FSAggregateRelationNode) orNode);
			}
		}

		if (orNode.hasChildren())
			for (AndNode node : orNode.getChildren())
				this.getFSAggregates(node, aggregates);
		
		if (orNode instanceof MaterializedPredicate)
			this.getFSAggregates(((MaterializedPredicate)orNode).getMaterializedRule(), aggregates);
				
		return;
	}
	
	// APS added 3/21/2013
	private void getFSAggregates(AndNode andNode, ArrayList<FSAggregateRelationNode> aggregates) {
		for (OrNode orNode : andNode.getChildren()) {
			if (orNode instanceof FSAggregateRelationNode) {
				if (orNode instanceof AggregateRelationNode) {
					// only grab read nodes
					if (((AggregateRelationNode)orNode).isReadAggregate()) {
						aggregates.add((FSAggregateRelationNode)orNode);
						if (!(orNode instanceof CliqueNode))
							this.getFSAggregates(orNode, aggregates);
					}
				} else if (orNode instanceof FSAggregateRelationNode) {
					aggregates.add((FSAggregateRelationNode)orNode);
					if (!(orNode instanceof CliqueNode))
						this.getFSAggregates(orNode, aggregates);
				}
			} else if (orNode instanceof FSAggregateRelationNode) {
				aggregates.add((FSAggregateRelationNode)orNode);
				if (!(orNode instanceof CliqueNode))
					this.getFSAggregates(orNode, aggregates);
			} else {
				this.getFSAggregates(orNode, aggregates);
			}
		}
		return;
	}
	
	@Override
	public FSCliqueNode copy(ProgramContext programContext) {
		if (programContext.getCliqueMapping().containsKey(this))
			return (FSCliqueNode) programContext.getCliqueMapping().get(this);
		
		FSCliqueNode copy = null;
		
		if ((this.getClass() == FSCliqueNode.class)
				|| (this.getClass() == MSNCliqueNode.class)
				|| (this.getClass() == EMSNCliqueNode.class)) {
			copy = FSCliqueNode.createFSCliqueNode(new String(this.predicateName), 
					programContext.copyArguments(this.arguments), 
					this.bindingPattern.copy(), 
					programContext.copyVariableList(this.freeVariableList), 
					new String(this.recursiveRelationName),
					this.isSharable,
					Arrays.asList(this.argumentTypeAdornment),
					EvaluationType.getEvaluationType(this.deALSContext.getConfiguration().getProperty("deals.interpreter.fsclique.evaluationtype").toLowerCase()));
		} else {
			copy = (FSCliqueNode) MutualFSCliqueNode.createMutualFSCliqueNode(new String(this.predicateName), 
					programContext.copyArguments(this.arguments), 
					this.bindingPattern.copy(), 
					programContext.copyVariableList(this.freeVariableList), 
					new String(this.recursiveRelationName),
					Arrays.asList(this.argumentTypeAdornment),
					EvaluationType.getEvaluationType(this.deALSContext.getConfiguration().getProperty("deals.interpreter.fsclique.evaluationtype").toLowerCase()));
		}
		/*FSCliqueNode copy = new FSCliqueNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList), 
				new String(this.recursiveRelationName),
				this.isSharable,
				Arrays.asList(this.argumentTypeAdornment));
		*/
		programContext.getCliqueMapping().put(this, copy);
		
		copy.exitRulesOrNode = this.exitRulesOrNode.copy(programContext);
		copy.recursiveRulesOrNode = this.recursiveRulesOrNode.copy(programContext);
		copy.stage = this.stage;
				
		for (IMutualClique mc : this.mutualCliqueList)
			copy.mutualCliqueList.add((IMutualClique) programContext.getCliqueMapping().get(mc));
		
		for (RecursiveLiteral rl : this.recursiveLiteralList)
			copy.recursiveLiteralList.add((RecursiveLiteral) programContext.getNodeMapping().get(rl));
		
		return copy;
	}
	
	public static FSCliqueNode createFSCliqueNode(String predicateName, NodeArguments arguments, Binding binding, 
			VariableList freeVariableList, String recursiveRelationName, boolean isSharable, List<ArgumentType> argumentTypes,
			EvaluationType evaluationType) {
		//EvaluationType evaluationType = EvaluationType.getEvaluationType(deALSConfiguration.getProperty("deals.interpreter.fsclique.evaluationtype").toLowerCase()); 
		switch (evaluationType) {
			case MonotonicSemiNaive:
				return new MSNCliqueNode(predicateName, arguments, binding, freeVariableList, recursiveRelationName, isSharable, argumentTypes);
			case EagerMonotonic:
				return new EMSNCliqueNode(predicateName, arguments, binding, freeVariableList, recursiveRelationName, isSharable, argumentTypes);
			case SSC: 
				return new SSCFSCliqueNode(predicateName, arguments, binding, freeVariableList, recursiveRelationName, isSharable, argumentTypes); 
			default :
				return new FSCliqueNode(predicateName, arguments, binding, freeVariableList, recursiveRelationName, isSharable, argumentTypes);
		}		
	}
}
