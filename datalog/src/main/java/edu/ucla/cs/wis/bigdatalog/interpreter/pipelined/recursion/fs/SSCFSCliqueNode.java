package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.fs;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregateType;
import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.AggregateRewriter;
import edu.ucla.cs.wis.bigdatalog.compiler.type.ArgumentType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.sssp.SSSPBPlusTreeForestLongKeysIntValuesScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.changetracker.ChangeTrackerCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.readoptimized.TupleBPlusTreeStoreROScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.AggregateRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.TupleAggregationStore;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.TupleAggregationStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.forests.AggregatorBPlusTreeForestLongKeysIntValues;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.interpreter.EvaluationType;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.fs.FSMinMaxAggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.LinearRecursiveLiteral;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.RecursiveLiteral;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueBaseNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueRuleType;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.BaseRelationNode;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class SSCFSCliqueNode extends FSCliqueNode {
	protected TupleAggregationStore		aggregationTupleStore;
	public AggregateRelation				aggregateRelation;
	public	 boolean						isNewTuple;
	protected boolean 					isMaterialized;
	protected DbTypeBase					node;
	protected ChangeTrackerCursor 			cursor;
	protected LinkedHashMap<DbTypeBase, AggregateRelation> relations;
		
	public SSCFSCliqueNode(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables,
			String recursiveRelationName, boolean isSharable, List<ArgumentType> argumentTypeAdornment) {
		super(predicateName, args, binding, freeVariables, recursiveRelationName, isSharable, argumentTypeAdornment);
		this.evaluationType = EvaluationType.SSC;
	}

	@Override
	public boolean initialize() {
		boolean status = false;

		if (!this.initialized) {
			if (this.recursiveRelationName != null) {
				if ((this.exitRulesOrNode != null) 
						&& (status = this.exitRulesOrNode.initialize())
						&& (this.recursiveRulesOrNode != null) 
						&& (status = this.recursiveRulesOrNode.initialize())) {						 
					this.initialized = true;
				}
				
				String relationName = AggregateRewriter.FS_AGGREGATE_NODE_NAME_PREFIX + this.getPredicateName().substring(0, this.getPredicateName().lastIndexOf("_"));
				int numberOfColumns = this.arity; 
				this.aggregateRelation = (AggregateRelation) this.database.getRelationManager()
						.getRelation(relationName, numberOfColumns);

				for (RecursiveLiteral literal : this.recursiveLiteralList) {
					if (literal instanceof LinearRecursiveLiteral) {
						if (literal.getRecursiveRelationName().equals(this.aggregateRelation.getName())) {
							this.cursor = (ChangeTrackerCursor) literal.getCursor();
							break;
						}
					}
				}
				
				this.cursor.initializeTracking(this.deALSContext.getConfiguration());
				
				this.isCleanedUp = false;
				this.stage = 0;
				this.numberOfDeltaFactsByIteration = new LinkedList<>();
				this.numberOfGeneratedFactsByIteration = new LinkedList<>();
				this.numberOfGeneratedFactsThisIteration = 0;
				
				this.time = System.currentTimeMillis();
			} else {
				throw new InterpreterException("Can not initialize clique.  No relation " + this.predicateName);
			}
		} else {
			status = true;
		}

		return status;
	}
	
	//protected void initializeIndex() {/*DO NOTHING*/}
			
	@Override
	public int getAnyTuple(Cursor readCursor, CliqueBaseNode clique, Tuple tuple) {
		if (DEBUG)
			this.logTrace("Entering getAnyTuple for {}", this.toStringNode());
		
		if (this.isMaterialized)
			return 0;//readCursor.getTuple(tuple);
	
		Status status = Status.FAIL;

		DataType[] schema = this.getSchema();
		
		FSMinMaxAggregateRelationNode recursiveRuleAggregateNode;		
		FSMinMaxAggregateRelationNode exitRuleAggregateNode; 
		
		OrNode orNode = this.exitRulesOrNode;
		while (!(orNode instanceof FSMinMaxAggregateRelationNode))
			orNode = orNode.getChild(0).getChild(0);
				
		exitRuleAggregateNode = (FSMinMaxAggregateRelationNode)orNode;
		orNode = this.exitRulesOrNode;
		
		while (!(orNode instanceof BaseRelationNode))
			orNode = orNode.getChild(0).getChild(0);	
		
		BaseRelationNode baseRelationNode = (BaseRelationNode) orNode;
		
		orNode = this.recursiveRulesOrNode;
		while (!(orNode instanceof FSMinMaxAggregateRelationNode))
			orNode = orNode.getChild(0).getChild(0);
						
		recursiveRuleAggregateNode = (FSMinMaxAggregateRelationNode) orNode;
		
		LinearRecursiveLiteral linearLiteral = (LinearRecursiveLiteral) this.recursiveRulesOrNode.getChild(0).getChild(0);
		
		//((BaseRelationNode)this.exitRulesOrNode.getChild(0).getChild(0)).getCursor();
		TupleBPlusTreeStoreROScanCursor nodeBaseRelationCursor = (TupleBPlusTreeStoreROScanCursor)baseRelationNode.getCursor(); 
		/*
		TupleStoreConfiguration tsc = new TupleStoreConfiguration(TupleStoreType.Aggregation, new int[]{0});
		tsc.setUniqueValue(true);
		tsc.setAggregateInfos(new AggregateInfo[]{new AggregateInfo(FSAggregateType.FSMIN, schema[1])});		
		*/
		this.relations = new LinkedHashMap<>();
		this.node = DbTypeBase.loadFrom(schema[0], 0);
		// hang on to this since the query form has a reference to it.
		AggregateRelation firstRelation = this.aggregateRelation;
		
		while (true) {
			if (this.currentRuleType == CliqueRuleType.EXIT_RULE) {		
				if (this.exitRulesOrNode.getTuple() != Status.SUCCESS)
					break;

				nodeBaseRelationCursor.startNextKey();
				this.node = this.getArgumentAsDbType(0);

				this.aggregateRelation = (AggregateRelation) exitRuleAggregateNode.getRelation();
				
				this.relations.put(this.node.copy(), this.aggregateRelation);
				
				this.numberOfDeltaFactsByIteration.add(1);
				// now we have our next new seed node, so we use the recursive rules until exhaustion
				this.stage = 0;
				this.cursor.beginNextStage(this.deALSContext, this.stage);
				this.prepareStatCountersForNextStage(this.stage++, 1);
				this.currentRuleType = CliqueRuleType.RECURSIVE_RULE;
			}

			if (this.currentRuleType == CliqueRuleType.RECURSIVE_RULE) {				
				// Generate the recursive tuples until exhausted
				while (true) {
					this.recursiveRulesOrNode.getTuple();
					
					if (this.cursor.isFixedPointReached())
						break;
					
					this.numberOfDeltaFactsByIteration.add(this.cursor.getSizeOfDelta());
					this.prepareStatCountersForNextStage(this.stage++, this.cursor.getSizeOfDelta());
					this.stage++;
					this.cursor.beginNextStage(this.deALSContext, this.stage);					
					//System.out.println(this.aggregateRelation.toString());
				}				

				this.aggregateRelation.setName(this.aggregateRelation.getName() + "_" + this.node.toString());
				
				exitRuleAggregateNode.initializeRelation();								
				recursiveRuleAggregateNode.initializeRelation();
				linearLiteral.initialize();
				this.prepareStatCountersForNextNode();
				this.cursor = (ChangeTrackerCursor) linearLiteral.getCursor(); 
				this.cursor.initializeTracking(this.deALSContext.getConfiguration());
				this.currentRuleType = CliqueRuleType.EXIT_RULE;
			}
		}
		
		// initialize the structure to hold the trees 
		/*AggregatorBPlusTreeIntKeysIntValues tree = (AggregatorBPlusTreeIntKeysIntValues)this.aggregationTupleStore.storageStructure;
		
		AggregatorBPlusTreeForestIntKeysIntValues forest 
			= new AggregatorBPlusTreeForestIntKeysIntValues(tree.getNodeSize(), tree.getKeyColumns(), tree.getKeyColumnTypes(), 
					tree.getValueColumns(), tree.getValueColumnTypes(), tsc.aggregateInfos);
		forest.setForest(relations);*/
		
		TupleAggregationStoreStructure tass = ((TupleAggregationStore)this.aggregateRelation.getTupleStore()).storageStructure;
		
		AggregatorBPlusTreeForestLongKeysIntValues forest 
			= new AggregatorBPlusTreeForestLongKeysIntValues(tass.getNodeSize(), tass.getKeyColumns(), 
					tass.getKeyColumnTypes(), tass.getValueColumns(), tass.getValueColumnTypes(), 
					new AggregateInfo[]{new AggregateInfo(FSAggregateType.FSMIN, DataType.INT)}, 
					this.deALSContext.getConfiguration(), this.typeManager);
		
		forest.setForest(this.relations.values());
		
		((TupleAggregationStore)firstRelation.getTupleStore()).storageStructure = forest;
		List<Cursor<?>> cursors = this.database.getCursorManager().getCursors().get(firstRelation);
		for (Cursor<?> cursor : cursors) {
			if (cursor instanceof SSSPBPlusTreeForestLongKeysIntValuesScanCursor) {
				((SSSPBPlusTreeForestLongKeysIntValuesScanCursor)cursor).reset(forest);
			}
		}
			/*
		long totalNumberOfShortestPaths = 0;
		for (Map.Entry<DbTypeBase,AggregateRelation> entry : relations.entrySet()) {
			totalNumberOfShortestPaths += entry.getValue().getTupleStore().getNumberOfTuples();
						/*Cursor cursor = CursorManager.createScanCursor(entry.getValue());
			Tuple ttuple = cursor.getEmptyTuple();
			while (cursor.getTuple(ttuple) > 0)
				System.out.println(entry.getKey() + "," + ttuple.getColumn(0) + "," + ttuple.getColumn(1));*/
		//}		
		
		//((SSSPBPlusTreeForestLongKeysIntValuesScanCursor)readCursor).setRelations(this.relations);
		
		//System.out.println(" Total # of Shortest Paths " + totalNumberOfShortestPaths);
		this.isMaterialized = true;
		
		if (DEBUG)
			this.logTrace("Exiting generateTuple for {} with status = {}", this.toStringNode(), status);
		
		return 0;//readCursor.getTuple(tuple);
	}
	
	private void prepareStatCountersForNextStage(int stage, int numberAddUpdatedThisStage) {
		if (DEBUG) {
			this.logInfo("Stage {} ({}) [{}] [total this stage: {}] time: {} # of delta tuples:{}", 
					stage, 
					this.node, 
					this.aggregateRelation.getTupleStore().getNumberOfTuples(), 
					numberAddUpdatedThisStage, 
					(System.currentTimeMillis() - time), 
					this.numberOfDeltaFactsByIteration);
			this.logInfo("***************************Stage {}-{} Complete***************************", stage, this.node);				
		}
		this.time = System.currentTimeMillis();
	}
	
	private void prepareStatCountersForNextNode() {
		if (DEBUG) {
			this.logInfo("All shortest paths found from {} time: {} # of delta tuples:{}", 
					this.node, 
					(System.currentTimeMillis() - time), 
					this.numberOfDeltaFactsByIteration);
			this.logInfo("///////////////////////////{} Complete///////////////////////////", this.node);				
		}
		this.time = System.currentTimeMillis();
	}
	
	@Override
	public void cleanUp() {
		this.cliqueCleanUp();

		//if (this.aggregateRelation != null)
		//	this.aggregateRelation.cleanUp();
		
		for (Map.Entry<DbTypeBase,AggregateRelation> entry : this.relations.entrySet())
			entry.getValue().cleanUp();
		
		this.cursor.initialize();
				
		this.isCleanedUp = true;
		this.isMaterialized = false;
	}
}
