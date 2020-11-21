package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.fs;

import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregateType;
import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.AggregateRewriter;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorResult;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.interpreter.EvaluationType;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterFunctor;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ExecutionMode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.AggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.AggregateStoreType;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.fs.EMSNCliqueNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueNode;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public abstract class FSAggregateRelationNode 
	extends AggregateRelationNode  {

	protected CliqueNode				clique;	
	protected final boolean			isResultInteger;
	protected Variable 				aggregateValueVariable;
	
	protected Tuple 					newTuple;
	protected Tuple 					oldTuple;
	protected AggregatorResult 			aggregatorResult;
	//protected ChangeTrackerCursor<?> 	changesCursor;

	public FSAggregateRelationNode(String predicateName, NodeArguments args, Binding binding, 
			VariableList freeVariables, AggregateInfo[] aggregateInfos, AggregateStoreType aggregateStoreType) {
		super(predicateName, args, binding, freeVariables, aggregateInfos, aggregateStoreType);
		
		this.isResultInteger = DataType.isInteger(((Variable)this.getArgument(this.getArity() - 1)).getDataType());
		this.aggregateStoreType = aggregateStoreType;
		this.aggregatorResult = new AggregatorResult();
		this.isMaterialized = false;
	}
	
	public void setExecutionMode(ExecutionMode executionMode) { 
		this.executionMode = executionMode;
	}
	
	public void setClique(CliqueNode clique) { this.clique = clique; }
	
	public boolean isInClique() { return (this.clique != null); 	}
	
	public void setNewOrUpdatedTuple(boolean isNewTuple) {
		if (this.clique == null)
			return;
		
		if (this.clique instanceof EMSNCliqueNode)
			((EMSNCliqueNode)this.clique).isNewTuple = isNewTuple;		
	}

	// get schema for a relation to store summary. 
	// Example: (a, b, 2)
	protected DataType[] determineSchemaMain(int[] keyColumns) {
		// we need 1 column for each key, 1 column for fsmax, 1 + size of 1st argument in functor for fscnt
		int relationArity = keyColumns.length + 1;

		DataType[] schema = new DataType[relationArity];
		for (int i = 0; i < keyColumns.length; i++)
			schema[i] = this.getArgument(i).getDataType();

		schema[relationArity - 1] = this.getArgument(keyColumns.length).getDataType();
		return schema;
	}
	
	/* get schema for a relation to store details. 
	 * Example: 
	 *   (a, b, a, 1)
	 *   (a, b, c, 1) */	
	/*protected DataType[] determineSchemaDetail(int[] keyColumns, int numberOfJoinKeys) {
		// we need 1 column for each key, 1 column for fsmax, 1 + size of 1st argument in functor for fscnt
		int relationArity = keyColumns.length + numberOfJoinKeys + 1;

		DataType[] schema = new DataType[relationArity];
		for (int i = 0; i < keyColumns.length; i++)
			schema[i] = this.getArgument(i).getDataType();

		if (this.getArgument(this.numberOfKeyColumns) instanceof InterpreterFunctor) {
			InterpreterFunctor functor = (InterpreterFunctor)this.getArgument(keyColumns.length);
			if (functor.getArgument(0) instanceof InterpreterFunctor)
				functor = (InterpreterFunctor)functor.getArgument(0);
			
			int position = 0;
			for (int i = keyColumns.length; i < (keyColumns.length + numberOfJoinKeys); i++)
				schema[i] = functor.getArgument(position++).getDataType();
		}
		
		schema[relationArity - 1] = this.isResultInteger ? this.deALSContext.getConfiguration().getIntegerDataType() : DataType.DOUBLE;
		return schema;
	}*/

	protected String getRelationName() {
		String relationName = this.getPredicateName().substring(0, FSAggregateType.getFSAggregateType(this.getPredicateName()).name().length());
		return AggregateRewriter.FS_AGGREGATE_NODE_NAME_PREFIX + this.getPredicateName().substring(relationName.length(), this.getPredicateName().lastIndexOf("_")); 
	}
	
	public static boolean isFSAggregateRelation(String name) {
		return name.startsWith(AggregateRewriter.FS_AGGREGATE_NODE_NAME_PREFIX);
	}
	
	public boolean matchesRelationName(String name) {
		FSAggregateType aggregateType = FSAggregateType.getFSAggregateType(this.predicateName);
		if (aggregateType != null) {
			if (name.startsWith(AggregateRewriter.FS_AGGREGATE_NODE_NAME_PREFIX))
				name = name.substring(AggregateRewriter.FS_AGGREGATE_NODE_NAME_PREFIX.length());
		
			return this.predicateName.substring(aggregateType.name().length()).startsWith(name);
		}
		
		return this.predicateName.startsWith(name);
	}	
	
	@Override
	public Status getTuple() {
		this.traceGetTupleEntry();

		Status status = this.doGetTuple();

		this.traceGetTupleExit(status);

		return status;
	}
	
	protected boolean isChangeTrackingStore() {
		if (this.clique == null)
			return false;

		return (this.clique.getEvaluationType() == EvaluationType.EagerMonotonic);
	}
	/*
	protected Status doRetrieveTuple() {
		Status status = Status.FAIL;
		
		while (this.changesCursor.getTuple(this.newTuple) > 0) {
			for (int i = 0; i < this.numberOfKeyColumns; i++)
				if (!this.arguments.innerArguments[i].isConstant())
					((Variable)this.arguments.innerArguments[i]).value = this.newTuple.columns[i];
			
			this.aggregateValueVariable.value = this.newTuple.columns[this.numberOfKeyColumns];

			status = Status.SUCCESS;
			this.setNewOrUpdatedTuple(true);
				
			// we must subtract 1 for each tuple addTuple() will process so as to not double count
			this.numberOfRecursiveTuplesDerived -= 1;
			if (this.clique != null)
				this.clique.numberOfDerivedTuplesThisIteration -= 1;
			
			break;
		}
		
		if (status != Status.SUCCESS){			
			status = Status.FAIL;
			this.isMaterialized = false;
		} 
			
		return status;
	}*/
	
	abstract protected Status doGetTuple();
}
