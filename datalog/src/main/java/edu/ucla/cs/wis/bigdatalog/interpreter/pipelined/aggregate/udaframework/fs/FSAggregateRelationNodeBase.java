package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.fs;

import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregateType;
import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.AggregateRewriter;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.type.DbDouble;
import edu.ucla.cs.wis.bigdatalog.database.type.DbFloat;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLongLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLongLongLongLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterFunctor;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.AggregateStoreType;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.AggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.fs.EMSNCliqueNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueNode;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public abstract class FSAggregateRelationNodeBase
	extends AggregateRelationNode {
	
	protected enum FSValueType {
		FS, OldFS, OldFSAsZero, OldFSAsMin, OldFSAsMax;
	}

	protected CliqueNode		clique;	
	protected final boolean		isResultInteger;
	protected Variable 			aggregateValueVariable;

	public FSAggregateRelationNodeBase(String predicateName, NodeArguments args,
			Binding binding, VariableList freeVariables, boolean isRead, AggregateStoreType aggregateStoreType) {
		super(predicateName, args, binding, freeVariables, isRead,  aggregateStoreType);
		this.isResultInteger = DataType.isInteger(this.getArgument(this.getArity() - 1).getDataType());	
	}

	public void setClique(CliqueNode clique) { this.clique = clique; }
	
	public boolean isInClique() { return (this.clique != null); 	}
	
	public void setLastTupleModified(boolean isNewTuple) {
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
	protected DataType[] determineSchemaDetail(int[] keyColumns, int numberOfJoinKeys) {
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
		
		schema[relationArity - 1] = this.isResultInteger ? this.deALSContext.getConfiguration().getCountDataType() : DataType.DOUBLE;
		return schema;
	}

	protected String getRelationName() {
		String relationName = this.getPredicateName().substring(0, FSAggregateType.getFSAggregateType(this.getPredicateName()).name().length());
		return AggregateRewriter.FS_AGGREGATE_NODE_NAME_PREFIX + this.getPredicateName().substring(relationName.length(), this.getPredicateName().lastIndexOf("_")); 
	}
	
	protected boolean setArgumentValues(FSValueType type) {	
		boolean isMatch = true;

		switch (type) {
		case OldFS:
			isMatch = this.aggregateValueVariable.match(this.oldValue.getColumn(this.numberOfKeyColumns + 1));
			break;		
		case OldFSAsZero:
			if (this.isResultInteger)
				isMatch = this.aggregateValueVariable.match(this.typeManager.castToCountDataType(0));
			else
				isMatch = this.aggregateValueVariable.match(DbFloat.create(0));
			break;			
		}
		return isMatch;
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
	
	protected DbTypeBase getMaxValue() {
		if (this.isResultInteger) {
			switch (this.deALSContext.getConfiguration().getCountDataType()) {
				case INT:
					return DbInteger.create(Integer.MAX_VALUE);
				case LONG:
					return DbLong.create(Long.MAX_VALUE);
				case LONGLONG:
					return DbLongLong.getMaxLongLong();
				case LONGLONGLONGLONG:
					return DbLongLongLongLong.getMaxLongLongLongLong();
			}
			return null;
		}
		
		return DbDouble.create(Double.MAX_VALUE);
	}
	
	protected DbTypeBase getMinValue() {
		if (this.isResultInteger) {
			switch (this.deALSContext.getConfiguration().getCountDataType()) {
				case INT:
					return DbInteger.create(Integer.MIN_VALUE);
				case LONG:
					return DbLong.create(Long.MIN_VALUE);
				case LONGLONG:
					return DbLongLong.getMinLongLong();
				case LONGLONGLONGLONG:
					return DbLongLongLongLong.getMinLongLongLongLong();
			}
			return null;
		}
		
		return DbDouble.create(Double.MIN_VALUE);
	}
}
