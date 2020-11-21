package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion;

import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregateType;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSCountNodeType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYPredicateType;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreConfiguration;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreManager.TupleStoreType;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.interpreter.EvaluationType;
import edu.ucla.cs.wis.bigdatalog.interpreter.Interpreter;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.AggregateStoreType;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.RelationNode;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public abstract class RecursiveLiteral 
	extends RelationNode {
	protected String recursiveRelationName;
	protected EvaluationType evaluationType;
	protected FSAggregateType fsAggregatePredicateType;
	protected Database database;
	protected Interpreter interpreter;

	public RecursiveLiteral(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables, 
			String recursiveRelationName) {
		super(predicateName, args, binding, freeVariables);
		this.recursiveRelationName = recursiveRelationName;
		this.fsAggregatePredicateType = FSAggregateType.NONE;
	}

	public String getRecursiveRelationName() { return this.recursiveRelationName; }

	public EvaluationType getEvaluationType() { return this.evaluationType; }

	public void setEvaluationType(EvaluationType evaluationType) { this.evaluationType = evaluationType; }

	public void setFSAggregateType(FSAggregateType fsAggregatePredicateType) { this.fsAggregatePredicateType = fsAggregatePredicateType; }
	
	@Override
	public boolean initialize() {
		if (!super.initialize())
			return false;

		if (this.getXYPredicateType() == XYPredicateType.NONE) {
			if (this.evaluationType == EvaluationType.EagerMonotonic
					|| this.evaluationType == EvaluationType.SSC) {
				this.relation = this.relationManager.getRelation(this.recursiveRelationName, this.arity);
				
				if (this.relation == null) {
					this.relation = this.relationManager.getRelation(this.recursiveRelationName, this.arity + 1);

					if (this.relation == null) {
						DataType[] schema = this.getSchema();					
						if (this.fsAggregatePredicateType == FSAggregateType.FSCNT) {						
							FSCountNodeType fscntNodeType = FSCountNodeType.getNodeType(this.deALSContext.getConfiguration().getProperty("deals.interpreter.fscntconfiguration").toLowerCase());
							if (fscntNodeType == FSCountNodeType.KEYVALUESTORE) {
								DataType[] tempSchema = this.getSchema();
								schema = new DataType[tempSchema.length + 1];
								for (int i = 0; i < tempSchema.length; i++)
									schema[i] = tempSchema[i];
								schema[tempSchema.length] = DataType.KEYVALUESTORE;
							}
						}
						// APS 9/16/2014 - this elaborate relation-creating is needed in case the recursive literal is intialized before the aggregate node 
						AggregateStoreType aggregateStoreType = AggregateStoreType.getAggregateStoreType(this.deALSContext.getConfiguration().getProperty("deals.database.tuplestores.aggregate.type"));
						int keyColumns[] = new int[this.arity - 1];
						for (int i = 0; i < keyColumns.length; i++)
							keyColumns[i] = i;
						
						switch (aggregateStoreType) {
							case Heap:
								this.relation = relationManager.createAggregateRelationHeap(this.recursiveRelationName, schema, keyColumns, true);
								break;	
							case BPlusTree:
								this.relation = relationManager.createAggregateRelationBPlusTree(this.recursiveRelationName, schema, keyColumns, true);
								break;
							case Aggregator:
								TupleStoreConfiguration tsc = new TupleStoreConfiguration(TupleStoreType.Aggregation, keyColumns);
								tsc.setUniqueValue(true);
								AggregateInfo aggregateInfo = new AggregateInfo(this.fsAggregatePredicateType, this.arguments.getDataType());

								tsc.setAggregateInfos(new AggregateInfo[]{aggregateInfo});	
								
								this.relation = relationManager.createAggregateRelationAggregator(this.recursiveRelationName, schema, tsc);
								break;
						}
						//this.relation = relationManager.createDerivedRelation(this.recursiveRelationName, schema, false);					
					}
				}
			} else if (this.evaluationType == EvaluationType.MonotonicSemiNaive) {
				this.relation = relationManager.getRelation(this.recursiveRelationName, this.arity);
			} else {
				this.relation = relationManager.createRecursiveRelation(this.recursiveRelationName, this.getSchema(), true);
			}
		} else {
			this.relation = relationManager.createRecursiveRelation(this.recursiveRelationName, this.getXYSchema(), true);

			this.interpreter.getXYLiteralListManager().addXYLiteralList(this, this.getRecursiveRelationName());
		}
		
		return true;
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
	public void attachContext(DeALSContext deALSContext) {
		super.attachContext(deALSContext);
		this.database = deALSContext.getDatabase();
		this.interpreter = deALSContext.getInterpreter();
	}
}