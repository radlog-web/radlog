package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.index.key.KeyIndex;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterFunctor;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.AndNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// APS 10/26/2014
public class LimitAndNode 
	extends AndNode {
	
	private KeyIndex<Tuple> index;
	private Tuple capturedTuple;
	private Argument limitArgument;
	private int limit = 0;
	private Database database;
	
	public LimitAndNode(String predicateName, NodeArguments arguments, Binding binding, Argument limitArgument) {
		super(BuiltInPredicate.LIMIT_PREDICATE_NAME + "_" + predicateName, arguments, binding);
		this.limitArgument = limitArgument;
	}
	
	@Override
	public boolean initialize() {
		if (!super.initialize())
			return false;
		
		// we need to declare a faux relation to make the keyindex work 
		// but, we will not add any tuples to the relation, just to the index	
		DataType[] schema = this.getSchema();
		Relation<?> relation = this.database.getRelationManager().createDerivedRelation(this.predicateName, schema);
		
		if (relation == null)
			return false;
		
		int[] columns = new int[this.getArity()];
		for (int i = 0; i < this.getArity(); i++)
			columns[i] = i;

		this.index = (KeyIndex<Tuple>) this.database.getIndexManager().createKeyIndex(relation, columns);
		if (index == null)
			return false;

		this.capturedTuple = relation.getEmptyTuple();
		
		if (this.limitArgument.isConstant())
			this.limit = ((DbInteger)this.limitArgument.toDbType(this.typeManager)).getValue();
		
		return true;
	}

	private DataType[] getSchema() {
		DataType[] schema = new DataType[this.getArity()];
		Argument arg;
		for (int i = 0; i < this.getArity(); i++) {
			arg = this.getArgument(i);
			if (arg instanceof InterpreterFunctor && !arg.isGround())			
				schema[i] = DataType.COMPLEX;
			else
				schema[i] = this.getArgument(i).getDataType();			
		}

		return schema;
	}
	
	public void deleteRelationsAndCursors() {
		if (this.index != null) {
			this.index.clear();
			this.index = null;
		}
		
		super.deleteRelationsAndCursors();
	}

	@Override
	public Status getTuple() {
		this.traceGetTupleEntry();

		Status status = this.getAndNodeTuple();

		this.isEntry = !(status == Status.SUCCESS);
		
		if (status == Status.SUCCESS) {
			// this.limit will be greater than zero if a constant is used in the program
			// otherwise, its a variable and must be checked each getTuple()
			if (this.limit > 0) {
				if (this.index.getNumberOfEntries() >= this.limit)
					status = Status.FAIL;
			} else { 
				if (this.index.getNumberOfEntries() >= ((DbInteger)this.limitArgument.toDbType(this.typeManager)).getValue())
					status = Status.FAIL;
			}
			
			if (status == Status.SUCCESS) {
				for (int i = 0; i < this.getArity(); i++)
					this.capturedTuple.columns[i] = this.getArgumentAsDbType(i);
				
				this.index.put(this.capturedTuple);
			}
		}

		this.traceGetTupleExit(status);

		return status;
	}

	@Override
	public LimitAndNode copy(ProgramContext programContext) {
		NodeArguments newArguments = programContext.copyArguments(this.arguments);
		Argument newLimitArgument = null;
		if (this.limitArgument.isConstant()) {
			newLimitArgument = this.limitArgument.copy();
		} else {
			for (int i = 0; i < this.arguments.innerArguments.length; i++) {
				if (this.arguments.innerArguments[i] == this.limitArgument) {
					newLimitArgument = newArguments.innerArguments[i];
					break;
				}
			}
		}			
		
		LimitAndNode copy = new LimitAndNode(new String(this.predicateName.substring(BuiltInPredicate.LIMIT_PREDICATE_NAME.length() + 1)),
				newArguments, 
				this.bindingPattern, 
				newLimitArgument);
		programContext.getNodeMapping().put(this, copy);
		
		copy.backtrackMap = this.backtrackMap;
		copy.ruleBacktrackPoint = this.ruleBacktrackPoint;
		copy.xyPredicateType = this.xyPredicateType;
		
		for (int i = 0; i < this.getNumberOfChildren(); i++)
			copy.addChild(this.getChild(i).copy(programContext));
				
		return copy;
	}
	
	@Override
	public void attachContext(DeALSContext deALSContext) {
		super.attachContext(deALSContext);
		this.database = deALSContext.getDatabase();
	}
}
