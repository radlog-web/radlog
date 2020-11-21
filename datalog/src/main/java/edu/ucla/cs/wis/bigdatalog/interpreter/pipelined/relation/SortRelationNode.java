package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation;

import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.common.Triple;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreConfiguration;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreManager.TupleStoreType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbString;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterFunctor;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class SortRelationNode 
	extends RelationNode {
	
	protected boolean isMaterialized;
	protected List<Triple<Integer, Argument, SortOrder>> sortArgumentInfos;
	protected Database database;

	public SortRelationNode(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables) {
		super(predicateName, args, binding, freeVariables);
		this.sortArgumentInfos = new ArrayList<>();
		this.arity = this.arity - 1;
		// last argument contains the sort conditions
		InterpreterList inputSortArguments = (InterpreterList)args.get(args.size() - 1);
		InterpreterFunctor inputPair = null;
		while (inputSortArguments != null && !inputSortArguments.isEmpty()) {
			inputPair = (InterpreterFunctor) inputSortArguments.getHead();
			Argument arg = inputPair.getArgument(0);
			int ordinalPosition = ((DbInteger)arg.toDbType(this.typeManager)).getValue();
			if ((ordinalPosition < 0) || (ordinalPosition > (args.size() - 1)))
				throw new InterpreterException("Sort arguments invalid.  Arguments used must appear in rule head.");

			this.sortArgumentInfos.add(new Triple<>(ordinalPosition, arg, 
					SortOrder.getSortOrder(((DbString)inputPair.getArgument(1).toDbType(this.typeManager)).getValue())));			
			inputSortArguments = inputSortArguments.getTail();
		}		
	}
	
	@Override
	public boolean initialize() {	
		if (!super.initialize())
			return false;
		
		// Need a sorted relation with arbitrary number of columns
		// Need store sorted by arbitrary columns and ASC or DESC				
		int[] keyColumnsBySortOrder = new int[this.sortArgumentInfos.size()];
		int[] keySortOrder = new int[this.sortArgumentInfos.size()];
		int counter = 0;
		for (Triple<Integer, Argument, SortOrder> sortColumnInfo : this.sortArgumentInfos) {
			keyColumnsBySortOrder[counter] = sortColumnInfo.getFirst();
			keySortOrder[counter] = sortColumnInfo.getThird().getId();
			counter++;
		}

		TupleStoreConfiguration configuration = new TupleStoreConfiguration(TupleStoreType.SortingBPlusTree, 
				keyColumnsBySortOrder, keySortOrder);
		
		// schema does not include sort arguments
		DataType[] schema = this.getSchema();
		this.relation = relationManager.createDerivedRelation(this.predicateName, schema, configuration, false);
		
		if (this.relation != null) {
			this.cursor = (Cursor<Tuple>) this.database.getCursorManager().createScanCursor(this.relation);
			this.capturedTuple = this.relation.getEmptyTuple();
			this.determineMatchFreeColumns();
		}
		
		return (this.cursor != null);
	}
		
	@Override
	public Status getTuple() {
		this.traceGetTupleEntry();
		
		if (this.isEntry && !this.isMaterialized)
			this.materialize();			
		
		Status status = super.getTuple();
		
		this.traceGetTupleExit(status);

		return status;		
	}
	
	private void materialize() {
		while (this.getOrNodeTuple() == Status.SUCCESS) {
			for (int i = 0; i < this.arity; i++)
				this.capturedTuple.columns[i] = this.getArgumentAsDbType(i);
			
			this.relation.add(this.capturedTuple);
		}

		this.isMaterialized = true;
	}
	
	@Override
	public void deleteRelationsAndCursors() {
		this.relationManager.deleteDerivedRelation((DerivedRelation) this.relation);
	  	super.deleteRelationsAndCursors();
	  	this.isMaterialized = false;
	}

	@Override
	public SortRelationNode copy(ProgramContext programContext) {		
		SortRelationNode copy = new SortRelationNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		
		programContext.getNodeMapping().put(this, copy);
		
		copy.executionMode = this.executionMode;
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
