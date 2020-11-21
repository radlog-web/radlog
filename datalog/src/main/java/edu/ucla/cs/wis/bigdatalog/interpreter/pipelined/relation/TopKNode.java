package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import edu.ucla.cs.wis.bigdatalog.common.Triple;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.index.key.KeyIndex;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.queue.FixedSizePriorityQueue;
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

public class TopKNode 
	extends RelationNode {
	
	protected boolean isMaterialized;
	protected List<Triple<Integer, Argument, SortOrder>> sortArgumentInfos;
	private Argument limitArgument;
	private int limit;
	private KeyIndex<Tuple> index;
	protected Database database;
	protected boolean usePriorityQueue = false;
	//protected MinMaxPriorityQueue<Tuple> queue;
	protected PriorityQueue<Tuple> queue;
	protected int counter;

	public TopKNode(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables) {
		super(predicateName + "_TopK", args, binding, freeVariables);
	}
	
	public void setConditions(InterpreterList sortConditions, Argument limitArgument) {		
		this.sortArgumentInfos = new ArrayList<>();
		// last argument contains the sort conditions
		InterpreterFunctor inputPair = null;
		while (sortConditions != null && !sortConditions.isEmpty()) {
			inputPair = (InterpreterFunctor) sortConditions.getHead();
			Argument arg = inputPair.getArgument(0);
			int ordinalPosition = ((DbInteger)arg.toDbType(this.typeManager)).getValue();
			if ((ordinalPosition < 0) || (ordinalPosition > this.arity))
				throw new InterpreterException("Sort arguments invalid.  Arguments used must appear in rule head.");

			this.sortArgumentInfos.add(new Triple<>(ordinalPosition, arg, 
					SortOrder.getSortOrder(((DbString)inputPair.getArgument(1).toDbType(this.typeManager)).getValue())));			
			sortConditions = sortConditions.getTail();
		}
		this.limitArgument = limitArgument;
	}
	
	@Override
	public boolean initialize() {	
		if (!super.initialize())
			return false;
		
		// Need a sorted relation with arbitrary number of columns
		// Need store sorted by arbitrary columns and ASC or DESC				
		final int[] keyColumnsBySortOrder = new int[this.sortArgumentInfos.size()];
		final int[] keySortOrder = new int[this.sortArgumentInfos.size()];
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

		this.limit = ((DbInteger)this.limitArgument.toDbType(this.typeManager)).getValue();
		
		this.relation = this.relationManager.createDerivedRelation(this.predicateName, schema, configuration, false);		
		if (this.relation != null) {
			this.cursor = (Cursor<Tuple>) this.database.getCursorManager().createScanCursor(this.relation);
			this.capturedTuple = this.relation.getEmptyTuple();
			this.determineMatchFreeColumns();
			
			int[] columns = new int[this.getArity()];
			for (int i = 0; i < this.getArity(); i++)
				columns[i] = i;
					
			if (this.usePriorityQueue) {
				this.index = (KeyIndex<Tuple>) this.database.getIndexManager().createKeyIndex(this.relation, columns);
				if (this.index == null)
					return false;
				
				Comparator<Tuple> comparator = new Comparator<Tuple>() {
					int[] columns = keyColumnsBySortOrder;
					int[] sortOrder = keySortOrder;
					@Override
					public int compare(Tuple t1, Tuple t2) {
						for (int i = 0; i < columns.length; i++) {
							if (sortOrder[i] == SortOrder.ASC.getId()) {
								if (t1.getColumn(columns[i]).lessThan(t2.getColumn(columns[i])))
									return 1;
								else if (t1.getColumn(columns[i]).greaterThan(t2.getColumn(columns[i])))
									return -1;
							} else {
								if (t1.getColumn(columns[i]).greaterThan(t2.getColumn(columns[i])))
									return 1;
								else if (t1.getColumn(columns[i]).lessThan(t2.getColumn(columns[i])))
									return -1;
							}
						}
						return 0;
					}
				};
				
				//this.queue = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(this.limit).create();
				this.queue = new FixedSizePriorityQueue(this.limit, comparator);
				//this.cursor = new IteratorWrapperCursor(this.relation, this.queue);
			}
		}
		return (this.cursor != null);
	}
		
	@Override
	public Status getTuple() {
		this.traceGetTupleEntry();
		
		if (this.isEntry && !this.isMaterialized) {
			
			if (this.usePriorityQueue) {
				while (this.getOrNodeTuple() == Status.SUCCESS) {
					for (int i = 0; i < this.arity; i++)
						this.capturedTuple.columns[i] = this.getArgumentAsDbType(i);
					
					if (this.index.put(this.capturedTuple)) {
						this.queue.add(this.capturedTuple.copy());

						System.out.println("Adding: " + this.capturedTuple.toString() + " " + this.queue.toString());
					}
				}

				Iterator<Tuple> iterator = this.queue.iterator();
				while (iterator.hasNext())
					this.relation.add(iterator.next());
			} else {
				while (this.getOrNodeTuple() == Status.SUCCESS) {
					for (int i = 0; i < this.arity; i++)
						this.capturedTuple.columns[i] = this.getArgumentAsDbType(i);
					
					this.relation.add(this.capturedTuple);
				}
			}
			
			this.isMaterialized = true;
			this.isEntry = true;
		}
		
		Status status = this.doGetTuple();
		
		this.traceGetTupleExit(status);

		return status;
	}
		
	private Status doGetTuple() {
		Status status = Status.SUCCESS;
		
		//this.isEntry = false;
		
		if (this.limit > 0) {
			//if (this.index.getNumberOfEntries() >= this.limit)
			if (this.counter >= this.limit)
				status = Status.FAIL;
		} else {
			//if (this.index.getNumberOfEntries() >= this.limitArgument.toDbType(this.typeManager).getIntValue())
			if (this.counter >= ((DbInteger)this.limitArgument.toDbType(this.typeManager)).getValue())
				status = Status.FAIL;
		}
		
		if (status == Status.SUCCESS) {
			status = super.getTuple();
			if (status == Status.SUCCESS) {
				this.counter++;
				//for (int i = 0; i < this.getArity(); i++)
				//	this.capturedTuple.columns[i] = this.getArgumentAsDbType(i);
			
				//this.index.put(this.capturedTuple);
			}
		}
		
		this.isEntry = false;
		
		return status;
	}
	
	@Override
	public void deleteRelationsAndCursors() {
		this.relationManager.deleteDerivedRelation((DerivedRelation) this.relation);
		if (this.index != null) {
			this.index.clear();
			this.index = null;
		}
		
		if (this.usePriorityQueue)
			this.queue.clear();
		
	  	super.deleteRelationsAndCursors();
	  	this.isMaterialized = false;
	}

	@Override
	public TopKNode copy(ProgramContext programContext) {		
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
		
		List<Triple<Integer, Argument, SortOrder>> newSortArgumentInfos = new ArrayList<>();
		for (Triple<Integer, Argument, SortOrder> sai : this.sortArgumentInfos)
			newSortArgumentInfos.add(new Triple<>(sai.getFirst(), sai.getSecond().copy(), sai.getThird()));
				
		TopKNode copy = new TopKNode(new String(this.predicateName), 
				newArguments, 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		
		copy.limitArgument = newLimitArgument;
		copy.sortArgumentInfos = newSortArgumentInfos;
		
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
