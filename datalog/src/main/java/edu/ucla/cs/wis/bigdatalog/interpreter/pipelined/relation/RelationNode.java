package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.database.RelationManager;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.changetracker.ChangeTrackerCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.BPlusTreeTupleStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InputVariable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterFunctor;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.choice.ChoicePredicate;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.RecursiveOrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ExecutionMode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public abstract class RelationNode 
	extends OrNode {
	protected int				arity;
	protected Relation 			relation;
	protected Cursor			cursor;
	protected boolean			hasBoundColumns;
	protected int[]				boundColumns;
	protected DbTypeBase[]		boundColumnValues;	
	protected int				numberOfUnboundColumns;
	protected boolean			hasMatchColumns;
	protected int[]				freeMatchColumns;
	protected Tuple				capturedTuple;
	protected ExecutionMode 	executionMode;
	protected boolean 			isMaterialized;
	protected RelationManager	relationManager; 
	
	public RelationNode(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables) {
		super(predicateName, args, binding, freeVariables);

		this.arity = args.size();		
		this.initializeBoundColumns();		
		this.relation = null;
		this.cursor	= null;
		this.executionMode = ExecutionMode.Pipelined;
		this.isMaterialized = false;
	}

	public Relation<?> getRelation() { return this.relation; }

	public void setRelation(Relation<?> relation) { this.relation = relation; }
	
	public Cursor<?> getCursor() { return this.cursor; }
	
	public void setCursor(Cursor<?> cursor) { this.cursor = cursor; }
	
	public void setExecutionMode(ExecutionMode executionMode) { this.executionMode = executionMode; }
	
	public ExecutionMode getExecutionMode() { return this.executionMode; } 
	
	public void initializeBoundColumns() {
		int[] tempBoundColumns = new int[this.arity];

		int boundCounter = 0;

		for (int i = 0; i < this.arity; i++) {
			if (this.getBinding(i) == BindingType.BOUND)
				tempBoundColumns[boundCounter++] = i;
		}
		
		if (boundCounter > 0) {			
			this.boundColumns = new int[boundCounter];
			for (int i = 0; i < boundCounter; i++)
				this.boundColumns[i] = tempBoundColumns[i];
			
			this.boundColumnValues = new DbTypeBase[this.boundColumns.length];
		}
		
		this.hasBoundColumns = (boundCounter > 0);
		this.numberOfUnboundColumns = this.arity - boundCounter;
	}
	
	protected void determineMatchFreeColumns() {
		int[] columns = new int[this.arity];
		for (int i = 0; i < this.arity; i++) {
			if (this.getArgument(i).isGround()
					|| ((this.getArgument(i).getDataType() == DataType.COMPLEX) 
					|| (this.getArgument(i).getDataType() == DataType.LIST)
					|| (this.getArgument(i).getDataType() == DataType.UNKNOWN))
					|| (this.getArgument(i) instanceof InputVariable)) {
				columns[i] = 1;
			} else {
				// if it is bound, it cannot be a recurive node - leave those alone 
				if (this.getBinding(i) == BindingType.BOUND) {
					if (this instanceof RecursiveOrNode) {
						columns[i] = 1;
					} else {
						// see if the column is an index lookup, then we don't need to match on it
						int[] indexedColumns = null;
						
						if (!(this.cursor instanceof ChangeTrackerCursor)) {
							if (this.cursor instanceof SelectionCursor) {
								indexedColumns = ((SelectionCursor<?>)this.cursor).getFilterColumns();
							} else if (this.cursor.getRelation().getTupleStore() instanceof BPlusTreeTupleStore) {
								indexedColumns = ((BPlusTreeTupleStore)this.cursor.getRelation().getTupleStore()).getKeyColumns();
							}
						}
						
						if (indexedColumns != null) {
							int j;
							for (j = 0; j < indexedColumns.length; j++)
								if (indexedColumns[j] == i)
									break;

							if (j < indexedColumns.length)
								columns[i] = 0;
							else
								columns[i] = 1;
						} else {
							columns[i] = 1;
						}
					}
				} else {
					columns[i] = 0;
					// if free, and if a variable appears more than once, we must match after 1st occurrence 
					for (int j = 0; j < i; j++) {
						if (this.getArgument(j) == this.getArgument(i)) {
							columns[i] = 1;
							break;
						}
					}
				}
			}
		}

		for (int i = 0; i < this.arity; i++) {
			if (columns[i] == 1)
				this.hasMatchColumns = true;
		}
		this.freeMatchColumns = columns;
		this.capturedTuple = this.relation.getEmptyTuple();
	}
			
	public DataType[] getSchema() {
		DataType[] schema = new DataType[this.arity];
		Argument arg;
		for (int i = 0; i < this.arity; i++) {
			arg = this.getArgument(i);
			// APS 9/16/2013 - for choice - the functor is a wrapper for a single argument
			if (arg instanceof InterpreterFunctor && !arg.isGround()) {
				InterpreterFunctor functor = (InterpreterFunctor)arg;
				if ((this instanceof ChoicePredicate) && (functor.getArity() == 1))
					schema[i] = functor.getArgument(0).getDataType();
				else
					schema[i] = DataType.COMPLEX;
			} else {
				schema[i] = this.getArgument(i).getDataType();
			}
		}

		return schema;
	}
	
	public void deleteRelationsAndCursors() {
		this.cursor = null;
		this.relation = null;
	}

	public void cleanUp() {
		super.cleanUp();
		this.freeVariableList.makeFree();
	}

	public void partialCleanUp() {
		this.cleanUp();
	}

	public String toString() { return toStringNode(); }

	public void setBoundColumnValues() {
		if (this.hasBoundColumns) {
			for (int i = 0; i < this.boundColumns.length; i++)
				this.boundColumnValues[i] = this.getArgumentAsDbType(this.boundColumns[i]);				
		}
	}

	@Override
	public Status getTuple() {		
		Status status = Status.FAIL;
		int getStatus = 0;

		this.traceGetTupleEntry();
		if (this.isEntry)
			this.resetCursor();

		int i = 0;

		if (this.isEntry || !this.hasAllArgumentsBound) {
			if (this.hasMatchColumns) {
				for (int j = 0; j < this.freeVariableList.variables.length; j++)
					this.freeVariableList.variables[j].makeFree();
			}
						
			while ((getStatus = this.cursor.getTuple(this.capturedTuple)) > 0) {
				for (i = 0; i < this.arity; i++) {
					if (this.freeMatchColumns[i] == 1) {
						if (!this.arguments.innerArguments[i].match(this.capturedTuple.columns[i])) {
							for (int j = 0; j < this.freeVariableList.variables.length; j++)
								this.freeVariableList.variables[j].makeFree();
							break;
						}
					} else {
						((Variable)this.arguments.innerArguments[i]).setValue(this.capturedTuple.columns[i]);
					}
				}
				
				if (i == this.arity)
					break;
			}
		}

		if (getStatus == 0) {
			for (int j = 0; j < this.freeVariableList.variables.length; j++)
				this.freeVariableList.variables[j].makeFree();
			
			if (this.isEntry)
				status = Status.ENTRY_FAIL;
			
			this.isEntry = true;
		} else {
			status = Status.SUCCESS;
			this.isEntry = false;
		}
	
		this.traceGetTupleExit(status);

		return status;
	}

	public void resetCursor() {
		// If we had an indexed cursor, we use the current bound values of the
		// argument list to reset our cursor with the appropriate indexed column values.
		if (this.hasBoundColumns) {
			if (this.cursor instanceof SelectionCursor) {
				for (int i = 0; i < this.boundColumns.length; i++)
					this.boundColumnValues[i] = this.getArgumentAsDbType(this.boundColumns[i]);					
			}
		}
		
		this.cursor.reset(this.boundColumnValues);
	}
	
	@Override
	public void attachContext(DeALSContext deALSContext) {
		super.attachContext(deALSContext);
		this.relationManager = deALSContext.getDatabase().getRelationManager();
	}
}
