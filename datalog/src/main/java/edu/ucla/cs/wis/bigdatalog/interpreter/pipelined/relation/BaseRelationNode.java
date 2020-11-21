package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.Module;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicateStructuralAttribute;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.BaseRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreConfiguration;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreManager.TupleStoreType;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.QueryFormNode;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class BaseRelationNode 
	extends RelationNode 
	implements QueryFormNode {
	
	private boolean useBPlusTree = false;
	protected Database database;
	protected Module module;

	public BaseRelationNode(String relationName, NodeArguments args, Binding binding, VariableList freeVariables) {
		super(relationName, args, binding, freeVariables);
	}
	
	@Override
	public boolean initialize() {
		if (!super.initialize())
			return false;

		// if in need of index, use an indexed tuple store and avoid a secondary index
		if (!this.useBPlusTree) {
			this.relation = this.relationManager.getBaseRelation(this.predicateName);
			this.cursor = this.database.getCursorManager().createCursor(this.relation, this.boundColumns);
		} else {
			String indexedOn = "_all";
			int[] keyColumns;
			DataType[] schema = this.getSchema();
			
			if (this.bindingPattern.allFree()) {				
				// use first column to keep long lists of tuples together so scanning is faster
				// simple data types, we use, otherwise, just use entire tuple for ease's sake
				if (schema[0] == DataType.INT || schema[0] == DataType.LONG || schema[0] == DataType.STRING) {
					keyColumns = new int[1];
					keyColumns[0] = 0;
				} else {
					keyColumns = new int[this.arity];
					for (int i = 0; i < this.arity; i++)
						keyColumns[i] = i;
				}
				indexedOn = Arrays.toString(keyColumns);
			} else {
				indexedOn = Arrays.toString(this.boundColumns);
				keyColumns = this.boundColumns;
			}
			
			String indexBaseRelationName = this.predicateName + "_indexed_on" + indexedOn;
			
			this.relation = relationManager.getBaseRelation(indexBaseRelationName);
			if (this.relation == null) {
				List<BasePredicateStructuralAttribute> bpsas = new ArrayList<>();
				for (DataType column : schema)
					bpsas.add(new BasePredicateStructuralAttribute(column));
				
				BasePredicate basePredicate = new BasePredicate(indexBaseRelationName, bpsas);
				
				TupleStoreConfiguration tsc = new TupleStoreConfiguration(TupleStoreType.TupleBPlusTree, keyColumns);
				tsc.uniqueValue = false;
				
				this.relation = this.module.install(basePredicate, tsc);
				
				if ((this.relation != null) && !this.module.addBasePredicate(basePredicate)) {
					this.relationManager.deleteBaseRelation((BaseRelation<?>)this.relation);
					this.relation = null;
				}
			} //else {
				// empty relation first
			//	this.relation.cleanUp();
			//}

			// this relation might have already been created by previous BaseRelationNode 
			if (this.relation.isEmpty()) {
				// load relation for regular relation
				Cursor cursor = this.database.getCursorManager().createScanCursor(relationManager.getBaseRelation(this.predicateName));
				
				Tuple tuple = cursor.getEmptyTuple();
				while (cursor.getTuple(tuple) > 0)
					this.relation.add(tuple);
	
				this.database.getCursorManager().destroyCursor(cursor);
			}
			
			if (this.bindingPattern.allFree())
				this.cursor = this.database.getCursorManager().createScanCursor(this.relation);
			else
				this.cursor = (Cursor<?>) this.database.getCursorManager().createIndexCursor(this.relation, keyColumns);
		}
		
		if (this.relation == null) {			
			this.logError("No base relation for SchemaRelation for {}", this.predicateName);
			throw new InterpreterException("No base relation for SchemaRelation for " + this.predicateName);
		}
		
		this.determineMatchFreeColumns();
		
		return true;	    
	}
	
	public void setAsBPlusTreeRelation() {		
		this.useBPlusTree = true;
		for (DataType dataType : this.getSchema())
			if (dataType == DataType.UNKNOWN)
				this.useBPlusTree = false;
	}

	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(super.toString());
		
		//if (this.cursor != null)
		//	output.append("\t" + this.cursor.hashCode());
		
		return output.toString();
	}

	@Override
	public Cursor<?> getQueryFormCursor() { return this.database.getCursorManager().createScanCursor(this.relation); }
	
	@Override
	public BaseRelationNode copy(ProgramContext programContext) { 
		BaseRelationNode copy = new BaseRelationNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		copy.executionMode = this.executionMode;
		programContext.getNodeMapping().put(this, copy);
		return copy;
	}
	
	@Override
	public void attachContext(DeALSContext deALSContext) {
		super.attachContext(deALSContext);
		this.database = deALSContext.getDatabase();
		this.module = deALSContext.getModuleManager().getActiveModule();
	}
}