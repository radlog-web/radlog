package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYPredicateType;
import edu.ucla.cs.wis.bigdatalog.database.cursor.CursorManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.EvaluationType;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;

public class LinearRecursiveLiteral 
	extends RecursiveLiteral {

	public LinearRecursiveLiteral(String predicateName, NodeArguments args, Binding binding, 
			VariableList freeVariables, String recursiveRelationName) {
		super(predicateName, args, binding, freeVariables, recursiveRelationName);
	}
	
	@Override
	public boolean initialize() {
		if (super.initialize()) {
			CursorManager cursorManager = this.database.getCursorManager();
			if (this.getXYPredicateType() == XYPredicateType.NONE) { 
				//if (this.evaluationType == EvaluationType.EagerMonotonic) {
					// TODO APS fix for bound situations
					//if (this.bindingPattern.allFree()) {
				//	int[] columns = new int[]{0,1};
				//	if ((this.relation.getArity() == columns.length) 
				//			&& this.fsAggregatePredicateType != FSAggregateType.NONE)
				//		columns = new int[]{0};
					
					/*if (this.relation.getTupleStore() instanceof AddressedTupleStore) {
						this.cursor = CursorManager.createTupleBPlusTreeIndexScanCursor(this.relation, columns);				
						this.relation.removeSecondaryIndex((SecondaryIndex<?>) ((TupleBPlusTreeIndexScanCursor)this.cursor).getIndex());
					}*/
					/*} else {
						this.cursor = (Cursor) CursorManager.createIndexCursor(this.relation, this.boundColumns);
						this.relation.removeSecondaryIndex((SecondaryIndex<?>) ((TupleBPlusTreeIndexScanCursor)this.cursor).getIndex());
					}*/
				//	this.cursor = CursorManager.createChangeTrackerCursor(this.relation);
				//} else 
				if (this.evaluationType == EvaluationType.EagerMonotonic) {
					this.cursor = cursorManager.createChangeTrackerCursor(this.relation, this.deALSContext.getConfiguration());
				} else if (this.evaluationType == EvaluationType.SSC) {
					this.cursor = cursorManager.createChangeTrackerCursor(this.relation, this.deALSContext.getConfiguration());
				} else {
					if (this.bindingPattern.allFree())
						this.cursor = cursorManager.createSemiNaiveCursor(this.relation, this.boundColumns);
					else
						this.cursor = cursorManager.createIndexSemiNaiveCursor(this.relation, this.boundColumns);
				}
			} else {
				this.cursor = cursorManager.createXYCursor(this.relation, this.boundColumns, this.deALSContext, this.database);
				this.evaluationType = EvaluationType.XY;
			}
			
			this.determineMatchFreeColumns();
			
			if (DEBUG && this.deALSContext.isDerivationTrackingEnabled())
				this.logDerivationTracking("{} will be executing as {}", this.predicateName, this.evaluationType.name());
			
			return true;
		}
		return false;
	}	
	
	@Override
	public LinearRecursiveLiteral copy(ProgramContext programContext) {
		LinearRecursiveLiteral copy = new LinearRecursiveLiteral(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList), 
				new String(this.recursiveRelationName));
		
		copy.evaluationType = this.evaluationType;
		copy.fsAggregatePredicateType = this.fsAggregatePredicateType;
		copy.executionMode = this.executionMode;
		copy.xyPredicateType = this.xyPredicateType;
		
		programContext.getNodeMapping().put(this, copy);
		
		return copy;
	}
}
