package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYPredicateType;
import edu.ucla.cs.wis.bigdatalog.database.cursor.CursorManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.EvaluationType;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;

public class NonLinearRecursiveLiteral 
	extends RecursiveLiteral {
	
	public NonLinearRecursiveLiteral(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables, 
			String recursiveRelationName) {
		super(predicateName, args, binding, freeVariables, recursiveRelationName);
	}

	@Override
	public boolean initialize() {
		if (super.initialize()) {
			CursorManager cursorManager = this.database.getCursorManager();
			if (this.getXYPredicateType() == XYPredicateType.NONE) {
				if (this.evaluationType == EvaluationType.EagerMonotonic) {
					this.cursor = cursorManager.createCursor(this.relation, this.boundColumns);
				} else {
					if (this.boundColumns != null) {
						this.cursor = cursorManager.createIndexNaiveCursor(this.relation, this.boundColumns);					
					} else
						this.cursor = cursorManager.createNaiveCursor(this.relation, this.boundColumns);
				}
			} else {
				// if we have bound columns, add an index so we can delete matching tuples during stage incrementing
				if (this.boundColumns != null)
					this.relation.addSecondaryIndex(this.boundColumns);				
				
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
	public NonLinearRecursiveLiteral copy(ProgramContext programContext) {
		NonLinearRecursiveLiteral copy = new NonLinearRecursiveLiteral(new String(this.predicateName), 
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