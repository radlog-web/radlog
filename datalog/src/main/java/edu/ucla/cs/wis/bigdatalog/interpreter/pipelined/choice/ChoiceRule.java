package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.choice;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.AndNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;

public class ChoiceRule 
	extends AndNode {
	protected NodeList<ChoicePredicate> choicePredicates;
	protected long numberOfChoicePredicates;

	public ChoiceRule(String predicateName, NodeArguments args, Binding binding) {
		super(predicateName, args, binding);
		this.choicePredicates = new NodeList<>();
		this.numberOfChoicePredicates = 0;
	}

	public Status getTuple() {
		Status	status;
		int 	numberOfChildren = this.getNumberOfChildren();
		int		index;

		this.traceGetTupleEntry();

		if (!this.isEntry && (this.ruleBacktrackPoint != this.currentChildIndex)) {
			// If we are here, then we are backtracking into this rule.
			// And the last literal of this rule does not contribute
			// any variables to the head.

			this.cleanUpLiteralsInRange(this.ruleBacktrackPoint, numberOfChildren - 1);

			// If this.ruleBacktrackPoint is equal to -1, no literals can contribute
			// any new values and thus, we should fail out of this and node, K.L.Ong 2/28/93
			if (this.ruleBacktrackPoint == -1) {
				this.currentChildIndex = 0;				  
				this.traceGetTupleExit(Status.FAIL);
				this.isEntry = true;
				return(Status.FAIL);
			}
			index = this.ruleBacktrackPoint;
		} else {
			index = this.currentChildIndex;
		}

		while (index < numberOfChildren) {
			status = this.getChild(index).getTuple();
			//Node.numberOfPredicateEvaluations++;

			switch (status) 
			{
			case SUCCESS:
				index++;
				break;

			case BACKTRACK_SUCCESS:
				this.updateChoicePredicates();
				status = Status.FAIL;

			case FAIL:
			{
				if (index == 0) {
					this.currentChildIndex = 0;
					this.traceGetTupleExit(status);
					this.isEntry = true;
					return status;
				}
				// force backtracking
				index--;
			}
			break;

			case ENTRY_FAIL:
			{
				if (index == 0) {
					this.currentChildIndex = 0;
					this.traceGetTupleExit(Status.FAIL);
					this.isEntry = true;
					return Status.FAIL;
				}
				this.cleanUpLiteralsInRange(this.backtrackMap[index], index - 1);

				if (this.backtrackMap[index]  == -1) {
					this.currentChildIndex = 0;
					this.traceGetTupleExit(Status.FAIL);
					this.isEntry = true;
					return status;
				}
				index = this.backtrackMap[index];
			}
			break;
			}
		}

		this.currentChildIndex = numberOfChildren - 1;
		this.updateChoicePredicates();
		this.isEntry = false;

		this.traceGetTupleExit(Status.SUCCESS);

		return Status.SUCCESS;
	}

	public void addChoicePredicate(ChoicePredicate predicate) {
		this.choicePredicates.add(predicate);
		this.numberOfChoicePredicates++;
	}

	public ChoicePredicate getIndexedChoicePredicate(int index) {
		return this.choicePredicates.get(index);
	}

	public void updateChoicePredicates() {
		for (ChoicePredicate predicate : this.choicePredicates)
			predicate.memoValue();
	}
	
	@Override
	public ChoiceRule copy(ProgramContext programContext) {
		ChoiceRule copy = new ChoiceRule(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy());
		
		copy.backtrackMap = this.backtrackMap;
		copy.ruleBacktrackPoint = this.ruleBacktrackPoint;
		
		programContext.getNodeMapping().put(this, copy);
		
		for (int i = 0; i < this.getNumberOfChildren(); i++)
			copy.addChild(this.getChild(i).copy(programContext));
				
		copy.numberOfChoicePredicates = this.numberOfChoicePredicates;		
		
		for (ChoicePredicate cp : this.choicePredicates)
			copy.choicePredicates.add((ChoicePredicate) programContext.getNodeMapping().get(cp));

		return copy;
	}
}
