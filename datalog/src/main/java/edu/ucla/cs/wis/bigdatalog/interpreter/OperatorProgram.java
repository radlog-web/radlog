package edu.ucla.cs.wis.bigdatalog.interpreter;

import java.io.Serializable;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.api.RecursionInformation;
import edu.ucla.cs.wis.bigdatalog.compiler.ProgramRules;
import edu.ucla.cs.wis.bigdatalog.database.relation.BaseRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.OperatorArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.Operator;

public class OperatorProgram extends Program<Operator> implements Serializable {
	private static final long serialVersionUID = 1L;

	private Hints hints;

	public OperatorProgram(){}
	
	public OperatorProgram(Operator root, ProgramRules programRules) {
		super(root, programRules);
	}
	
	public Argument getArgument(int position) {
		return this.root.getArguments().get(position);
	}
	
	public OperatorArguments getArguments() { return this.root.getArguments(); }

	public Hints getHints() {
		return hints;
	}

	public void setHints(Hints hints) {
		this.hints = hints;
	}

	public boolean isRecursive() {
		return isRecursive(this.root);
	}
	
	private boolean isRecursive(Operator operator) {
		if (operator.getOperatorType().isRecursive())
			return true;
		
		for (Operator child : operator.getChildren())
			if (isRecursive(child))
				return true;
				
		return false;
	}

	@Override
	public List<RecursionInformation> getRecursionInformationByClique() { return null; }

	@Override
	public boolean initialize() { return true; }
		
	@Override
	public String toString() { return this.root.toStringTree(); }

	@Override
	public Status execute(Relation<?> resultRelation) { return Status.FAIL; }
	
	@Override
	public Status execute() { return Status.FAIL; }

	@Override
	public void compressBoundArguments() { }

	@Override
	protected String getNodeInfo(Operator node) { return null; }

	@Override
	protected boolean isUsedInProgram(BaseRelation<?> baseRelation, Operator node) { return false; }

	@Override
	public void cleanUp() { }
}
