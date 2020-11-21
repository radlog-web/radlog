package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.comparison;

import java.util.regex.Pattern;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.interpreter.Interpreter;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;

public class LikeNode 
	extends ComparisonNode {	
	protected Interpreter interpreter;
	private Pattern pattern;
	private boolean hasConstantPattern;
	private boolean isLike;
	
	public LikeNode(NodeArguments args, Binding binding, VariableList freeVariables, boolean isLike) {
		super(BuiltInPredicate.LIKE_PREDICATE_NAME, args, binding, freeVariables);
		this.isLike = isLike;
		if (this.getArgument(1).reduce().isConstant()) {
			this.setPattern();
			this.hasConstantPattern = true;
		}		
	}
	
	@Override
	public void cleanUp() {
		this.baseNodeCleanUp();
	}
	
	private void setPattern() {
		//Pattern p = Pattern.compile(".*A.*B.*");
		this.pattern = Pattern.compile("^" + this.getArgument(1).reduce().toString().trim().replace("%", ".*?") + "$");
	}
	
	@Override
	public Status getTuple() {
		Status status;
	  
		this.traceGetTupleEntry();

		if (this.isEntry) {
			// we compare the expression from 'right' against the value from 'left' 
			// 'right' has to be a string after reduce
			// we compare against the string representation of 'left'
			if (!this.hasConstantPattern)
				this.setPattern();

			String value = this.getArgument(0).reduce().toString().trim();

			if (this.pattern.matcher(value).matches() == this.isLike) {
				status = Status.SUCCESS;
				this.isEntry = false;
			} else {
				status = Status.FAIL;
			}	
	    } else {
	    	this.isEntry = true;
	    	status = Status.ENTRY_FAIL;
	    }

		this.traceGetTupleExit(status);

		return status;
	}
	
	@Override
	public LikeNode copy(ProgramContext programContext) {
		LikeNode copy = new LikeNode(programContext.copyArguments(this.arguments), this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList), this.isLike);
		
		programContext.getNodeMapping().put(this, copy);
		
		return copy;
	}
}
