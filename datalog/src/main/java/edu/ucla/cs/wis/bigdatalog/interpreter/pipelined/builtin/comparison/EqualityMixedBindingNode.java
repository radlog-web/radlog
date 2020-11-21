package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.comparison;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;

public class EqualityMixedBindingNode extends ComparisonNode {

	enum EqualityMixedNodeType {BF, FB}

	protected EqualityMixedNodeType equalityMixedNodeType; 
	protected final int firstArgIndex;
	protected final int secondArgIndex;
	protected final boolean hasArithmeticArguments;
	protected boolean firstArgIsVariable;
	protected Variable firstArgVariable;
	
	public EqualityMixedBindingNode(NodeArguments args, Binding binding, VariableList freeVariables) {
		super(BuiltInPredicate.EQUALITY_PREDICATE_NAME, args, binding, freeVariables);

		// there are only 2 choices
		if (binding.getBinding(0) == BindingType.BOUND) {
			this.equalityMixedNodeType = EqualityMixedNodeType.BF;
			//this.argumentIndex = new int[]{1,0};
			this.firstArgIndex = 1;
			this.secondArgIndex = 0;
		} else {
			this.equalityMixedNodeType = EqualityMixedNodeType.FB;
			//this.argumentIndex = new int[]{0,1};
			this.firstArgIndex = 0;
			this.secondArgIndex = 1;
		}

		this.hasArithmeticArguments = this.hasArithmeticArguments();		
	}

	@Override
	public boolean initialize() {
		if (!super.initialize())
			return false;
		
		this.firstArgIsVariable = (this.arguments.innerArguments[this.firstArgIndex] instanceof Variable);

		// do this here instead of constructor because of variable compression done by the program generator
		if (this.firstArgIsVariable)
			this.firstArgVariable = (Variable)this.arguments.innerArguments[this.firstArgIndex];
		else
			this.firstArgVariable = null;
		
		return true;
	}
	
	@Override
	public void cleanUp() {
		//this.baseNodeCleanUp();
		this.isEntry = true;
		this.currentChildIndex = 0;
		
		this.freeVariableList.makeFree();
	}
	
	@Override
	public void partialCleanUp() {
		this.isEntry = true;
		this.currentChildIndex = 0;
	}

	@Override
	public Status getTuple() {
		Status status;

		this.traceGetTupleEntry();

		for (int i = 0; i < this.freeVariableList.variables.length; i++)
			this.freeVariableList.variables[i].makeFree();

		if (this.isEntry) {
			if (this.hasArithmeticArguments) {
				Argument reducedBoundObject = this.arguments.innerArguments[this.secondArgIndex].reduce();
				
				if (this.firstArgIsVariable) {
					this.firstArgVariable.setValue(reducedBoundObject);
					status = Status.SUCCESS;
					this.isEntry = false;
				} else {
					Argument reducedFreeObject = this.arguments.innerArguments[this.firstArgIndex].reduce();
					if (reducedFreeObject instanceof Variable) {
						((Variable)reducedFreeObject).setValue(reducedBoundObject);
						status = Status.SUCCESS;
						this.isEntry = false;
					} else if (this.solveFreeBound2(reducedFreeObject, (DbTypeBase) reducedBoundObject)) {
						status = Status.SUCCESS;
						this.isEntry = false;
					} else {
						status = Status.FAIL;
						this.cleanUp();
					}
				}
			} else if (this.arguments.innerArguments[this.firstArgIndex].matchByFree(this.arguments.innerArguments[this.secondArgIndex])) {
				status = Status.SUCCESS;
				this.isEntry = false;
			} else {
				status = Status.FAIL;
				this.cleanUp();
			}
		} else {
			status = Status.ENTRY_FAIL;
			this.cleanUp();
		}

		this.traceGetTupleExit(status);

		return status;
	}

	private boolean solveFreeBound2(Argument freeObject, DbTypeBase boundObject) {
		boolean status = false;

		if (freeObject instanceof Variable) {
			((Variable)freeObject).setValue(boundObject);
			status = true;
		} else {
			throw new InterpreterException("Invalid argument type for arithmetic operation");
		}

		return status;
	}
	
	@Override
	public EqualityMixedBindingNode copy(ProgramContext programContext) {
		EqualityMixedBindingNode copy = new EqualityMixedBindingNode(programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		
		programContext.getNodeMapping().put(this, copy);
		
		return copy;
	}
}
