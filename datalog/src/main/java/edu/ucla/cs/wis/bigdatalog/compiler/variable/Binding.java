package edu.ucla.cs.wis.bigdatalog.compiler.variable;

import edu.ucla.cs.wis.bigdatalog.compiler.CompilerException;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;

public class Binding extends CompilerTypeBase {
	protected BindingType[] bindingPattern;
	
	public Binding(int arity) {
		this(arity, BindingType.UNKNOWN);
	}
	
	public Binding(int arity, BindingType bindingType) {
		super(CompilerType.BINDING);
		
		if (arity < 0)
			throw new CompilerException("Invalid binding.  Can not have binding with arity < 0");
		
		if (arity == 0) {
			this.bindingPattern = null;
		} else { 
			this.bindingPattern = new BindingType[arity];			
			this.initializeBindingPattern(bindingType);
		}
	}

	public int getArity() {
		if (this.bindingPattern == null)
			return 0;
		return this.bindingPattern.length; 
	}

	public boolean hasNoBinding() {
		return ((this.bindingPattern == null) 
				|| (this.bindingPattern[0] == BindingType.UNKNOWN));
	}

	public void setAsNoBinding() {
		if (this.getArity() > 0)
			this.bindingPattern[0] = BindingType.UNKNOWN;
	}

	public void setAsAllFreeBinding() {
		this.initializeBindingPattern(BindingType.FREE);
	}
	
	public void setAsAllBoundBinding() {
		this.initializeBindingPattern(BindingType.BOUND);
	}

	public BindingType getBinding(int position) {
		// APS said on 3/7/2013 - this is to avoid having u bindings set to b
		return this.bindingPattern[position] == BindingType.BOUND ? BindingType.BOUND : BindingType.FREE;
	}

	public boolean allBound() {
		return this.allArgumentAreOfType(BindingType.BOUND);
	}

	public boolean allFree(){
		return this.allArgumentAreOfType(BindingType.FREE);
	}

	public Binding copy() {
		Binding binding = new Binding(this.getArity());
		binding.assignBinding(this);
		return binding;
	}

	public String toString() {
		StringBuilder retval = new StringBuilder();
		for (int i = 0; i < this.getArity(); i++)
			retval.append(this.bindingPattern[i]);
		
		return retval.toString();
	}

	public String toStringNthBinding(int position) {
		return String.valueOf(this.bindingPattern[position]);
	}

	public int getNumberOfBoundArguments() {
		return this.getNumberOfArgumentsOfBindingType(BindingType.BOUND);
	}

	public int getNumberOfFreeArguments() {
		return this.getNumberOfArgumentsOfBindingType(BindingType.FREE);
	}

	public void setBinding(int position, BindingType bindingType) {
		if (position < 0 || position >= this.getArity())
			throw new CompilerException("Position requested is out of range for binding");
			
		if (0 <= position || position < this.getArity())
			this.bindingPattern[position] = bindingType;
	}

	protected void initializeBindingPattern(BindingType bindingType) {
		for (int i = 0; i < this.getArity(); i++)
			this.bindingPattern[i] = bindingType;
	}

	protected boolean allArgumentAreOfType(BindingType bindingType) {
		for (int i = 0; i < this.getArity(); i++)
			if (this.bindingPattern[i] != bindingType)
				return false;
		
		return true;
	}

	public String createBindingString() {
		StringBuilder retval = new StringBuilder();
		
		for (int i = 0; i < this.getArity(); i++)
			retval.append(this.bindingPattern[i]);			
		
		return retval.toString();
	}

	public void assignBinding(Binding binding) {
		if (binding.getArity() != this.getArity())
			throw new CompilerException("Can not assign binding.  binding exceeds current arity. Arity: " + this.getArity() + " binding.arity: " + binding.getArity());

		if (binding.hasNoBinding()) {
			this.setAsNoBinding();
		} else {
			for (int i = 0; i < this.getArity(); i++)
				this.bindingPattern[i] = binding.bindingPattern[i];
	    }
	}

	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof Binding))
			return false;
		
		Binding otherBinding = (Binding)other;
		
		boolean status = false;

		if (this.getArity() == otherBinding.getArity()) {
			if (this.hasNoBinding() && otherBinding.hasNoBinding()) {
				status = true;
			} else if (!this.hasNoBinding() && !otherBinding.hasNoBinding()) {
				status = true;
				for (int i = 0; i < this.getArity(); i++) {
					if (this.bindingPattern[i] != otherBinding.getBinding(i)) {
						status = false;
						break;
					}
				}				
			} else {
				status = false;
			}
	    }

		return status;
	}

	protected int getNumberOfArgumentsOfBindingType(BindingType bindingType) {
		int count;
		
		if (this.hasNoBinding()) {
			count = -1;
		} else {
			count = 0;
			for (int i = 0; i < this.getArity(); i++) {
				if (this.bindingPattern[i] == bindingType)
					count++;
			}
		}
		
		return count;
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		throw new RuntimeException("Binding type can not be copied with variable list.");
	}
}
