package edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph;

import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.PredicateOperatorType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.PredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerFunctor;
import edu.ucla.cs.wis.bigdatalog.compiler.type.expression.CompilerArithmeticExpression;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;

public abstract class PCGNode<T extends PCGNodeChild> extends CompilerTypeBase {
	private static final long serialVersionUID = 1L;

	// attributes of a predicate
	protected Predicate predicate;
	
	// members for graph
	protected List<T> children;
	
	protected boolean noBacktrack;
	
	// members for binding 
	protected Binding binding;
	protected Binding executionBinding;	
	protected BindingType xyStageBinding; /*for recursive xy node. --HW*/
	public CompilerTypeList originalArguments; // so we can tell what was removed by xystagevariable removal  
	
	abstract void unsetChildrenVariables();
	
	public PCGNode(String predicateName, CompilerTypeList arguments, CompilerType compilerType) {
		super(compilerType);
		this.predicate = new Predicate(predicateName, arguments, compilerType);
		this.children = new ArrayList<>();
		this.binding = new Binding(arguments.size());
		this.executionBinding = new Binding(arguments.size());
		this.noBacktrack = false;
	}
	
	public PCGNode(String predicateName, CompilerTypeList arguments, CompilerType compilerType, 
			BuiltInPredicateType builtInPredicateType) {
		super(compilerType);
		
		if (builtInPredicateType == BuiltInPredicateType.UNKNOWN)
			this.predicate = new Predicate(predicateName, arguments);
		else
			this.predicate = new BuiltInPredicate(predicateName, arguments, builtInPredicateType);			

		this.children = new ArrayList<>();
		this.binding = new Binding(arguments.size());
		this.executionBinding = new Binding(arguments.size());

		this.noBacktrack = false;
	}
	
	public Predicate getPredicate() { return this.predicate; }
	
	public String getPredicateName() { return this.predicate.getPredicateName(); }
	
	public void setPredicateName(String predicateName) { 
		this.predicate.setPredicateName(predicateName);
	}
	
	public int getArity() { return this.predicate.getArity(); }
		
	public CompilerTypeList getArguments() { return this.predicate.getArguments(); }
	
	public void setArguments(CompilerTypeList arguments) {
		this.predicate.setArguments(arguments);
	}
	
	public CompilerTypeBase getArgument(int position) {
		return this.predicate.getArguments().get(position);
	}	
	
	public void overwriteNthArgument(int position, CompilerTypeBase object) {
		this.predicate.getArguments().set(position, object);
	}
	
	public void removeArgument(int position) {
		this.predicate.removeArgument(position);
		
		Binding binding1 = new Binding(this.getBindingPattern().getArity() - 1);
		for (int i = 0; i < this.getBindingPattern().getArity(); i++) {
			if (i == position)
				continue;
			binding1.setBinding(i-1, this.getBindingPattern().getBinding(i));
		}
		this.replaceBindingPattern(binding1);
		
		Binding binding2 = new Binding(this.getExecutionBindingPattern().getArity() - 1);
		for (int i = 1; i < this.getExecutionBindingPattern().getArity(); i++) {
			if (i == position)
				continue;
			binding2.setBinding(i-1, this.getExecutionBindingPattern().getBinding(i));
		}
		
		this.replaceExecutionBindingPattern(binding2);
	}
		
	public PredicateType getPredicateType() { return this.predicate.getPredicateType(); }
	
	public void setPredicateType(PredicateType type) { 
		this.predicate.setPredicateType(type); 
	}
	
	public PredicateOperatorType getPredicateOperatorType() { return this.predicate.getPredicateOperatorType(); }
	
	public void setPredicateOperatorType(PredicateOperatorType type) {
		this.predicate.setPredicateOperatorType(type);
	}
	
	public BuiltInPredicateType getBuiltInPredicateType() {
		if (this.predicate instanceof BuiltInPredicate)
			return ((BuiltInPredicate)predicate).getBuiltInPredicateType();
		
		return BuiltInPredicateType.UNKNOWN;
	}
	
	public void setBuiltInPredicateType(BuiltInPredicateType builtInPredicateType) {
		if (builtInPredicateType == BuiltInPredicateType.UNKNOWN)
			return;

		if (this.predicate == null)
			return;
		
		// we have a BuiltInPredicate
		if (this.predicate instanceof BuiltInPredicate) {
			((BuiltInPredicate)this.predicate).setBuiltInPredicateType(builtInPredicateType);
		} else {
			BuiltInPredicate bip = new BuiltInPredicate(this.getPredicateName(), this.getArguments(), builtInPredicateType);
			if (this.predicate.isRecursive())
				bip.setAsRecursive();
			
			this.predicate = bip;
			/*boolean isRecursive = this.predicate.isRecursive();
			// we need a BuiltInPredicate
			this.predicate = new BuiltInPredicate(this.getPredicateName(), this.getArguments(), builtInPredicateType);
			if (isRecursive)
				this.predicate.setAsRecursive();*/
		}	
	}
	
	public boolean isBuiltInPredicate() {
		return (this.predicate instanceof BuiltInPredicate);
	}
	
	public List<T> getChildren() {return this.children;}

	public int getNumberOfChildren() {return (this.children.size());}

	public T getChild(int i) {return this.children.get(i);}

	public void setChild(int i, T node) {
		this.children.set(i, node);
	}

	public void insertChild(int i, T node) {
		this.children.add(i, node);
	}

	public void prependChild(T node) {
		this.children.add(0, node);
	}

	public void addChild(T node) {
		this.children.add(node);
	}

	public void removeChild(T node) {
		this.children.remove(node);
	}

	public T removeChild(int i) {
		return this.children.remove(i);
	}

	public T getLastChild() {
		return this.children.get(this.getNumberOfChildren() - 1);
	}

	public String toString() {
		return this.toStringAsPredicate();
	}

	public void setNoBacktrack(boolean value) { this.noBacktrack = value; }

	public boolean noBacktrack() { return this.noBacktrack; }

	public String toStringAsPredicate() {
		StringBuilder retval = new StringBuilder();
		
		if (this.binding.hasNoBinding()) {
			retval.append(this.predicate.toString());
		} else {
			String oldPredicateName = this.getPredicateName();
			this.setPredicateName(this.createPredicateNameWithBinding());
			retval.append(this.predicate.toString());
			this.setPredicateName(oldPredicateName);
	    }
		
		return retval.toString();
	}
	
	public String createPredicateNameWithBinding() {
		if (this.binding.hasNoBinding())
			return this.getPredicateName();
		
		String bindingString = this.binding.createBindingString();

		if (this.xyStageBinding != null)
			bindingString += this.xyStageBinding.toString();

		if (this.executionBinding.hasNoBinding())
			return this.getPredicateName() + "_" + bindingString;
		
		String executionBindingString = this.executionBinding.createBindingString();

		if (this.xyStageBinding != null)
			executionBindingString += this.xyStageBinding.toString();

		return this.getPredicateName() + "_" + bindingString + "_" + executionBindingString;
	}

	protected void unsetChildrenVariablesAux() {
		for (T node : this.children) {
			node.unsetChildrenVariables();
			node.unsetVariables();
		}
	}

	public void unsetVariables() {
		this.unsetVariables(this.getArguments());
	}

	protected void unsetVariables(CompilerTypeBase object) {
		switch (object.getType())
		{
	    	case VARIABLE:
	    	{
	    		((CompilerVariable) object).makeFree();
	    	}
	    		break;
	    	case COMPILER_FUNCTOR:
	    	{
	    		this.unsetVariables(((CompilerFunctor)object).getArguments());
	    	}
	    		break;

	    	case COMPILER_LIST:
	    	{
	    		CompilerList list = (CompilerList) object;
		    
	    		if (!list.isEmpty()) {
	    			this.unsetVariables(list.getHead());
	    			this.unsetVariables(list.getTail());
	    		}
	    	}
	    		break;

	    	case COMPILER_TYPE_LIST:
	    	{
	    		CompilerTypeList objectList = (CompilerTypeList) object;
	    		
	    		for (CompilerTypeBase compilerTypeObject : objectList)
	    			this.unsetVariables(compilerTypeObject);
	    	}
	    		break;
	    
	    	case ARITHMETIC_EXPRESSION:
	    	{
	    		CompilerArithmeticExpression cae = (CompilerArithmeticExpression)object;
	    		this.unsetVariables(cae.getArgument1());
	    		if (cae.isBinary())
	    			this.unsetVariables(cae.getArgument2());
	    	}
	    		break;
	    		
	    	case COMPILER_STRING:
	    	case COMPILER_INT:
	    	case COMPILER_LONG:
	    	case COMPILER_LONGLONG:
	    	case COMPILER_LONGLONGLONGLONG:
	    	case COMPILER_DATETIME:
	    	case COMPILER_FLOAT:
			case COMPILER_BYTE:
			case COMPILER_SHORT:
			case COMPILER_DOUBLE:
	    	case INPUT_VARIABLE:
	    	default:
	    		break;
	   	}
	}
	
	/* After eliminating xy stage variable in pcg node, binding size should be decreased by one. -- HW*/
	public void resetArgumentsForXYNode(CompilerTypeList arguments) {
		Binding binding = new Binding(arguments.size());
		CompilerTypeBase arg;
		int i = 0, j = 0;
	  
		//copy binding and delete arg J+1
		while (i < this.getArity()) {
			arg = this.getArgument(i);
			if (arg == arguments.get(j)) {
				//new_binding.setBinding(j, this.getBinding(i));
				j++;
			} else if ((i == 0) && (arg.getType() == CompilerType.ARITHMETIC_EXPRESSION)) {
				this.overwriteNthArgument(i, null);
			}
			i++;
		}

		// set internal stage variable binding info
		// APS said on 3/8/2013 - changed to any node that has a stage argument, not just recursive nodes
		//if (this.recursiveP()) {
		// APS said on 3/13/2013
		// this is always free, since this is now called before adornment begins
		if (this.getArguments().size() > arguments.size()) 
			this.xyStageBinding = this.getBinding(0);
	  
		this.originalArguments = this.getArguments().copyList();
		
		//set arguments
		this.setArguments(arguments);

		//set binding
		this.binding = binding;
		this.executionBinding = new Binding(this.getArity());
	}

	public Binding getBindingPattern() {return this.binding;}

	public void setBindingPattern(Binding binding) {this.binding.assignBinding(binding);}

	public void setBindingPattern(int i, BindingType bindingType) {
		this.binding.setBinding(i, bindingType);
	}

	public void clearBindingPattern() {this.binding.setAsAllFreeBinding();}

	public void replaceBindingPattern(Binding binding) {this.binding = binding;}

	public BindingType getBinding(int i) {return this.binding.getBinding(i);}

	public Binding getExecutionBindingPattern() {return this.executionBinding;}

	public void setExecutionBindingPattern(Binding binding) {
		this.executionBinding.assignBinding(binding);
	}

	public void setExecutionBindingPattern(int i, BindingType bindingType) {
		this.executionBinding.setBinding(i, bindingType);
	}

	public void clearExecutionBindingPattern() {
		this.executionBinding.setAsAllFreeBinding();
	}

	public void replaceExecutionBindingPattern(Binding binding) {
		this.executionBinding = binding;
	}

	public BindingType getExecutionBinding(int i) { return this.executionBinding.getBinding(i); }
	
	public BindingType getXYStageVariableBinding() { return this.xyStageBinding; }
	
	public boolean hasXYStageVariableBinding() { return (this.xyStageBinding != null); }
}