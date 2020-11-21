package edu.ucla.cs.wis.bigdatalog.interpreter;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYPredicateType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ExecutionMode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.RelationNode;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public abstract class Node<T extends Node<?>> {

	public final boolean DEBUG = false;
	protected static int depth;
	protected static int displayIndentLevel;
	public int numberOfRecursiveFactsDerived;
	
	protected String 			predicateName;
	protected NodeArguments 	arguments;
	protected T[]				children;

	protected Binding 			bindingPattern;	
	protected XYPredicateType 	xyPredicateType;
	protected TypeManager		typeManager;

	public Node(String predicateName, NodeArguments args, Binding binding) {
		this.predicateName		= predicateName;
		this.arguments 			= args;
		this.children			= this.createArray(0); 

		this.bindingPattern		= binding;
		this.xyPredicateType 	= XYPredicateType.NONE;
		this.numberOfRecursiveFactsDerived = 0;
	}
	
	public String getPredicateName() { return this.predicateName; }
	
	public String getPredicateNameWithBinding() { 
		if (this.bindingPattern == null)
			return this.predicateName;

		return this.predicateName.toString() + "_" + this.bindingPattern.toString();
	}

	public int getArity() { return this.arguments.size(); }
	
	public NodeArguments getArguments() { return this.arguments; }
	
	public void getVariables(VariableList variableList) {
		Utilities.getVariables(this.getArguments(), variableList);
	}

	public Argument getArgument(int i) { return this.arguments.get(i); }
	
	public DbTypeBase getArgumentAsDbType(int i) { return this.arguments.get(i).toDbType(this.typeManager); }
	
	public Binding getBindingPattern() { return this.bindingPattern; }
	
	public void addChild(T node) {
		T[] temp = this.createArray(this.children.length + 1);
						
		for (int i = 0; i < this.children.length; i++)
			temp[i] = this.children[i];
		this.children = temp;
		this.children[this.children.length - 1] = node;
	}

	public void addChildAtHead(T node) {
		T[] temp = this.createArray(this.children.length + 1);
		for (int i = 0; i < this.children.length; i++)
			temp[i+1] = this.children[i];
		
		this.children = temp;		
		this.children[0] = node;		
	}
	
	public void insertChild(T node, int index) {
		T[] temp = this.createArray(this.children.length + 1);
		
		for (int i = 0; i < index; i++)
			temp[i] = this.children[i];
		
		temp[index] = node;
		
		for (int i = index + 1; i < temp.length; i++)
			temp[i] = this.children[i-1];
		
		this.children = temp;
	}

	public T[] getChildren() {
		return this.children; 
	}
	
	public T getChild(int index) {
		return this.children[index];
	}
	
	public void replaceChild(int index, T node) {
		this.children[index] = node;
	}
	
	public void removeChild(int index) {
		if (index >= this.children.length)
			return;

		if (this.children.length == 1) {
			this.children = null;
			return;
		}
		
		T[] temp = this.createArray(this.children.length - 1);
		for (int i = 0; i < index; i++)
			temp[i] = this.children[i];
		
		for (int i = index + 1; i < this.children.length; i++)
			temp[index++] = this.children[i];
				
		this.children = temp;
	}
	
	public int getNumberOfChildren() {
		return this.children.length;
	}
		
	public boolean hasChildren() {
		return (this.children.length != 0);
	}

	public BindingType getBinding(int i) {
		return this.bindingPattern.getBinding(i);
	}
	
	public void setBinding(int i, BindingType bindingType) {
		this.bindingPattern.setBinding(i, bindingType);
	}
	
	public XYPredicateType getXYPredicateType() { return this.xyPredicateType; }
	
	public void setXYPredicateType(XYPredicateType xyPredicateType) {
		this.xyPredicateType = xyPredicateType;
	}

	public static String toStringIndent() {
		StringBuilder output = new StringBuilder();
		output.append("\n");
		
		for (int i = 0; i < displayIndentLevel; i++)
			output.append(" ");
		
		output.append(displayIndentLevel + ": ");
		
		return output.toString();
	}

	public String toStringNode() {
		StringBuilder output = new StringBuilder();

		if ((this.predicateName.equals(ComparisonOperation.EQUALITY.getSymbol())  
				|| this.predicateName.equals(ComparisonOperation.INEQUALITY.getSymbol()) 
				|| this.predicateName.equals(ComparisonOperation.LESS_THAN_OR_EQUAL.getSymbol()) 
				|| this.predicateName.equals(ComparisonOperation.GREATER_THAN_OR_EQUAL.getSymbol()) 
				|| this.predicateName.equals(ComparisonOperation.LESS_THAN.getSymbol())  
				|| this.predicateName.equals(ComparisonOperation.GREATER_THAN.getSymbol()))
				&& (this.arguments.size() == 2)) {
			output.append(this.getArgument(0).toString());
			output.append(" ");
			output.append(this.predicateName.toString());
			output.append("_");
			output.append(this.bindingPattern.toString());
			output.append(" ");
			output.append(this.getArgument(1).toString());
	    } else {
	    	/* mark it if it's an "I+1" type Node in Y rule of XY Clique --HW */
	    	if (this.xyPredicateType == XYPredicateType.NEW)
	    		output.append("NEW-XY_");
	    	if (this.xyPredicateType == XYPredicateType.OLD)
	    		output.append("OLD-XY_");

	    	output.append(this.predicateName);
	    	if (this.bindingPattern != null && this.bindingPattern.getArity() > 0) {
	    		output.append("_");
	    		output.append(this.bindingPattern);
	    	}
	    	
	    	output.append("(");	    	

    		for (int i = 0; i < this.arguments.size(); i++) {
    			if (i > 0)
    				output.append(", "); 
    			output.append(this.getArgument(i).toString());
	    	}
	    	output.append(")");
	    }

	    output.append(" <");
	    output.append(this.getClass().getSimpleName());
	    if ((this instanceof RelationNode) && ((RelationNode)this).getExecutionMode() == ExecutionMode.Materialized) 
	    	output.append("(**" + ((RelationNode)this).getExecutionMode().toString().toUpperCase() + "**)");
	    output.append(">");
	    output.append("[");
	    output.append(this.hashCode());
	    output.append("]");
		
		return output.toString();
	}
	
	public String toString() {
		return toStringNode();
	}

	public String toStringTree() {
		StringBuilder output = new StringBuilder();
		output.append(Node.toStringIndent());
		output.append(this.toString());

		displayIndentLevel++;

		if (this.hasChildren())
			for (T node : this.getChildren())
				output.append(node.toStringTree());

		displayIndentLevel--;
		
		return output.toString();
	}
	
	public String toStringTreeOutline(int depth) {
		StringBuilder output = new StringBuilder();
		String tabs = "";		
		int d = depth;
		
		while (d > 0) {
			tabs += "\t";
			d--;
		}
		
		output.append("\n");
		output.append(tabs);
		output.append(this.predicateName.toString());
		output.append(" ");
		output.append(this.getClass().getName());
		output.append("\n");
		
		if (this.hasChildren())
			for (T node : this.getChildren())
				output.append(node.toStringTreeOutline(depth + 1));
		
		return output.toString();
	}
	
	public String toStringWithAssignments() {
		StringBuilder output = new StringBuilder();

		if ((this.predicateName.equals(ComparisonOperation.EQUALITY.getSymbol())  
				|| this.predicateName.equals(ComparisonOperation.INEQUALITY.getSymbol()) 
				|| this.predicateName.equals(ComparisonOperation.LESS_THAN_OR_EQUAL.getSymbol()) 
				|| this.predicateName.equals(ComparisonOperation.GREATER_THAN_OR_EQUAL.getSymbol()) 
				|| this.predicateName.equals(ComparisonOperation.LESS_THAN.getSymbol())  
				|| this.predicateName.equals(ComparisonOperation.GREATER_THAN.getSymbol()))
				&& (this.arguments.size() == 2)) {
			output.append(this.getArgument(0).toString());
			output.append(" ");
			output.append(this.predicateName.toString());
			output.append("_");
			output.append(this.bindingPattern.toString());
			output.append(" ");
			output.append(this.getArgument(1).toString());
	    } else {
	    	/* mark it if it's an "I+1" type Node in Y rule of XY Clique --HW */
	    	if (this.xyPredicateType == XYPredicateType.NEW)
	    		output.append("NEW-XY_");
	    	if (this.xyPredicateType == XYPredicateType.OLD)
	    		output.append("OLD-XY_");

	    	output.append(this.predicateName);	    	
	    	output.append("(");	    	

	    	if (this.arguments.size() > 0) {
	    		for (int i = 0; i < this.arguments.size(); i++) {
	    			if (i > 0)
	    				output.append(", "); 
	    			output.append(this.getArgument(i).toString());
	    		}
	    	}
	    	output.append(")");
	    }

		return output.toString();
	}
	
	public void attachContext(DeALSContext deALSContext) {
		this.typeManager = deALSContext.getDatabase().getTypeManager();
		for (Node<?> child : this.getChildren())
			child.attachContext(deALSContext);
	}
	
	abstract protected T[] createArray(int size);
}