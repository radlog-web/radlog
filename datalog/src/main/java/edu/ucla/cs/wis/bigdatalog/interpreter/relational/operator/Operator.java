package edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGAndNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.OperatorArguments;
import java.io.Serializable;
import java.util.ArrayList;

public class Operator implements Serializable {
	private static final long serialVersionUID = 1L;

	protected static int displayIndentLevel;

	protected String name;
	protected OperatorType operatorType;
	protected OperatorArguments arguments;
	protected Operator[] children;	
	protected PCGAndNode pcgAndNode;
	protected boolean cached;
	// only for AGGREGATE_FS
	private int sampleVarPosInArguments = -1;

	public Operator(String name, OperatorType operatorType) {
		this.name = name;
		this.operatorType = operatorType;
		this.arguments = new OperatorArguments();
		this.children = new Operator[0];
	}
	
	public Operator(String name, OperatorType operatorType, OperatorArguments arguments) {
		this(name, operatorType);	
		this.arguments = arguments;
	}
	
	public String getName() { return this.name; }
	
	public void setName(String name) { this.name = name; }
	
	public int getArity() { return this.arguments.size(); }

	public int getSampleVarPosInArguments() {
		return sampleVarPosInArguments;
	}

	public void setSampleVarPosInArguments(int sampleVarPosInArguments) {
		this.sampleVarPosInArguments = sampleVarPosInArguments;
	}

	public ArrayList<Argument> getArgumentsAsArrayList() {
		return this.arguments;
	}

	public OperatorArguments getArguments() { return this.arguments; }

	public void setArguments(OperatorArguments arguments) { this.arguments = arguments; }

	public Argument getArgument(int index) { return this.arguments.get(index); }
	
	public void setOperatorType(OperatorType operatorType) { this.operatorType = operatorType; }
	
	public OperatorType getOperatorType() { return this.operatorType; }

	public int getNumberOfChildren() { 
		if (this.children == null) 
			return 0;
		return this.children.length; 
	}
	
	public Operator[] getChildren() { return this.children; }	
	
	public Operator getChild(int index) { return this.children[index]; }
		
	public void addChild(Operator operator) {
		Operator[] temp = new Operator[this.children.length + 1];

		for (int i = 0; i < this.children.length; i++)
			temp[i] = this.children[i];
		this.children = temp;
		this.children[this.children.length - 1] = operator;		
	}
	
	public void addChildAtHead(Operator operator) {
		Operator[] temp = new Operator[this.children.length + 1];
		for (int i = 0; i < this.children.length; i++)
			temp[i+1] = this.children[i];
		
		this.children = temp;
		this.children[0] = operator;		
	}
	
	public void insertChild(Operator operator, int index) {
		Operator[] temp = new Operator[this.children.length + 1];
		
		for (int i = 0; i < index; i++)
			temp[i] = this.children[i];
		
		temp[index] = operator;
		
		for (int i = index + 1; i < temp.length; i++)
			temp[i] = this.children[i-1];
		
		this.children = temp;
	}
	
	public void replaceChild(int index, Operator operator) {
		this.children[index] = operator;
	}
	
	public void removeChild(int index) {
		if (index >= this.children.length)
			return;

		if (this.children.length == 1) {
			this.children = null;
			return;
		}
		
		Operator[] temp = new Operator[this.children.length - 1];
		for (int i = 0; i < index; i++)
			temp[i] = this.children[i];
		
		for (int i = index + 1; i < this.children.length; i++)
			temp[index++] = this.children[i];
		
		this.children = temp;
	}
	
	public void setPCGNode(PCGAndNode pcgNode) { this.pcgAndNode = pcgNode; }
	
	public PCGAndNode getPCGAndNode() { return this.pcgAndNode; }
	
	public void cached(boolean cached) { this.cached = cached; }
	
	public boolean cached() { return this.cached; }
	
	public String toStringTree() {
		StringBuilder output = new StringBuilder();
		output.append(Operator.toStringIndent());
		output.append(this.toString());

		displayIndentLevel++;

		for (Operator operator : this.children)
			output.append(operator.toStringTree());

		displayIndentLevel--;
		
		return output.toString();
	}
	
	public String toString() {
		StringBuilder output = new StringBuilder();
	
		if (((this.operatorType == OperatorType.COMPARISON) 
				|| (this.operatorType == OperatorType.ASSIGNMENT)) 
				&& (this.getArguments().size() == 2)) {
			output.append(this.getArguments().get(0).toString());
			output.append(" ");
			output.append(this.name);
			output.append(" ");
			output.append(this.getArguments().get(1).toString());
	    } else {
	    	if ((this.getOperatorType() != OperatorType.AGGREGATE) && (this.getOperatorType() != OperatorType.AGGREGATE_FS))
	    		output.append(this.name);
	    	output.append("(");
	    	output.append(this.arguments.toString());
	    	output.append(")");
	    }

	    output.append(" <");
		output.append(this.operatorType.name());
	    output.append(">");
	    //output.append("[");
	    //output.append(this.hashCode());
	    //output.append("]");
		
		return output.toString();
	}
	
	public static String toStringIndent() {
		StringBuilder output = new StringBuilder();
		output.append("\n");
		
		for (int i = 0; i < displayIndentLevel; i++)
			output.append(" ");
		
		output.append(displayIndentLevel + ": ");
		
		return output.toString();
	}
}
