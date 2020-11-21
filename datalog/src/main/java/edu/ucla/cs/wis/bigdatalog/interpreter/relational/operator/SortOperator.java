package edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator;

import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.OperatorArguments;

public class SortOperator extends Operator {
	private static final long serialVersionUID = 1L;

	public class SortOrder {
		public int index;
		public boolean ascending;
		
		public SortOrder(int index, boolean ascending) {
			this.index = index;
			this.ascending = ascending;
		}
	}
	
	private List<SortOrder> sortOrders;
	
	public SortOperator(String name, OperatorArguments arguments) {
		super(name, OperatorType.SORT, arguments);
		this.sortOrders = new ArrayList<>();
	}

	public List<SortOrder> getSortOrders() { return this.sortOrders; }
	
	public void addSortOrder(int index, boolean ascending) {
		this.sortOrders.add(new SortOrder(index, ascending));
	}
	
	public String toString() {
		StringBuilder output = new StringBuilder();
   		output.append(this.name);
   		output.append("(");
	   	output.append(this.arguments.toString());
	   	output.append(")[");
	
	   	for (int i = 0; i < this.sortOrders.size(); i++) {
	   		if (i > 0)
	   			output.append(", ");
	   	
	   		output.append("[");
	   		output.append(this.sortOrders.get(i).index);
	   		output.append(",");
	   		if (this.sortOrders.get(i).ascending) 
	   			output.append("ASC");
	   		else 
	   			output.append("DESC");
	   		output.append("]");
	   	}
	   	output.append("]");
	  
	    output.append(" <");
		output.append(this.operatorType.name());
	    output.append(">");
	    //output.append("[");
	    //output.append(this.hashCode());
	    //output.append("]");
		
		return output.toString();
	}
	
}
