package edu.ucla.cs.wis.bigdatalog.compiler;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

public class Rule extends CompilerTypeBase implements Serializable{
	private static final long serialVersionUID = 1L;
	protected String id;
	protected Predicate head;
	protected List<Predicate> body;
	protected boolean isRewritten;			// true if the rule was rewritten
	protected boolean isResultOfRewrite;	// true if the rule is a result of a rewrite
	protected boolean allFunctionsMonotonic;
	protected boolean generateSealingAggregate;
	protected MonotonicRuleType monotonicRuleType;
	protected Rule originalRule; // to track the original program
	
	public Rule(String id, Predicate head, List<Predicate> body) {
		super(CompilerType.RULE);
		this.id = id;
		this.head = head;

		if (body == null)
			this.body = new ArrayList<>();
		else
			this.body = body;
		
		this.isRewritten = false;
		this.isResultOfRewrite = false;
		this.allFunctionsMonotonic = false;
		this.generateSealingAggregate = false;
		this.monotonicRuleType = MonotonicRuleType.N2N;
	}

	public String getRuleId() { return this.id; }

	public Predicate getHead() { return this.head; }

	public List<Predicate> getBody() { return this.body; }

	public boolean isRewritten() { return this.isRewritten; }
	
	public void setRewritten(boolean value) { this.isRewritten = value; }
	
	public boolean isResultOfRewrite() { return this.isResultOfRewrite; }
	
	public void setResultOfRewrite(boolean value) { this.isResultOfRewrite = value; }
	
	public Rule getOriginalRule() { return this.originalRule; }
	
	public void setOriginalRule(Rule originalRule) { this.originalRule = originalRule; }
	
	public void setAnnotations(String text) {
		if (text.toLowerCase().contains("monotonic"))
			this.allFunctionsMonotonic = true; 
		else if (text.toLowerCase().contains("final"))
			this.generateSealingAggregate = true;		
	}
	
	public boolean allFunctionsMonotonic() { return this.allFunctionsMonotonic; }
	
	public boolean generateSealingAggregate() { return this.generateSealingAggregate; }
	
	public MonotonicRuleType getMonotonicRuleType() { return this.monotonicRuleType; }
	
	public void setMonotonicRuleType(MonotonicRuleType monotonicRuleType) {
		this.monotonicRuleType = monotonicRuleType; 
	}
	
	public Rule copy() {
		CompilerVariableList variableList = new CompilerVariableList();
		Predicate newHead = this.head.copy(variableList);
		List<Predicate> newBody = new ArrayList<>();
		for (Predicate goal : this.body) 
			newBody.add(goal.copy());
				
		return new Rule(this.id, newHead, newBody);
	}

	public int getNumberOfLiterals() { return this.body.size(); }

	public Predicate getLiteral(int position) { return this.body.get(position); }

	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(this.id);
		output.append(") ");
		output.append(this.head.toString());

		if (this.body.size() > 0) {
			output.append(" <- ");
			String spacer = "";
			for (int i = 0; i < output.length(); i++)
				spacer += " ";

			for (int i = 0; i < this.body.size(); i++) {
				if (i > 0) {
					output.append(", ");
					if (i % 3 == 0) {
						output.append("\n");
						output.append(spacer);
					}
				}
				
				output.append(this.body.get(i).toString());
			}
	    }
		output.append(".");
		return output.toString();
	}

	public String toStringIndent(int level) {
		StringBuilder output = new StringBuilder();
		output.append(super.toStringIndent(level));
		output.append(this.id);
		output.append(") "); 
		output.append(this.head.toString());

		if (this.body.size() > 0) {
			output.append(" <- ");
			
			String spacer = "";
			for (int i = 0; i < output.length(); i++)
				spacer += " ";
			
			for (int i = 0; i < this.body.size(); i++) {
				if (i > 0) {
					output.append(", ");
					if (i % 3 == 0) {
						output.append("\n");
						output.append(spacer);
					}
				}
				
				output.append(this.body.get(i).toString());
			}
	    }

		output.append(".");
		return output.toString();
	}
	
	public String toStringCompilable() {
		StringBuilder output = new StringBuilder();
		output.append(this.head.toString());

		if (this.body.size() > 0) {
			output.append(" <- ");

			for (int i = 0; i < this.body.size(); i++) {
				if (i > 0)
					output.append(", ");

				output.append(this.body.get(i).toStringCompilable());
			}
	    }
		output.append(".");
		return output.toString();
	}

	public String toJson() {
		StringBuilder output = new StringBuilder();
		//output.append("{\"head\":");
		output.append("{\"head\":\"");
		output.append(this.head.toStringShort());
		//output.append(this.head.toJson());
		output.append("\", \"body\":[");
		if (this.body.size() > 0) {
			for (int i = 0; i < this.body.size(); i++) {
				if (i > 0)
					output.append(",");
				output.append("\"");
				//output.append(this.body.get(i).toJson());
				output.append(this.body.get(i).toStringShort());
				output.append("\"");
			}
		}
		output.append("]}");
		return output.toString();
	}
	
	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		return copy();
	}

	@Override
	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof Rule))
			return false;
		
		Rule otherRule = (Rule)other;
		
		if (!this.id.equals(otherRule.getRuleId()))
			return false;
		
		if (this.head == null && otherRule.getHead() != null)
			return false;
		
		if (!this.head.equals(otherRule.getHead()))
			return false;
		
		if (this.body == null && otherRule.getBody() != null)
			return false;
					
		if (this.body.size() != otherRule.getBody().size())
			return false;
		
		for (int i = 0; i < this.body.size(); i++)
			if (!this.body.get(i).equals(otherRule.getBody().get(i)))
				return false;		
				
		return true;
	}
}
