package edu.ucla.cs.wis.bigdatalog.compiler.xy;

import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGAndNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliquePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;

public class XYCliquePredicate extends CliquePredicate {
	private static final long serialVersionUID = 1L;
	protected List<PCGAndNode> xRules;
    protected List<PCGAndNode> yRules;
    protected List<PCGAndNode> copyRules;
    protected List<PCGAndNode> deleteRules;
    
    public XYCliquePredicate(String predicateName, int arity) {
    	super(predicateName, arity - 1, CompilerType.XY_CLIQUE_PREDICATE);
    	this.isRewritten	= false;
    	this.xRules 		= new ArrayList<>();
    	this.yRules 		= new ArrayList<>();
    	this.copyRules		= new ArrayList<>();
    	this.deleteRules 	= new ArrayList<>();
    }  

    public int getNumberOfXRules() { return this.xRules.size(); }

    public int getNumberOfYRules() { return this.yRules.size(); }
    
    public int getNumberOfCopyRules() { return this.copyRules.size(); }

    public int getNumberOfDeleteRules() { return this.deleteRules.size(); }

    public void addXRule(PCGAndNode rule) {
    	this.xRules.add(rule);
    }

    public void addYRule(PCGAndNode rule) {
    	this.yRules.add(rule);
    }

    public void addCopyRule(PCGAndNode rule) {
    	this.copyRules.add(rule);
    }

    public void addDeleteRule(PCGAndNode rule) {
        this.deleteRules.add(rule);
    }
    
    public List<PCGAndNode> getXRules() { return this.xRules; }
    
    public PCGAndNode getXRule(int position) { return this.xRules.get(position); }    
    
    public List<PCGAndNode> getYRules() { return this.yRules; }

    public PCGAndNode getYRule(int position) { return this.yRules.get(position); }
    
    public List<PCGAndNode> getCopyRules() { return this.copyRules; }

    public PCGAndNode getCopyRule(int position) { return this.copyRules.get(position); }
    
    public List<PCGAndNode> getDeleteRules() { return this.deleteRules; }

    public PCGAndNode getDeleteRule(int position) { return this.deleteRules.get(position); }
    
    public PCGAndNode removeXRule(int position) { return this.xRules.remove(position); }

    public PCGAndNode removeYRule(int position) { return this.yRules.remove(position); }

    public PCGAndNode removeCopyRule(int position) { return this.copyRules.remove(position); }

    public PCGAndNode removeDeleteRule(int position) { return this.deleteRules.remove(position); }

    public String toStringIndentXYRules(String title, List<PCGAndNode> rules, XYClique xyClique, int level) {
    	StringBuilder output = new StringBuilder();

    	if (rules != null && rules.size() > 0) {
        	output.append(this.toStringIndent(level + 1));
        	output.append(title);
	    	for (PCGAndNode andNode : rules) {
	    		output.append(this.toStringIndent(level + 2));
	    		output.append(andNode.toString());
	
	    		for (PCGOrNode orNode : andNode.getChildren()) {
	    			if (orNode.isRecursive()) {
	    				output.append(this.toStringIndent(level + 3));
	    				output.append(orNode.toString());
	    	      
	    				if (!orNode.isRecursiveLiteral(xyClique)) {
	    					output.append(orNode.getBaseClique().toStringTree(level + 4));
	    				}
	    			} else {
	    				output.append(orNode.toStringTree(level + 3));
	    	  		}
	    		}
	    	}
    	}
    	return output.toString();
    }

    public String toStringTree(XYClique xyClique, int level) {
    	StringBuilder output = new StringBuilder();
    	output.append(this.toStringIndent(level));
    	//output.append("XYCliquePred( predicate: ");
    	output.append("XYCliquePred(");
    	output.append(this.predicateName.toString());

    	if (!this.binding.hasNoBinding()) {  // no adornment yet
    		output.append("_");
    		output.append(this.binding.toString());
        }

    	output.append(", arity: ");
    	output.append(this.arity);
    	output.append(", binding: ");
    	output.append(this.binding.toString());
    	output.append(", #ExitRules: ");
    	output.append(this.getNumberOfExitRules());
    	output.append(", #XRules: ");
    	output.append(this.getNumberOfXRules());
    	output.append(", #YRules: ");
    	output.append(this.getNumberOfYRules());
    	output.append(", #CopyRules: ");
    	output.append(this.getNumberOfCopyRules());
    	output.append(", #DeleteRules: ");
    	output.append(this.getNumberOfDeleteRules());
    	output.append(")");
    	
    	output.append(this.toStringIndentXYRules("Exit Rules:", this.exitRules, xyClique, level));
    	output.append(this.toStringIndentXYRules("X Rules:", this.xRules, xyClique, level));
    	output.append(this.toStringIndentXYRules("Y Rules:", this.yRules, xyClique, level));
    	output.append(this.toStringIndentXYRules("Copy Rules:", this.copyRules, xyClique, level));
    	output.append(this.toStringIndentXYRules("Delete Rules:", this.deleteRules, xyClique, level));    	
    	return output.toString();
    }

    public String toString() {
    	StringBuilder output = new StringBuilder();
    	//output.append("\n XYCliquePred( predicate: " + this.predicateName);
    	output.append("XYCliquePred(" + this.predicateName);

      	if (!this.binding.hasNoBinding()) {  // no adornment yet
      		output.append("_");
      		output.append(this.binding.toString());
        }

      	output.append(", arity: ");
      	output.append(this.arity);
      	output.append(", binding: ");
      	output.append(this.binding.toString());
      	output.append(", #ExitRules: ");
      	output.append(this.getNumberOfExitRules());
      	output.append(", #XRules: ");
      	output.append(this.getNumberOfXRules());
      	output.append(", #YRules: ");
      	output.append(this.getNumberOfYRules());
      	output.append(", #CopyRules: ");
      	output.append(this.getNumberOfCopyRules());
      	output.append(", #DeleteRules: ");
      	output.append(this.getNumberOfDeleteRules());
      	output.append(")");
      	
      	if (this.exitRules.size() > 0) {
      		output.append("\n  Exit Rules:"); 
      		for (PCGAndNode andNode : this.exitRules)
      			output.append(andNode.toStringAsRule());
      	}
      	
      	if (this.xRules.size() > 0) {
      		output.append("\n  X Rules:");      	     	      	
      		for (PCGAndNode andNode : this.xRules)
      			output.append(andNode.toStringAsRule());
      	}
      	
      	if (this.yRules.size() > 0) {
      		output.append("\n  Y Rules:");
      	   	for (PCGAndNode andNode : this.yRules)
      	   		output.append(andNode.toStringAsRule());
      	}

      	if (this.copyRules.size() > 0) {
      		output.append("\n  Copy Rules:");
      	  	for (PCGAndNode andNode : this.copyRules)
      	  		output.append(andNode.toStringAsRule());
      	}

      	if (this.deleteRules.size() > 0) {
      		output.append("\n  Delete Rules:");      	
      		for (PCGAndNode andNode : this.deleteRules)
      			output.append(andNode.toStringAsRule());
      	}

      	output.append("\n )");
      	return output.toString();
    }

    public void resetIsDescribed() {
    	for (PCGAndNode andNode : this.exitRules)
      		andNode.resetIsDescribed();
      	
      	for (PCGAndNode andNode : this.xRules)
      		andNode.resetIsDescribed();

      	for (PCGAndNode andNode : this.yRules)
      		andNode.resetIsDescribed();

      	for (PCGAndNode andNode : this.copyRules)
      		andNode.resetIsDescribed();

      	for (PCGAndNode andNode : this.deleteRules)
      		andNode.resetIsDescribed();
    }

    public XYCliquePredicate copy() {
    	XYCliquePredicate xyCliquePredicate = new XYCliquePredicate(this.predicateName, this.arity);
    	xyCliquePredicate.setBindingPattern(this.binding);
    	
    	for (PCGAndNode andNode : this.exitRules)
    		xyCliquePredicate.addExitRule(andNode.copyTree());
    	
      	for (PCGAndNode andNode : this.xRules)
      		xyCliquePredicate.addXRule(andNode.copyTree());

      	for (PCGAndNode andNode : this.yRules)
      		xyCliquePredicate.addYRule(andNode.copyTree());

      	for (PCGAndNode andNode : this.copyRules)
      		xyCliquePredicate.addCopyRule(andNode.copyTree());

      	for (PCGAndNode andNode : this.deleteRules)
      		xyCliquePredicate.addDeleteRule(andNode.copyTree());

    	return xyCliquePredicate;
    }

    public void clearBinding() {
    	this.binding.setAsNoBinding();

    	for (PCGAndNode andNode : this.exitRules)
    		andNode.clearBinding();
    	
    	for (PCGAndNode andNode : this.xRules)
    		andNode.clearBinding();

      	for (PCGAndNode andNode : this.yRules)
      		andNode.clearBinding();

      	for (PCGAndNode andNode : this.copyRules)
      		andNode.clearBinding();

      	for (PCGAndNode andNode : this.deleteRules)
      		andNode.clearBinding();
    }
}
