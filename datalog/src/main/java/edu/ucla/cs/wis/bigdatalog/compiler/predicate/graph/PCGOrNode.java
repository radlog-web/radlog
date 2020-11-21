package edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.compiler.recursion.Clique;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliqueBase;
import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.RewritingMethodType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYClique;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYPredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.PredicateBase;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.PredicateType;

public class PCGOrNode extends PCGNode<PCGOrNodeChild> implements PCGNodeChild {
	private static final long serialVersionUID = 1L;

	private static Logger logger = LoggerFactory.getLogger(PCGOrNode.class.getName());
		
	private static final int CLIQUE_INDEX = 0;
	private static final int BASE_PREDICATE_INDEX = 0;

	protected RewritingMethodType 	rewritingMethod;
	protected List<PredicateBase> 	usedBasePredicates;
	protected boolean				extended;
	
	public PCGOrNode(String predicateName, CompilerTypeList arguments) {
		this(predicateName, arguments, CompilerType.PCG_OR_NODE, BuiltInPredicateType.UNKNOWN);
	}

	protected PCGOrNode(String predicateName, CompilerTypeList arguments, 
			CompilerType compilerType, BuiltInPredicateType builtInPredicateType) {
		super(predicateName, arguments, compilerType, builtInPredicateType);
		this.rewritingMethod = RewritingMethodType.UNKNOWN;
		this.usedBasePredicates	= new ArrayList<>();
		this.extended = false;
	}
	
	public PCGOrNode(String predicateName, CompilerTypeList arguments, BuiltInPredicateType builtInPredicateType) {
		this(predicateName, arguments, CompilerType.PCG_OR_NODE, builtInPredicateType);
	}
		
	public boolean isRecursive() { return this.predicate.isRecursive(); }

	public void setAsRecursive() { this.predicate.setAsRecursive(); }
	
	public void setAsNonRecursive() { this.predicate.setAsNonRecursive(); }
	
	public void setBaseClique(CliqueBase baseClique) {
		this.insertChild(CLIQUE_INDEX, baseClique);
	}

	public void resetBaseClique(CliqueBase baseClique) {
		this.setChild(CLIQUE_INDEX, baseClique);
	}

	public CliqueBase getBaseClique() { return (CliqueBase)this.getChild(CLIQUE_INDEX); }

	public void setBasePredicate(BasePredicate basePredicate) {
		this.insertChild(BASE_PREDICATE_INDEX, basePredicate);
	}

	public BasePredicate getBasePredicate() {
		if (this.predicate.isBase())
			return (BasePredicate)this.getChild(BASE_PREDICATE_INDEX);
		
		return null;
	}

	public boolean isRecursiveLiteral(CliqueBase baseClique) {
		return (this.predicate.isRecursive() && this.getBaseClique() == baseClique);
	}

	public boolean isSharableClique() {
		return (this.rewritingMethod == RewritingMethodType.NAIVE 
				|| this.rewritingMethod == RewritingMethodType.SEMI_NAIVE 
				|| this.rewritingMethod == RewritingMethodType.UNKNOWN);
	}

	public void setRewritingMethod(RewritingMethodType rewritingMethod) {
		this.rewritingMethod = rewritingMethod;
	}

	public RewritingMethodType getRewritingMethod() { return this.rewritingMethod; }

	public List<PredicateBase> getUsedBasePredicates() { return this.usedBasePredicates; }
	
	public int getNumberOfUsedBasePredicates() { return this.usedBasePredicates.size(); }
	
	public PredicateBase getUsedBasePredicate(int position) { return this.usedBasePredicates.get(position); }

	public void addUsedBasePredicate(PredicateBase basePredicate) {
		if (!this.isUsedBasePredicate(basePredicate)) {
			PredicateBase newPredicate = new PredicateBase(basePredicate.getPredicateName(), basePredicate.getArity());
			this.usedBasePredicates.add(newPredicate);
		}
	}
	
	public PCGOrNode copyNode() {
		PCGOrNode orNode = new PCGOrNode(this.getPredicateName(), this.getArguments().copy());

		orNode.setPredicateOperatorType(this.getPredicateOperatorType());
		orNode.setPredicateType(this.getPredicateType());
		orNode.setBuiltInPredicateType(this.getBuiltInPredicateType());
		if (this.isRecursive())
			orNode.setAsRecursive();
		
		orNode.setBindingPattern(this.binding);
		orNode.setExecutionBindingPattern(this.executionBinding);
		
		orNode.setRewritingMethod(this.rewritingMethod);
		orNode.copyUsedBaseAndUpdatedPredicates(this);
		orNode.setNoBacktrack(this.noBacktrack);

		orNode.predicate.setArgumentTypeAdornment(this.predicate.getArgumentTypeAdornment());
		orNode.predicate.setFSAggregateType(this.predicate.getFSAggregateType());
		orNode.originalArguments = this.originalArguments;
		return orNode;
	}

	public PCGOrNode copyNode(CompilerVariableList variableList) {
		PCGOrNode orNode = new PCGOrNode(this.getPredicateName(), this.getArguments().copy(variableList));

		orNode.setPredicateOperatorType(this.getPredicateOperatorType());
		orNode.setPredicateType(this.getPredicateType());
		orNode.setBuiltInPredicateType(this.getBuiltInPredicateType());
		if (this.isRecursive())
			orNode.setAsRecursive();		

		orNode.setBindingPattern(this.binding);
		orNode.setExecutionBindingPattern(this.executionBinding);
		
		orNode.setRewritingMethod(this.rewritingMethod);
		orNode.copyUsedBaseAndUpdatedPredicates(this);
		orNode.setNoBacktrack(this.noBacktrack);
		
		orNode.predicate.setArgumentTypeAdornment(this.predicate.getArgumentTypeAdornment());
		orNode.predicate.setFSAggregateType(this.predicate.getFSAggregateType());
		orNode.originalArguments = this.originalArguments;
		return orNode;
	}

	public void delete() {
		if (this.predicate.isRecursive()) {
			CliqueBase baseClique = this.getBaseClique();

			if (baseClique != null) {
				baseClique.removeParent(this);
			}
		} else if (this.predicate.isDerived() 
				|| (this.predicate.isBuiltIn() 
						&& (((BuiltInPredicate)this.predicate).isIfThenElse()
								|| ((BuiltInPredicate)this.predicate).isIfThen()
								|| ((BuiltInPredicate)this.predicate).isAggregate()
								|| ((BuiltInPredicate)this.predicate).isFSAggregate()))) {
			for (int i = this.getNumberOfChildren() - 1; i >= 0; i--)
				((PCGAndNode)this.removeChild(i)).delete();			
		}
	}

	public String toStringTree() {
		return toStringTree(0);
	}

	public String toStringTree(int level) {
		StringBuilder retval = new StringBuilder();
		retval.append(this.toStringIndent(level));
		retval.append(this.toString());

		if (this.isBuiltInPredicate()) {
			BuiltInPredicate bip = (BuiltInPredicate)this.predicate;
			if (bip.isIfThenElse() 
					|| bip.isIfThen() 
					|| bip.isAggregate() 
					|| bip.isFSAggregate()
					|| bip.isReadAggregate() 
					|| bip.isReadAggregateFS()
					|| bip.isSort()
					|| bip.isTopK()) {
				for (PCGOrNodeChild node : this.children)
					retval.append(((PCGAndNode)node).toStringTree(level + 1));
			}
			if (this.predicate.isRecursive())
				retval.append(this.getBaseClique().toStringTree(level + 1));
		} else if (this.predicate.isRecursive()) {
			retval.append(this.getBaseClique().toStringTree(level + 1));			
		} else if (this.predicate.isBase()) {
			retval.append(this.toStringIndent(level + 1));
			retval.append(this.getBasePredicate().toString());
		} else {
			for (int i = 0; i < this.getNumberOfChildren(); i++)
				retval.append(((PCGAndNode)this.getChild(i)).toStringTree(level + 1));
		}
		return retval.toString();
	}

	public void resetIsDescribed() { 
		if (this.isBuiltInPredicate()) {
			BuiltInPredicate bip = (BuiltInPredicate)this.predicate;
			if (bip.isIfThenElse() || bip.isIfThen() 
					|| bip.isAggregate() || bip.isFSAggregate() || bip.isAggregatePredicate()) {
				for (PCGOrNodeChild node : this.children) {
					if (node instanceof PCGAndNode)
						((PCGAndNode)node).resetIsDescribed();
					else if (node instanceof Clique)
						((Clique)node).resetIsDescribed();
				}
			}
		} else if (this.predicate.isRecursive()) {
			this.getBaseClique().resetIsDescribed();
		} else if (this.predicate.isDerived()) {
			for (PCGOrNodeChild node : this.children)
				((PCGAndNode)node).resetIsDescribed();
		}
	}

	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("PCGOrNode(");
		retval.append(super.toStringAsPredicate());
		retval.append(", #Children: ");
		retval.append(this.children.size());
		retval.append(", predicateType: ");
		retval.append(this.getPredicateType().name());
		
		if (this.getPredicateType() != PredicateType.BASE) {
			retval.append(", isRecursive: ");
			if (this.isRecursive())
				retval.append("Yes");
			else
				retval.append("No");
		}
		
		if (this.rewritingMethod != RewritingMethodType.UNKNOWN) {
			retval.append(", rewritingMethod: ");
			retval.append(this.rewritingMethod.getName());
		}
		
		if (this.predicate.getXYPredicateType() != XYPredicateType.NONE) {
			retval.append(", XYPredicateType:");
			if (this.predicate.getXYPredicateType() == XYPredicateType.NEW)
				retval.append("NEW");
			else if (this.predicate.getXYPredicateType() == XYPredicateType.OLD)
				retval.append("OLD");
		}

		retval.append(")");
		return retval.toString();
	}

	public String toStringUsedBasePredicates() {
		StringBuilder retval = new StringBuilder();
		if (this.getNumberOfUsedBasePredicates() <= 0) {
			retval.append("[UsedBasePredicates = none]");
		} else {
			retval.append("[UsedBasePredicates = ");

			for (int i = 0; i < this.getNumberOfUsedBasePredicates(); i++) {
				if (i > 0)
					retval.append(", ");
				retval.append(this.getUsedBasePredicate(i).toString());
			}

			retval.append("]");
		}
		return retval.toString();
	}

	public PCGOrNode copy() {
		logger.error("PCGOrNode can not be copied without a variable list.");
		throw new RuntimeException("PCGOrNode can not be copied without a variable list.");
	}

	public PCGOrNode copy(CompilerVariableList variableList) {
		PCGOrNode orNode = this.copyNode(variableList);
		
		// we share children of those leaf node because they point to special structures
		if (orNode.predicate.isBase()) {
			orNode.setBasePredicate(this.getBasePredicate());
		} else if (orNode.isRecursive()) {
			// make sure that the clique has a new parent
			orNode.setBaseClique(this.getBaseClique());
			orNode.getBaseClique().addParent(orNode);
		} else if (orNode.predicate.isDerived() || 
				(orNode.isBuiltInPredicate() 
						&& (((BuiltInPredicate)orNode.predicate).isAggregate() || ((BuiltInPredicate)orNode.predicate).isFSAggregate()))) {
			// reference these types of predicates
			for (PCGOrNodeChild node : this.children)
				orNode.addChild(node);

		} else if (this.isBuiltInPredicate()) {
			BuiltInPredicate bip = (BuiltInPredicate)this.predicate;
			if ((bip.isIfThenElse() || bip.isIfThen())) {
				// copy these types of predicates
				for (PCGOrNodeChild node : this.children)
					orNode.addChild((PCGOrNodeChild)node.copy(variableList));
			}
		}

		return orNode;
	}

	public PCGOrNode copyTree(CompilerVariableList variableList) {
		PCGOrNode orNode = this.copyNode(variableList);

		// we share children of those leaf node because they point to special structures
		if (orNode.predicate.isBase()) {
			orNode.setBasePredicate(this.getBasePredicate());
		} else if (orNode.isRecursive()) {
			// make sure that the clique has a new parent
			orNode.setBaseClique(this.getBaseClique());

			if (this.getBaseClique() != null)
				orNode.getBaseClique().addParent(orNode);
		} else if (orNode.predicate.isDerived()
				|| (orNode.isBuiltInPredicate() && (
						((BuiltInPredicate)orNode.predicate).isAggregate()
						|| ((BuiltInPredicate)orNode.predicate).isFSAggregate()
						|| ((BuiltInPredicate)orNode.predicate).isReadAggregate()
						|| ((BuiltInPredicate)orNode.predicate).isReadAggregateFS() /* APS 4/11/2013*/))) {
			for (PCGOrNodeChild node : this.children)
				orNode.addChild(((PCGAndNode)node).copyTree(variableList));

		} else if (this.isBuiltInPredicate()) {
			BuiltInPredicate bip = (BuiltInPredicate)this.predicate;		
			if (bip.isIfThenElse() || bip.isIfThen()) {
				// we handle these built-in predicate differently				
				for (PCGOrNodeChild node : this.children)
					orNode.addChild(((PCGAndNode)node).copyTree(variableList));
			}
		}

		return orNode;
	}

	public void copyUsedBaseAndUpdatedPredicates(PCGOrNode orNode) {
		for (int i = 0; i < orNode.getNumberOfUsedBasePredicates(); i++)
			this.addUsedBasePredicate(orNode.getUsedBasePredicate(i));
	}

	public void clearBinding() {
		this.binding.setAsNoBinding();

		if (this.predicate.isDerived() && !this.isRecursive()) {
			for (PCGOrNodeChild node : this.children)
				((PCGAndNode)node).clearBinding();
		}
	}

	public boolean isUsedBasePredicate(PredicateBase predicate) {
		if (predicate == null)
			return false;
		
		for (PredicateBase basePredicate : this.usedBasePredicates) {
			if (basePredicate.getPredicateName().equals(predicate.getPredicateName()) 
					&& basePredicate.getArity() == predicate.getArity())
				return true;
		}
		
		return false;
	}

	public void collectUsedBasePredicates(PCGAndNode andNode) {
		for (PCGOrNode orNode : andNode.getChildren()) {
			for (PredicateBase basePredicate : orNode.getUsedBasePredicates())
				this.addUsedBasePredicate(basePredicate);
		}
	}

	public boolean hasChoiceLiteral() {
		boolean hasChoiceLiteral = false;
		PCGAndNode ifAndNode, thenAndNode, elseAndNode;

		if (!(this.predicate instanceof BuiltInPredicate))
			return false;
		
		BuiltInPredicate bip = (BuiltInPredicate)this.predicate;
		
		if (bip.isChoice()) {
			hasChoiceLiteral = true;
		} else if (bip.isIfThen()) {
			ifAndNode = (PCGAndNode) this.getChild(0); 
			thenAndNode = (PCGAndNode) this.getChild(1); 

			if (ifAndNode.hasChoiceLiteral() || thenAndNode.hasChoiceLiteral())
				hasChoiceLiteral = true;
		} else if (bip.isIfThenElse()) {
			ifAndNode = (PCGAndNode) this.getChild(0); 
			thenAndNode = (PCGAndNode) this.getChild(1); 
			elseAndNode = (PCGAndNode) this.getChild(2); 

			if (ifAndNode.hasChoiceLiteral() || thenAndNode.hasChoiceLiteral() 
					|| elseAndNode.hasChoiceLiteral())
				hasChoiceLiteral = true;
		}
		return hasChoiceLiteral;
	}

	public void unsetChildrenVariables() {
		boolean hasChildrenNeedingUnsetting = false;
		
		if (this.predicate instanceof BuiltInPredicate) {
			BuiltInPredicate bip = (BuiltInPredicate)this.predicate;
			if (bip.isIfThenElse() || bip.isIfThen()  
					|| bip.isAggregate() || bip.isFSAggregate())
				hasChildrenNeedingUnsetting = true;
			else
				hasChildrenNeedingUnsetting = false;
		} else if (this.predicate.isBase() || this.predicate.isRecursive()) { 
			hasChildrenNeedingUnsetting = false;
		} else {
			hasChildrenNeedingUnsetting = true;
		}

		if (hasChildrenNeedingUnsetting)
			this.unsetChildrenVariablesAux();
	}

	@Override	
	public boolean equals(CompilerTypeBase other) {
		if (!(other instanceof PCGOrNode))
			return false;
		
		PCGOrNode otherNode = (PCGOrNode)other;		
		return this.predicate.equals(otherNode.predicate);
	} 
		
	public boolean isFalseNode() { return this.isBuiltInPredicateType(BuiltInPredicateType.FALSE); }
	
	public boolean isIfThenElseNode() { return this.isBuiltInPredicateType(BuiltInPredicateType.IFTHENELSE); }

	public boolean isIfThenNode() { return this.isBuiltInPredicateType(BuiltInPredicateType.IFTHEN); }
		
	public boolean isAggregateNode() { return this.isBuiltInPredicateType(BuiltInPredicateType.AGGREGATE); }
	
	public boolean isFSAggregateNode() { return this.isBuiltInPredicateType(BuiltInPredicateType.AGGREGATE_FS); }
	
	public boolean isBuiltInPredicateType(BuiltInPredicateType builtInPredicateType) {
		if (this.predicate instanceof BuiltInPredicate) {
			BuiltInPredicate bip = (BuiltInPredicate)this.predicate;
			return bip.getBuiltInPredicateType() == builtInPredicateType;
		}
		return false;
	}
	
	public boolean isAnyOf(List<BuiltInPredicateType> builtInPredicateTypes) {
		if (!(this.predicate instanceof BuiltInPredicate))
			return false;
		
		BuiltInPredicate bip = (BuiltInPredicate)this.predicate;
		for (BuiltInPredicateType builtInPredicateType : builtInPredicateTypes) {
			if (builtInPredicateType == bip.getBuiltInPredicateType())
				return true;
		}
		return false;
	}
	
	public boolean containsXYClique() {
		for (PCGOrNodeChild orNodeChild : this.children) {
			if (orNodeChild.getClass() == XYClique.class)
				return true;
			
			if (orNodeChild.getClass() == PCGAndNode.class)
				if (((PCGAndNode)orNodeChild).containsXYClique())
					return true;
		}
		return false;
	}
	
	public void setExtended(boolean extended) { this.extended = extended; }
	
	public boolean getExtended() { return this.extended; }
}
