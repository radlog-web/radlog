package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined;

import java.util.HashMap;
import java.util.Map;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.ArgumentList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InputVariable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterFunctor;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.BinaryExpression;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.UnaryExpression;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueBaseNode;

public class ProgramContext {

	private ArgumentList originalArgumentsList;
	private ArgumentList newArgumentsList;
	private Map<CliqueBaseNode, CliqueBaseNode> cliqueMapping;
	private Map<Node<?>, Node<?>> nodeMapping;
	//private Stack<ArgumentList> argumentListStack;
	
	public ProgramContext() {		
		this.originalArgumentsList = new ArgumentList();
		this.newArgumentsList = new ArgumentList();
		this.cliqueMapping = new HashMap<>();
		this.nodeMapping = new HashMap<>();
		//this.argumentListStack = new Stack<>();
	}

	public ArgumentList getArgumentList() { return this.newArgumentsList; }

	public Map<CliqueBaseNode, CliqueBaseNode> getCliqueMapping() { return cliqueMapping; }
	
	public Map<Node<?>, Node<?>> getNodeMapping() { return nodeMapping; }
	
	public NodeArguments copyArguments(NodeArguments original) {
		NodeArguments copy = new NodeArguments(original.size());
		for (int i = 0; i < original.size(); i++) {
			int index = this.getOriginalArgument(original.get(i));
			
			if (index >= 0)
				copy.set(i, this.newArgumentsList.get(index));
			else
				copy.set(i, this.copy(original.get(i)));			
		}		
					
		return copy;
	}
	
	private int getOriginalArgument(Argument original) {
		for (int i = 0; i < this.originalArgumentsList.size(); i++)
			if (this.originalArgumentsList.get(i) == original)
				return i;
		return -1;		
	}
	
	private Argument copy(Argument originalArgument) {
		Argument newArgument = null;
		if (originalArgument instanceof Variable)
			newArgument = this.copy((Variable) originalArgument);
		else if (originalArgument instanceof InterpreterFunctor)
			newArgument = this.copy((InterpreterFunctor) originalArgument);
		else if (originalArgument instanceof InterpreterList)
			newArgument = this.copy((InterpreterList)originalArgument);
		else if (originalArgument instanceof InputVariable)
			newArgument = this.copy((InputVariable) originalArgument);
		else if (originalArgument instanceof DbTypeBase)
			newArgument = ((DbTypeBase)originalArgument).copy();
		else if (originalArgument instanceof BinaryExpression)
			newArgument = ((BinaryExpression)originalArgument).copy();
		else if (originalArgument instanceof UnaryExpression)
			newArgument = ((UnaryExpression)originalArgument).copy();
		return newArgument;
	}
	
	private Argument copy(Variable original) {
		Variable copy = original.deepCopy();
		this.originalArgumentsList.add(original);
		this.newArgumentsList.add(copy);
		return copy;
	}
	
	private Argument copy(InterpreterFunctor original) {
		NodeArguments newArguments = this.copyArguments(original.getArguments());		

		InterpreterFunctor copy = new InterpreterFunctor(new String(original.getFunctorName()), newArguments);
		this.originalArgumentsList.add(original);
		this.newArgumentsList.add(copy);
		return copy;
	}
	
	private Argument copy(InterpreterList original) {
		if (original.isEmpty())
			return new InterpreterList(null);

		Argument newHead;
		int index = this.getOriginalArgument(original.getHead());
		
		if (index >= 0)
			newHead = this.newArgumentsList.get(index);
		else
			newHead = this.copy(original.getHead());
		
		InterpreterList newTail = null;
		if (original.getTail() != null)
			newTail = (InterpreterList)this.copy(original.getTail());
		
		InterpreterList copy = new InterpreterList(newHead, newTail);
		this.originalArgumentsList.add(original);
		this.newArgumentsList.add(copy);
		return copy;
	}
	
	private Argument copy(InputVariable original) {
		InputVariable copy;
		if (original.getValue() == null)
			copy = new InputVariable(original.getName(), original.getDataType());
		else
			copy = new InputVariable(original.getName(), original.getValue());
		this.originalArgumentsList.add(original);
		this.newArgumentsList.add(copy);
		return copy;
	}
	
	public VariableList copyVariableList(VariableList originalVariables) {
		VariableList copy = new VariableList();
		for (int i = 0; i < originalVariables.size(); i++) {
			for (int j = 0 ; j < this.originalArgumentsList.size(); j++)
				if (this.originalArgumentsList.get(j) == originalVariables.get(i))
					copy.add((Variable) this.newArgumentsList.get(j));
		}
		return copy;
	}
	
	public AggregateInfo[] copyAggregateInfos(AggregateInfo[] aggregateInfos) {
		AggregateInfo[] copy = new AggregateInfo[aggregateInfos.length];
		for (int i = 0; i < aggregateInfos.length; i++) {
			AggregateInfo newAggregateInfo = new AggregateInfo();
			newAggregateInfo.aggregateType = aggregateInfos[i].aggregateType;
			newAggregateInfo.dataType = aggregateInfos[i].dataType;
			
			int index = this.getOriginalArgument(aggregateInfos[i].sourceArgument);
			
			if (index >= 0)
				newAggregateInfo.sourceArgument = this.newArgumentsList.get(index);
			else
				newAggregateInfo.sourceArgument = this.copy(aggregateInfos[i].sourceArgument);
			copy[i] = newAggregateInfo;
		}
		
		return copy;
	}
	
	/*
	public void pushArgumentContext() {
		this.argumentListStack.push(this.argumentList);
		this.argumentList = new ArgumentList();
	}
	
	public void popArgumentContext() {
		if (!this.argumentListStack.isEmpty())
			this.argumentList = this.argumentListStack.pop();
	}*/
}
