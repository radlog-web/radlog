package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.xy;

import edu.ucla.cs.wis.bigdatalog.compiler.CompilerException;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYPredicateType;
import edu.ucla.cs.wis.bigdatalog.database.cursor.XYCursor;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InputVariable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.BinaryExpression;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.AndNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.NegationOrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;

/*************************************************************
 * COMMENTS: Stage variables are eliminated in XY recursive node in rpcg extraction. 
 * This node is used to retrieve the stage value from the xy clique at run time. 
 * If the variable is bound, the node returns SUCCESSFUL only when the the bound value is equal to that inside the xy clique. 
 * If the variable is not bound, current stage value of the clique is returned. 
 * AUTHOR:	HW
 **************************************************************/
public class XYStageVariableNode 
	extends OrNode {	

	protected XYRecursiveOrNode xyNode;
		
	public XYStageVariableNode(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables) {
		super(predicateName, args, binding, freeVariables);
	}
	
	@Override
	public boolean initialize() { return true; }

	public void deleteRelationsAndCursors() { }

	public void cleanUp() {
		this.baseNodeCleanUp();
	}

	public void partialCleanUp() {
		this.cleanUp();
	}
	
	public String toString() { return toStringNode()/* + "[" + this.hashCode() + "]"*/; }

	public boolean setUnboundStageValue(int relationStage) {
		DbTypeBase dbTypeObject = null;
		Argument argument = null;

		if (this.xyPredicateType == XYPredicateType.NEW && relationStage == 0)
			return false;

		if (this.getArgument(0) instanceof BinaryExpression) {
			dbTypeObject = DbInteger.create(relationStage - 1);
			argument = this.getArgument(0);
		} else if (this.getArgument(0) instanceof Variable) {
			dbTypeObject = DbInteger.create(relationStage);
			argument = this.getArgument(0);
		} else { 
			throw new CompilerException("setUnBoundStageValue() xy stage is neither INT nor FUNCTOR");
		}

		return argument.match(dbTypeObject);
		//return this.matchDbTypeToArgument(dbTypeObject, argument);
	}

	public int getBoundStageValue() {
		if (DEBUG)
			this.logTrace("Entering getBoundStageValue");
		
		Argument argument = this.getArgument(0);		

		if (argument instanceof Variable)
			argument = ((Variable)argument).dereference();
		else if (argument instanceof InputVariable)
			argument = ((InputVariable)argument).getValue();
		
		DbTypeBase dbTypeObject = null;
		
		if (argument instanceof BinaryExpression)
			dbTypeObject = ((BinaryExpression)argument).toDbType(this.typeManager);
		else if (argument instanceof DbInteger)
			dbTypeObject = (DbTypeBase) argument;
		else
			throw new CompilerException("getBoundStageValue() xy stage is neither INT nor FUNCTOR");
		
		int stageValue = (this.xyPredicateType == XYPredicateType.NEW) ? (((DbInteger)dbTypeObject).getValue() + 1) 
				: ((DbInteger)dbTypeObject).getValue();
		
		if (DEBUG)
			this.logTrace("Exiting getBoundStageValue with stageValue = {}", stageValue);
		
		return stageValue;
	}

	public int xyStage() {
		int stage;

		this.freeVariableList.makeFree();
		
		if (this.getBinding(0) == BindingType.BOUND)
			stage = this.getBoundStageValue();
		else
			stage = -1;

		return stage;
	}

	@Override
	public Status getTuple() {
		Status status;

		this.traceGetTupleEntry();

		this.freeVariableList.makeFree();

		if (this.isEntry) {
			int	stage = ((XYCursor)this.xyNode.getCursor()).XYStage;

			if (this.getBinding(0) == BindingType.BOUND) {
				if (this.getBoundStageValue() == stage) {
					this.isEntry = false;
					status = Status.SUCCESS;
				} else {
					status = Status.FAIL;
					this.cleanUp();
				}
			} else{
				if (this.setUnboundStageValue(stage)) {
					status = Status.SUCCESS;
					this.isEntry = false;
				} else {
					status = Status.FAIL;
					this.cleanUp();
				}
			}
		} else {
			status = Status.FAIL;
			this.cleanUp();
		}

		this.traceGetTupleExit(status);

		return status;
	}

	public void setXYNode(AndNode andNode) {
		// After we eliminated stage variable and created auxiliary stage
		// node, the stage node and the xy_node should be the only two
		// children of their parent node. However, after rule compression, the
		// pcg tree is flattened so that there may be more than 2 nodes under
		// their parent node. However, stage node and xy_node should still be
		// on the same level and be close to each other.
		OrNode orNode = null;
		boolean found = false;
		
		for (int i = (andNode.getNumberOfChildren() - 1); i >= 0; i--) {
			orNode = andNode.getChild(i);

			//if (orNode.getType() == NodeType.NEGATION_OR_NODE)
			if (orNode instanceof NegationOrNode)
				orNode = ((NegationOrNode)orNode).getLiteralToBeNegated();

			//if (orNode.getType() == NodeType.XY_RECURSIVE_OR_NODE) {
			if (orNode instanceof XYRecursiveOrNode) {
				found = true;
				break;
			}
		}
		
		if (!found)
			throw new CompilerException("Can not find XY Recursive Or Node for " + this.toString());

		this.xyNode = (XYRecursiveOrNode)orNode;
		this.xyNode.setXYStageNode(this);
	}
	
	@Override
	public XYStageVariableNode copy(ProgramContext programContext) {
		if (programContext.getNodeMapping().containsKey(this))
			return (XYStageVariableNode) programContext.getNodeMapping().get(this);
		
		XYStageVariableNode copy = new XYStageVariableNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		copy.xyPredicateType = this.xyPredicateType;
		
		if (programContext.getNodeMapping().containsKey(this.xyNode))
			copy.xyNode = (XYRecursiveOrNode) programContext.getNodeMapping().get(this.xyNode);
		else
			copy.xyNode = this.xyNode.copy(programContext);
				
		programContext.getNodeMapping().put(this, copy);
		
		return copy;
	}
}
