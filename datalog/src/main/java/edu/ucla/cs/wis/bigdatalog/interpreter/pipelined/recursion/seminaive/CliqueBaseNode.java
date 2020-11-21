package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive;

import java.util.LinkedList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.type.TypeInferrer;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.FixpointCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.RecursiveRelation;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.EvaluationType;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.RecursiveLiteral;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

abstract public class CliqueBaseNode 
	extends OrNode 
	implements IClique {
	
	protected final int					arity;
	protected boolean 					initialized;
	protected boolean 					isCleanedUp;	
	
	protected boolean 					isSharable;	
	protected int 							backtrackCount;
	
	protected String						recursiveRelationName;
	protected RecursiveRelation 			recursiveRelation;
	protected NodeList<IMutualClique>		mutualCliqueList;
	
	protected FixpointCursor 				fixpointCursor; // used to be NaiveCursor APS 11/20/2013
	protected CliqueRuleType 				currentRuleType;
	protected NodeList<RecursiveLiteral>	recursiveLiteralList;
	
	protected OrNode 						exitRulesOrNode;
	
	protected int 							stage;
	protected EvaluationType 				evaluationType;
	protected List<Integer>					numberOfDeltaFactsByIteration;
	protected List<Integer>					numberOfGeneratedFactsByIteration;
	public int								numberOfGeneratedFactsThisIteration;
	protected boolean						isLinearRecursive;
	protected Tuple							returnedTuple;
	
	public CliqueBaseNode(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables, 
			String recursiveRelationName, boolean isSharable) {
		super(predicateName, args, binding, freeVariables);
		
		this.initialized 				= false;
		this.isCleanedUp 				= true;
		
		this.currentRuleType			= CliqueRuleType.EXIT_RULE;
		
		this.isSharable			 		= isSharable;
		this.backtrackCount 			= 0;
		
		this.mutualCliqueList	 		= new NodeList<>();
		this.fixpointCursor		 		= null;
		
		this.recursiveRelationName		= recursiveRelationName;
		this.recursiveRelation			= null;
		this.recursiveLiteralList		= new NodeList<>();
		//APS 11/27/2013
		this.stage						= 0;
		this.arity 						= args.size();
		this.numberOfDeltaFactsByIteration  	= new LinkedList<>();
		this.numberOfGeneratedFactsByIteration 	= new LinkedList<>();
	}
	
	public EvaluationType getEvaluationType() { return this.evaluationType;}
	
	public void setEvaluationType(EvaluationType evaluationType) { this.evaluationType = evaluationType; }
	
	public void setIsLinearRecursive(boolean value) { this.isLinearRecursive = value; }
		
	public int getNumberOfIterations() { 
		return this.stage + 1; // stage is zero based
	}
	
	public List<Integer> getDeltaFactsByIteration() { return this.numberOfDeltaFactsByIteration;}
	
	public List<Integer> getGeneratedFactsByIteration() { return this.numberOfGeneratedFactsByIteration;}
	
	public int getStage() { return this.stage; }
	
	public void setStage(int stage) { this.stage = stage; }
	
	public boolean getIsSharable() { return this.isSharable; }
	
	public OrNode getExitRulesOrNode() { return this.exitRulesOrNode; }
		
	protected void cliqueCleanUp() {
		this.baseNodeCleanUp();
	}
	
	public void partialCleanUp() {		
		if (!this.isSharable)
			this.cleanUp();
	}
	
	public Status getTuple() {
		throw new InterpreterException("BaseCliqueNode.getTuple() should not be called.");
	}

	public RecursiveRelation getRecursiveRelation() { return this.recursiveRelation;	}

	public NodeList<IMutualClique> getMutualCliqueList() { return this.mutualCliqueList; }

	public NodeList<RecursiveLiteral> getRecursiveLiteralList() { return this.recursiveLiteralList; }
	
	public void setRecursiveLiteralList(NodeList<RecursiveLiteral> recursiveLiterals) {
		this.recursiveLiteralList = recursiveLiterals;
	}
	
	public DataType[] getSchema() {
		return TypeInferrer.getSchema(this.getArguments());
	}
	
	public void incrementBacktrackCount() {
		this.backtrackCount++;
	}

	public void decrementBacktrackCount() {
		if (this.backtrackCount > 0) {
			this.backtrackCount--;

			if (this.backtrackCount == 0)
				this.partialCleanUp();
		}
	}

	public void setMutualCliqueList(NodeList<IMutualClique> mutualCliques) {
		this.mutualCliqueList = mutualCliques;
	}

	public String getRecursiveRelationName() { return this.recursiveRelationName; }

	protected void initializeIndex() {	}

	public void addExitRulesOrNode(OrNode node) {
		this.exitRulesOrNode = node;
	}
	
	public void clearExitRulesOrNode() {
		this.exitRulesOrNode = null;				
	}
	
	/*************************************************************
	 * Here we generate a tuple from the exit rules. The binding pattern for the sub-query 
	 * to an exit rule is assumed to contain all free variables. We get such a tuple and store it
	 * locally in temporary relation. Then we do a post select on it using the matchTuple function. 
	 * If this succeeds, we return, else we continue with the current exit rule. When the current exit
	 * rule is exhausted, we try the next exit rule and so on ...
	 **************************************************************/
	protected Status getExitTuple() {
		if (DEBUG)
			this.logTrace("Entering getExitTuple for {}", this.toStringNode());
	
		Status status = Status.FAIL;
		
		// If we generated a tuple, we better add it in our local recursive relation. 
		// If it was a duplicate, we try again, else we are done
		while (((status = this.exitRulesOrNode.getTuple()) == Status.SUCCESS) 
				&& (!this.addTuple()));
		
		if (DEBUG)
			this.logTrace("Exiting getExitTuple for {} with status = {}", this.toStringNode(), status);
		
		return status;
	}
	
	@Override
	public void attachContext(DeALSContext deALSContext) {
		super.attachContext(deALSContext);
		this.exitRulesOrNode.attachContext(deALSContext);
	}
	
	abstract protected boolean addTuple();
	
	abstract public Status generateTuple();
	
	abstract public CliqueBaseNode copy(ProgramContext programContext);
}