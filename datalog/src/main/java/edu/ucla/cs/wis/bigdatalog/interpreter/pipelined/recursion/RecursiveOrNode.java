package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion;

import java.util.LinkedList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.AggregateRewriter;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.interpreter.EvaluationType;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ExecutionMode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.QueryFormNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.fs.FSMinMaxAggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.fs.FSCliqueNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueBaseNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.RelationNode;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class RecursiveOrNode 
	extends RelationNode 
	implements QueryFormNode {
	protected String 			recursiveRelationName;
	protected CliqueBaseNode 	parentClique;
	protected CliqueBaseNode 	clique;
	protected EvaluationType 	evaluationType;
	protected Cursor<?> 		queryFormCursor;
	protected Relation<?> 		queryFormRelation;
	protected List<RecursiveOrNode> otherInstances;
	protected Database			database;
	
	public RecursiveOrNode(String predicateName, NodeArguments args, Binding bindings, VariableList freeVariables) {
		super(predicateName, args, bindings, freeVariables);	
	
		this.recursiveRelationName = null;
		this.clique = null;
		this.parentClique = null;
	}
	
	public EvaluationType getEvaluationType() { return this.evaluationType; } 
	
	public void setEvaluationType(EvaluationType evaluationType) { this.evaluationType = evaluationType; }
		
	@Override
	public boolean initialize() {
		if (!super.initialize())
			return false;
		
		boolean initialized = true;

		if (this.clique != null)
			initialized = this.clique.initialize();
	  
		if (initialized) {
			//if (this.evaluationType == EvaluationType.EagerMonotonic
			//		|| this.evaluationType == EvaluationType.SSC) {
			if (this.clique instanceof FSCliqueNode) {
				String relationName = AggregateRewriter.FS_AGGREGATE_NODE_NAME_PREFIX + this.getPredicateName().substring(0, this.getPredicateName().lastIndexOf("_"));

				// first try to find larger table, then smaller
				this.relation = this.relationManager.getRelation(relationName, this.arity);
				if (this.relation == null)
					this.relation = this.relationManager.getRelation(relationName, this.arity + 1);
			} else {
				//String relationName = this.getPredicateName().substring(0, this.getPredicateName().lastIndexOf("_"));
				this.relation = this.relationManager.createRecursiveRelation(this.recursiveRelationName, this.getSchema(), true);
			}
			
			this.cursor = this.database.getCursorManager().createCursor(this.relation, this.boundColumns);
			
			this.determineMatchFreeColumns();
	    }

		return initialized;		
	}

	public void deleteRelationsAndCursors() {
		super.deleteRelationsAndCursors();

		if (this.clique != null)
			this.clique.deleteRelationsAndCursors();
		
		this.isMaterialized = false;
	}

	public void cleanUp() {
		super.cleanUp();

	  	if (this.clique != null)
	  		this.clique.cleanUp();
	  	
	  	this.isMaterialized = false;
	}

	public void partialCleanUp() {
		super.cleanUp();

		if (this.clique != null)
			this.clique.partialCleanUp();
	}
	
	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(this.toStringNode());
		displayIndentLevel++;

		if (this.clique != null)
			output.append(this.clique.toString());

		displayIndentLevel--;
	
		return output.toString();
	}

	@Override
	public Status getTuple() {
		if (this.executionMode == ExecutionMode.Materialized) {
			if (this.isEntry && !this.isMaterialized) {
				while (this.doGetTuple() == Status.SUCCESS) {};
				this.isMaterialized = this.determineMaterialized();
				if (this.isMaterialized)
					this.setMaterializedAllInstances();
			}

			return super.getTuple();
		}
		return this.doGetTuple();
	}
	
	private Status doGetTuple() {
		Status status = Status.FAIL;

		this.traceGetTupleEntry();

		if (this.isEntry) {
			this.clique.incrementBacktrackCount();
			this.resetCursor();
		}

		if (!this.isEntry && this.hasAllArgumentsBound) {
			// We should not enforce clean-up unless the clique is unsharable - KayLiang Ong
			this.partialCleanUp();
		} else {
			int getStatus = 0;
			int i = 0;
			
			if (this.hasMatchColumns) {
				for (int j = 0; j < this.freeVariableList.variables.length; j++)
					this.freeVariableList.variables[j].makeFree();
			}				
			
			while ((getStatus = this.parentClique.getAnyTuple(this.cursor, this.clique, this.capturedTuple)) > 0) {
				for (i = 0; i < this.arity; i++) {
					if (this.freeMatchColumns[i] == 1) {
						if (!this.arguments.innerArguments[i].match(this.capturedTuple.columns[i])) {
							for (int j = 0; j < this.freeVariableList.variables.length; j++)
								this.freeVariableList.variables[j].makeFree();
							break;
						}
					} else {
						((Variable)this.arguments.innerArguments[i]).setValue(this.capturedTuple.columns[i]);
					}
				}
				
				if (i == this.arity)
					break;
			}
			
			if (getStatus == 0) {
				for (int j = 0; j < this.freeVariableList.variables.length; j++)
					this.freeVariableList.variables[j].makeFree();
				if (this.isEntry)
					status = Status.ENTRY_FAIL;
				
				this.isEntry = true;
			} else {
				status = Status.SUCCESS;
				this.isEntry = false;
			}
		}

		if (status != Status.SUCCESS)
			if (this.executionMode == ExecutionMode.Pipelined)
				this.clique.decrementBacktrackCount();

		this.traceGetTupleExit(status);

		return status;
	}
	
	public CliqueBaseNode getClique() { return this.clique; }
	
	public void setClique(CliqueBaseNode clique) { this.clique = clique; }	

	public CliqueBaseNode getParentClique() { return this.parentClique; }	
	
	public void setParentClique(CliqueBaseNode clique) { this.parentClique = clique; }

	public String getRecursiveRelationName() { return this.recursiveRelationName; }
	
	public void setRecursiveRelationName(String recursiveRelationName) { this.recursiveRelationName = recursiveRelationName; }

	@Override
	public Cursor<?> getQueryFormCursor() {
		if (this.evaluationType == EvaluationType.SSC) {
			this.queryFormCursor = this.database.getCursorManager().createSSSPCursor(this.queryFormRelation);
			return this.queryFormCursor;
		}
		
		return this.database.getCursorManager().createScanCursor(this.relation); 
	}
	
	@Override
	public Relation<?> getRelation(){
		if (this.evaluationType == EvaluationType.SSC) {			
			//TupleStoreConfiguration tsc = new TupleStoreConfiguration(TupleStoreType.Aggregation, new int[]{0});
			//tsc.setUniqueValue(true);
			//tsc.setAggregateInfos(new AggregateInfo[]{new AggregateInfo(FSAggregateType.FSMIN, DataType.INTEGER)});
			
			this.queryFormRelation = ((FSMinMaxAggregateRelationNode)this.getClique().getExitRulesOrNode()).getRelation();//.getChild(0).getChild(0)).getAggregateRelation();
			//Runtime.getDatabaseManager().getActiveDatabase().getRelationManager().createDerivedRelation(relationName,	this.getSchema(), tsc, false);

			return this.queryFormRelation;
		}
		return super.getRelation();
	}
	
	private boolean determineMaterialized() {
		if (this.bindingPattern.allFree())
			return true;
		
		if (this.bindingPattern.allBound())
			return true;

		return false;
	}
	
	public void addOtherInstance(RecursiveOrNode instance) {
		if (this.otherInstances == null)
			this.otherInstances = new LinkedList<>(); 
		
		if (!this.otherInstances.contains(instance))
			this.otherInstances.add(instance);
	}
	
	private void setMaterializedAllInstances() {
		if (this.otherInstances != null)
			for (RecursiveOrNode instance : this.otherInstances) {
				instance.isMaterialized = true;
				instance.executionMode = ExecutionMode.Materialized;
			}
	}
	
	@Override
	public RecursiveOrNode copy(ProgramContext programContext) {
		RecursiveOrNode copy = new RecursiveOrNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		
		programContext.getNodeMapping().put(this, copy);
		
		copy.clique = this.clique.copy(programContext);
		if (this.clique == this.parentClique)
			copy.parentClique = copy.clique;
		else
			copy.parentClique = this.parentClique.copy(programContext);
		
		copy.executionMode = this.executionMode;
		copy.evaluationType = this.evaluationType;
		copy.recursiveRelationName = this.recursiveRelationName;
		return copy;
	}
	
	@Override
	public void attachContext(DeALSContext deALSContext) {
		super.attachContext(deALSContext);
		this.database = deALSContext.getDatabase();
		this.clique.attachContext(deALSContext);
	}	
}
