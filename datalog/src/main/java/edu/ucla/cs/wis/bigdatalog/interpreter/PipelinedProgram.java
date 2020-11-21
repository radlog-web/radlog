package edu.ucla.cs.wis.bigdatalog.interpreter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.api.RecursionInformation;
import edu.ucla.cs.wis.bigdatalog.compiler.ProgramRules;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.BaseRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InputVariable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.AndNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.NegationOrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.AggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.RecursiveLiteral;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.RecursiveOrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueBaseNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.MaterializedPredicate;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.RelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.BaseRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.xy.XYCliqueNode;

public class PipelinedProgram extends Program<OrNode> implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public PipelinedProgram(OrNode rootNode, ProgramRules programRules) {
		super(rootNode, programRules);
	}
	
	public boolean initialize() {
		return this.root.initialize();
	}
	
	public Argument getArgument(int position) {
		return this.root.getArgument(position);
	}
	
	public NodeArguments getArguments() { return this.root.getArguments(); }
	
	public List<RecursionInformation> getRecursionInformationByClique() {
		List<CliqueBaseNode> cliques = new ArrayList<>();
		this.getCliques(this.root, cliques);
		
		List<RecursionInformation> recursionInfos = new ArrayList<>();
		for (CliqueBaseNode clique : cliques)
			recursionInfos.add(new RecursionInformation(clique.getPredicateName(), 
					clique.getEvaluationType(), 
					clique.getNumberOfIterations(), 
					clique.getDeltaFactsByIteration(),
					clique.getGeneratedFactsByIteration()));
		
		return recursionInfos;
	}
			
	public void cleanUp() {
		this.root.cleanUp();
		this.root.deleteRelationsAndCursors();
				
		//CursorManager.clear();
		//IndexManager.clearNonBaseIndexes();
		
		//Arithmetic.clear();
	}
		
	public String toString() {
		return this.root.toStringTree();
	}
	
	protected String getNodeInfo(OrNode node) {
		StringBuilder retval = new StringBuilder();
		if (node instanceof RecursiveLiteral) {
			//Relation relation = ((RecursiveLiteral)node).getRelation();
			retval.append("  Recursive Relation Node: " + node.getPredicateName());
			retval.append(" [Relation: " + ((RecursiveLiteral)node).getRelation().getName() + " | ");
			retval.append(" Evaluation Type: " + ((RecursiveLiteral)node).getEvaluationType() + "]\n");
		} else if (node instanceof RecursiveOrNode) {
			CliqueBaseNode clique = ((RecursiveOrNode) node).getClique();
			retval.append(clique.getPredicateName() + " [# iterations: " + clique.getNumberOfIterations() + "]\n");
			retval.append(getNodeInfo(clique.getExitRulesOrNode()));
			if (clique instanceof CliqueNode)
				retval.append(getNodeInfo(((CliqueNode) clique).getRecursiveRulesOrNode()));
		} else if (node instanceof BaseRelationNode) {
			retval.append("  Schema Relation Node: " + node.getPredicateName());
			retval.append(" [Relation: " + ((BaseRelationNode)node).getRelation().getName() + "]\n");
			//retval.append(((SchemaRelationNode)node).getRelation().toStringDetails());
			//retval.append("[Tuple Store: " + ((SchemaRelationNode)node).getRelation().getTupleStore().getClass().getSimpleName() + "]");
		} else if (node instanceof AggregateRelationNode) {
			retval.append("  Aggregate Relation Node: " + node.getPredicateName());
			retval.append(" [Relation: " + ((AggregateRelationNode)node).getRelation().getName());
			/*if ((node.getClass() == FSSingleAggregateRelationNode.class) || (node.getClass() == FSCountSingleAggregateRelationNode.class))
				retval.append("[FS Single]");
			else if ((node instanceof FSCountDoubleDeltaAggregateRelationNode))
				retval.append(", Relation: " + ((FSCountDoubleDeltaAggregateRelationNode)node).getDetailsRelation().getName() + "[FS Double Delta]");
			else if ((node instanceof FSCountDoubleAggregateRelationNode))
				retval.append(", Relation: " + ((FSCountDoubleAggregateRelationNode)node).getDetailsRelation().getName() + "[FS Double]");
			else if ((node.getClass() == FSMaxBased2SingleAggregateRelationNode.class))
				retval.append("[FS Max Based Single]");*/
			retval.append("]\n");
		} else if (node instanceof RelationNode) {
			retval.append("  Relation Node: " + node.getPredicateName());
			retval.append(" [Relation: " + ((RelationNode)node).getRelation().getName() + "]\n");
			//	retval.append("[Tuple Store: " + ((RelationNode)node).getRelation().getTupleStore().getClass().getSimpleName() + "]");
			//retval.append(((RelationNode)node).getRelation().toStringDetails());
		}
		
		for (int i = 0; i < node.getNumberOfChildren(); i++)
			retval.append(getNodeInfo2(node.getChild(i)));
				
		return retval.toString();
	}
	
	protected boolean isUsedInProgram(BaseRelation<?> baseRelation, OrNode node) {
		if (node instanceof BaseRelationNode) {
			if (((BaseRelationNode)node).getRelation().equals(baseRelation))
				return true;
		} else if (node instanceof RecursiveOrNode) {
			CliqueBaseNode clique = ((RecursiveOrNode) node).getClique();
			if (isUsedInProgram(baseRelation, clique.getExitRulesOrNode()))
				return true;
			
			if (clique instanceof CliqueNode) {
				if (isUsedInProgram(baseRelation, ((CliqueNode)clique).getRecursiveRulesOrNode()))
					return true;
			}/* else if (clique instanceof XYCliqueNode) {
				if (isUsedInProgram(baseRelation, ((XYCliqueNode)clique).getXRulesOrNode()))
					return true;
				
				if (isUsedInProgram(baseRelation, ((XYCliqueNode)clique).getYRulesOrNode()))
					return true;
				
				if (isUsedInProgram(baseRelation, ((XYCliqueNode)clique).getCopyRulesOrNode()))
					return true;
				
				if (isUsedInProgram(baseRelation, ((XYCliqueNode)clique).getDeleteRulesOrNode()))
					return true;
			}*/
		}
		
		for (int i = 0; i < node.getNumberOfChildren(); i++) {
			if (isUsedInProgram2(baseRelation, node.getChild(i)))
				return true;		
		}
		return false;
	}

	protected void getCliques(OrNode node, List<CliqueBaseNode> cliques) {
		if (node instanceof RecursiveOrNode) {
			CliqueBaseNode clique = ((RecursiveOrNode)node).getClique();
			if (!cliques.contains(clique)) {
				cliques.add(clique);
				getCliques(clique.getExitRulesOrNode(), cliques);
			
				if (clique instanceof CliqueNode)
					getCliques(((CliqueNode) clique).getRecursiveRulesOrNode(), cliques);
			}
		}// else if (node instanceof MaterializedClique) {
		//	getCliques(((MaterializedClique) node).getOrNode(), cliques);
				
		for (int i = 0; i < node.getNumberOfChildren(); i++)
			getCliques2(node.getChild(i), cliques);
	}
	
	protected void getCliques2(AndNode node, List<CliqueBaseNode> cliques) {
		for (int i = 0; i < node.getNumberOfChildren(); i++)
			getCliques(node.getChild(i), cliques);		
	}

	// This execute method gets tuple from the program and inserts into the resultRelation.
	@Override
	public Status execute(Relation resultRelation) {
		int numberOfColumns = this.root.getArity();
		Status status = this.root.getTuple();
				
		// for existential queries
		if (numberOfColumns > 0) {
			Tuple tuple = null;
			if (resultRelation != null)
				tuple = resultRelation.getEmptyTuple();
			
			while (status == Status.SUCCESS) {
				if (resultRelation != null) {
					for (int i = 0; i < numberOfColumns; i++)
						tuple.columns[i] = this.root.getArgumentAsDbType(i);

					resultRelation.add(tuple);
				}
				status = this.root.getTuple();	
			}
		}
		return status;
	}

	// This execute method will materialize the root node and then later, the interpreter will use a cursor to read from the root's relation.
	@Override
	public Status execute() {
		Status status;
		if (this.root instanceof AggregateRelationNode) {
			((AggregateRelationNode)this.root).setExecutionModeMaterialized();
			// only one call to getTuple is needed when rootnode is set as materialized;
			status = this.root.getTuple();
		} else { 
			status = this.root.getTuple();
				
			// for existential queries we only need 1 result
			if (this.root.getArity() > 0)
				while ((status = this.root.getTuple()) == Status.SUCCESS) {};
		}
		
		return status;
	}
	
	// this method should be called after program arguments are bound 
	public void compressBoundArguments() {
		//System.out.println(this.rootNode.toString());
		compressInputVariables(this.root);
		//System.out.println(this.rootNode.toString());
	}
	
	public void compressInputVariables(OrNode node) {
		for (int i = 0; i < node.getArity(); i++)
			if (node.getArgument(i) instanceof InputVariable)
				node.arguments.innerArguments[i] = node.getArgumentAsDbType(i);//((InputVariable)node.getArgument(i)).getValue().toDbType();			
				
		if (node instanceof RecursiveOrNode) {
			CliqueBaseNode clique = ((RecursiveOrNode)node).getClique();
			
			compressInputVariables(clique.getExitRulesOrNode());
			
			if (clique instanceof CliqueNode) {
				compressInputVariables(((CliqueNode)clique).getRecursiveRulesOrNode());
			} else if (clique instanceof XYCliqueNode) {
				XYCliqueNode xyClique = (XYCliqueNode) clique;

				compressInputVariables(xyClique.getXRulesOrNode());

				compressInputVariables(xyClique.getYRulesOrNode());

				compressInputVariables(xyClique.getCopyRulesOrNode());

				compressInputVariables(xyClique.getDeleteRulesOrNode());
			}
		} else if (node instanceof MaterializedPredicate) {
			compressInputVariables(((MaterializedPredicate) node).getMaterializedRule());
		} else if (node instanceof NegationOrNode) {
			compressInputVariables(((NegationOrNode) node).getLiteralToBeNegated());
		}
		
		for (AndNode child : node.getChildren())
			compressInputVariables(child);
	}
	
	public void compressInputVariables(AndNode node) {
		for (int i = 0; i < node.getArity(); i++)
			if (node.getArgument(i) instanceof InputVariable)
				node.arguments.innerArguments[i] = node.getArgumentAsDbType(i);//((InputVariable)node.arguments.innerArguments[i]).getValue().toDbType();
		
		for (OrNode child : node.getChildren())
			compressInputVariables(child);
	}
}
