package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework;

import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.BuiltInAggregateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.type.DbAverage;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.interpreter.ArithmeticExpression;
import edu.ucla.cs.wis.bigdatalog.interpreter.ArithmeticOperation;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;

public class SingleMultiNode 
	extends OrNode {	

	enum RuleType { SINGLE, MULTI, RETURN }
	
	protected RuleType ruleType;
	protected BuiltInAggregateType builtInAggregateType;
	protected ArithmeticExpression expression;
	
	public SingleMultiNode(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables) {		  
		super(BuiltInPredicate.SINGLE_MULTI_PREDICATE_NAME, args, binding, freeVariables);
		
		if (predicateName.equals("return"))
			this.ruleType = RuleType.RETURN;
		else if (predicateName.equals("single"))
			this.ruleType = RuleType.SINGLE;
		else 
			this.ruleType = RuleType.MULTI;
		
		this.builtInAggregateType = BuiltInAggregateType.getBuiltInAggregateType(((DbInteger)args.get(0)).getValue());
		if (this.builtInAggregateType == BuiltInAggregateType.SUM)
			this.expression = new ArithmeticExpression(ArithmeticOperation.ADDITION);
		else if (this.builtInAggregateType == BuiltInAggregateType.AVG)
			this.expression = new ArithmeticExpression(ArithmeticOperation.DIVISION);
	}
	
	public boolean initialize() {
		if (this.ruleType == RuleType.SINGLE)
			readAggregateNodes.peek().addAggregateInfo(new AggregateInfo(this.builtInAggregateType));
		return true;
	}

	public void deleteRelationsAndCursors() {	}

	public void cleanUp() {
		this.baseNodeCleanUp();
	}

	public void partialCleanUp() {
		this.cleanUp();
	}
	
	public String toString() { return toStringNode(); }

	public Status getTuple() {
		Status status = Status.FAIL;
		DbNumericType arg0, arg1;
		DbAverage arg2;
		DbTypeBase retvalDbTypeObject;
		DbTypeBase[] dbTypeList = new DbTypeBase[2];
		
		this.traceGetTupleEntry();
	  
		this.freeVariableList.makeFree();
	  
		if (this.isEntry) {
			this.isEntry = false;

			switch (this.ruleType) {
				case RETURN:
					switch (this.builtInAggregateType) {
						case AVG: 
							this.getArgument(2).match(((DbAverage) this.getArgumentAsDbType(1)).computeAverage());
							status = Status.SUCCESS;
							
							break;
						case SUM:
						case MAX:
						case MIN:
						case COUNT:
						case COUNT_DISTINCT:
							if (this.getArgument(2).matchByFree(this.getArgumentAsDbType(1))) {
								status = Status.SUCCESS;
							} else {
								status = Status.ENTRY_FAIL;
								this.cleanUp();
							}
							break;
					}
					break;
			case SINGLE:
				switch(this.builtInAggregateType) {
					case AVG: 							
						this.getArgument(2).match(DbAverage.create((DbNumericType)this.getArgumentAsDbType(1)));
						status = Status.SUCCESS;

						break;
					case MAX:
					case MIN:
						//  Ynew=N
						if (this.getArgument(2).matchByFree(this.getArgumentAsDbType(1))) {
							status = Status.SUCCESS;
						} else {
							status = Status.ENTRY_FAIL;
							this.cleanUp();
						}
						break;
					case SUM:
						if (this.getArgument(2).matchByFree(this.getArgumentAsDbType(1))) {
							status = Status.SUCCESS;
						} else {
							status = Status.ENTRY_FAIL;
							this.cleanUp();
						}
						break;
					case COUNT:
						this.getArgument(2).match(this.typeManager.castToCountDataType(1));
						status = Status.SUCCESS;
						break;
					case COUNT_DISTINCT:
						status = Status.SUCCESS;
						this.getArgument(2).match(this.getArgumentAsDbType(1));
						break;
				}
				break;	      
			case MULTI: 

				switch(this.builtInAggregateType) {
					case SUM:
						arg0 = (DbNumericType) this.getArgumentAsDbType(1);
						arg1 = (DbNumericType) this.getArgumentAsDbType(2);
						// Ynew=N+Yold
						this.getArgument(3).match(this.expression.evaluate(arg0, arg1));
						status = Status.SUCCESS;
						break;
					case COUNT:
						arg0 = (DbNumericType) this.getArgumentAsDbType(1);
						arg1 = (DbNumericType) this.getArgumentAsDbType(2);
						// Ynew=1+Yold						
						this.getArgument(3).match(arg1.add(this.typeManager.castToCountDataType(1)));
						status = Status.SUCCESS;
						break;
					case COUNT_DISTINCT:
						// just pass through and let the write aggregate put it into the set to deteremine if it is new or not						
						this.getArgument(3).match(this.getArgumentAsDbType(1));
						status = Status.SUCCESS;
						break;
					case AVG:
						DbAverage average = (DbAverage)this.getArgumentAsDbType(2);
						average.accrue((DbNumericType) this.getArgumentAsDbType(1));
						this.getArgument(3).match(average);
						
						status = Status.SUCCESS;
						break;
					case MAX:
						arg0 = (DbNumericType) this.getArgumentAsDbType(1);
						arg1 = (DbNumericType) this.getArgumentAsDbType(2);
						// Ynew=max(Yold,N)
						if (arg0.greaterThan(arg1))
							this.getArgument(3).match(arg0);
						else
							this.getArgument(3).match(arg1);
						status = Status.SUCCESS;
						break;
					case MIN:
						arg0 = (DbNumericType) this.getArgumentAsDbType(1);
						arg1 = (DbNumericType) this.getArgumentAsDbType(2);
						// Ynew=min(Yold,N)
						if (arg0.lessThan(arg1))
							this.getArgument(3).match(arg0);
						else
							this.getArgument(3).match(arg1);
						status = Status.SUCCESS;
						break;
				}
				break;
			}
		} else {
			status = Status.FAIL;
			this.cleanUp();
		}
	  
		this.traceGetTupleExit(status);

		return status;
	}
	
	@Override
	public SingleMultiNode copy(ProgramContext programContext) {
		SingleMultiNode copy = new SingleMultiNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		
		programContext.getNodeMapping().put(this, copy);
		
		return copy;
	}
}
