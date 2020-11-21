package edu.ucla.cs.wis.bigdatalog.compiler.predicate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.QueryForm;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregateType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.ArgumentType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.expression.CompilerArithmeticExpression;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerInputVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYPredicateType;

public class Predicate extends PredicateBase implements Serializable {
	private static final long serialVersionUID = 1L;

	public static int GLOBAL_PREDICATE_NAME_COUNT = 1;
	
	protected XYPredicateType			xyPredicateType; /*HW*/
	protected PredicateOperatorType 	predicateOperatorType;
	protected PredicateType 			predicateType;
	protected boolean 					isRecursive;
	protected CompilerTypeList 			arguments;
	protected FSAggregateType			fsAggregatePredicateType;

	//APS 4/15/2013 @DATALOGFS - so we can identify which arguments are aggregates
	protected List<ArgumentType> 		argumentTypeAdornment;	

	public Predicate(String predicateName, CompilerTypeList arguments, CompilerType compilerType, 
			PredicateOperatorType predicateOperatorType, PredicateType predicateType) {
		super(predicateName, arguments.size(), compilerType);
		
		this.predicateOperatorType = predicateOperatorType;
		this.predicateType = predicateType;
		this.arguments = arguments;
		this.xyPredicateType = XYPredicateType.NONE;
		this.isRecursive = false;
		this.fsAggregatePredicateType = FSAggregateType.NONE;
	}	

	public Predicate(String predicateName, CompilerTypeList arguments, CompilerType compilerType, PredicateType predicateType) {
		this(predicateName, arguments, compilerType, PredicateOperatorType.POSITIVE, predicateType);
	}
	
	public Predicate(String predicateName, CompilerTypeList arguments, CompilerType compilerType) {
		this(predicateName, arguments, compilerType, PredicateOperatorType.POSITIVE, PredicateType.DERIVED);
	}

	public Predicate(String predicateName, CompilerTypeList arguments) {
		this(predicateName, arguments, CompilerType.PREDICATE);
	}
	
	@Override
	public int getArity() { return this.arguments.size();}

	public CompilerTypeList getArguments() { return this.arguments; }

	public CompilerTypeBase getArgument(int i) { return (this.arguments.get(i)); }

	public void setArguments(CompilerTypeList arguments) {
		this.arguments.clearExceptVariables();
		this.arguments = arguments;
		this.arity = this.arguments.size();
	}

	public void setArgument(int position, CompilerTypeBase argument) {
		this.arguments.set(position, argument);
	}
	
	public void addArgument(CompilerTypeBase argument) {
		this.addArgument(this.arity, argument);
	}
	
	public void addArgument(int position, CompilerTypeBase argument) {
		this.arguments.add(position, argument);
		this.arity++;
	}
	
	public void removeArgument(int position) {
		this.arguments.remove(position);
		this.arity--;
		if (this.argumentTypeAdornment != null && this.argumentTypeAdornment.size() > position)
			this.argumentTypeAdornment.remove(position);
	}
	
	public void setAsRecursive() { this.isRecursive = true; }
	
	public void setAsNonRecursive() { this.isRecursive = false; }

	public boolean isRecursive() { return this.isRecursive; }

	// start predicate types	
	public PredicateType getPredicateType() { return this.predicateType;}

	public void setPredicateType(PredicateType predicateType) { this.predicateType = predicateType; }	

	public void setAsDerivedPredicate() {this.predicateType = PredicateType.DERIVED;}

	public void setAsBasePredicate() {this.predicateType = PredicateType.BASE;}

	public boolean isDerived() {return (this.predicateType == PredicateType.DERIVED);}

	public boolean isBase() {return (this.predicateType == PredicateType.BASE); }

	public boolean isBuiltIn() {return (this.predicateType == PredicateType.BUILT_IN); }
	// end predicate types
	// start predicate operator types
	public PredicateOperatorType getPredicateOperatorType() {return (this.predicateOperatorType);}

	public void setPredicateOperatorType(PredicateOperatorType predicateOperatorType) {this.predicateOperatorType = predicateOperatorType;}

	public void setAsPositive() {this.predicateOperatorType = PredicateOperatorType.POSITIVE;}

	public void setAsNegative() {this.predicateOperatorType = PredicateOperatorType.NEGATIVE;}

	public boolean isPositive() {return (this.predicateOperatorType == PredicateOperatorType.POSITIVE); }

	public boolean isNegative() {return (this.predicateOperatorType == PredicateOperatorType.NEGATIVE);}
	// end predicate operator types
		
	public Predicate copy() {
		Predicate predicate = new Predicate(this.predicateName, this.arguments.copy(), 
				this.getType(), this.predicateOperatorType, this.predicateType);
		predicate.setArgumentTypeAdornment(this.getArgumentTypeAdornment());
		predicate.setFSAggregateType(this.getFSAggregateType());
		return predicate;
	}

	public Predicate copy(CompilerVariableList variableList) { 
		Predicate predicate = new Predicate(this.predicateName, this.arguments.copy(variableList), 
				this.getType(), this.predicateOperatorType, this.predicateType);
		predicate.setArgumentTypeAdornment(this.getArgumentTypeAdornment());
		predicate.setFSAggregateType(this.getFSAggregateType());
		return predicate;
	}

	public String toString() {
		StringBuilder output = new StringBuilder();
		switch (this.predicateOperatorType)
		{
			case POSITIVE:
				break;
			case NEGATIVE:
				output.append("~");
				break;
		}

		output.append(this.predicateName.toString());
		output.append("(");
		
		if (this.arity > 0) {
			for (int i = 0; i < this.arguments.size(); i++) {
				if (i > 0)
					output.append(", ");
				output.append(this.arguments.get(i).toString());
			}
		}
		output.append(")");
		return output.toString();
	}
	
	public String toStringShort() {
		StringBuilder output = new StringBuilder();
		switch (this.predicateOperatorType)
		{
			case POSITIVE:
				break;
			case NEGATIVE:
				output.append("~");
				break;
		}
		
		if ((this.predicateType == PredicateType.BUILT_IN) 
				&& ((BuiltInPredicate)this).getBuiltInPredicateType() == BuiltInPredicateType.BINARY) {
			output.append(this.arguments.get(0));
			output.append(this.predicateName.toString());
			output.append(this.arguments.get(1));
		} else {
			output.append(this.predicateName.toString());
			output.append("(");	
			for (int i = 0; i < this.arguments.size(); i++) {
				if (i > 0)
					output.append(", ");
				output.append(this.arguments.get(i).toString());
			}
			output.append(")");
		}
		return output.toString();
	}
	
	public String toStringCompilable() {
		if ((this.predicateType == PredicateType.BUILT_IN) 
				&& ((BuiltInPredicate)this).getBuiltInPredicateType() == BuiltInPredicateType.BINARY) {
			StringBuilder output = new StringBuilder();
			output.append(this.arguments.get(0));
			output.append(this.predicateName.toString());
			if (this.arguments.get(1) instanceof CompilerArithmeticExpression)
				output.append(((CompilerArithmeticExpression)this.arguments.get(1)).toStringInfix());
			else
				output.append(this.arguments.get(1));
			return output.toString();
		}
		return toString();		
	}
	
	public String toJson() {
		StringBuilder output = new StringBuilder();
		output.append("{\"name\":\"");
		output.append(this.predicateName);
		output.append("\", \"arguments\":[");

		for (int i = 0; i < this.arguments.size(); i++) {
			if (i > 0)
				output.append(",");
			String value = "";
			String dataType = "UNKNOWN";
			if (this.arguments.get(i).isVariable()) {
				dataType = ((CompilerVariable)this.arguments.get(i)).getDataType().name();
				value = ((CompilerVariable)this.arguments.get(i)).getVariableName();
			} else {
				value = this.arguments.get(i).toString();
			}
			
			output.append("[\"");
			output.append(value);
			output.append("\", \"");						
			output.append(dataType);
			output.append("\"]");
		}
		output.append("]}");
		return output.toString();
	}

	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof Predicate))
			return false;
		
		Predicate otherPredicate = (Predicate)other;
		
		return (this.predicateName.equals(otherPredicate.getPredicateName()) 
				&& this.arity == otherPredicate.getArity() 
				&& this.arguments.equals(otherPredicate.getArguments()));
	}
	
	public XYPredicateType getXYPredicateType() { return this.xyPredicateType; }
	
	public void setXYPredicateType(XYPredicateType xyPredicateType) { this.xyPredicateType = xyPredicateType; }
	
	public FSAggregateType getFSAggregateType() { return this.fsAggregatePredicateType; }
	
	public void setFSAggregateType(FSAggregateType fsAggregatePredicateType) { this.fsAggregatePredicateType = fsAggregatePredicateType; }
	
	public void determineFSAggregatePredicateType() {
		int count = 0;
		for (CompilerTypeBase arg: this.getArguments()) {
			if (arg.containsFSAggregate()) {
				this.fsAggregatePredicateType = ((FSAggregate)arg).getFSAggregateType();
				count++;
			}
		}
		
		if (count > 1)
			this.fsAggregatePredicateType = FSAggregateType.FSMANY;
	}
	
	public void generateArgumentTypeAdornment() {
		List<ArgumentType> adornment = new ArrayList<>();
		CompilerTypeList args = this.getArguments();
		
		CompilerTypeBase arg;
		for (int i = 0; i < args.size(); i++) {
			arg = args.get(i);
			if (arg.containsBuiltInAggregate() || arg.containsUserDefinedAggregate())
				adornment.add(ArgumentType.AGGREGATE);
			else if (arg.containsFSAggregate())
				adornment.add(ArgumentType.FSAGGREGATE);
			else if (arg.isConstant())
				adornment.add(ArgumentType.CONSTANT);
			else if (arg.isVariable())
				adornment.add(ArgumentType.VARIABLE);
			else if (arg.isInputVariable())
				adornment.add(ArgumentType.INPUT_VARIABLE);
			else if (arg.isExpression())
				adornment.add(ArgumentType.EXPRESSION);
			else
				adornment.add(ArgumentType.UNKNOWN);
		}
		
		this.argumentTypeAdornment = adornment;
	}
	
	public void setArgumentTypeAdornment(List<ArgumentType> argumentTypeAdornment) {
		if (this.argumentTypeAdornment == null 
				|| this.argumentTypeAdornment.size() == 0 
				|| this.argumentTypeAdornment.size() == argumentTypeAdornment.size())
			this.argumentTypeAdornment = argumentTypeAdornment;
	}
	
	public List<ArgumentType> getArgumentTypeAdornment() {
		if (this.argumentTypeAdornment == null)
			this.generateArgumentTypeAdornment();
		return this.argumentTypeAdornment; 
	}
	
	public boolean hasArgumentTypeAdornment() { return (this.argumentTypeAdornment != null); }
	
	public boolean isGround() {
		for (CompilerTypeBase arg : this.arguments)
			if (!arg.isGround())
				return false;
		return true;
	}

	public QueryForm getQueryForm() {
		int inputVariableCount = 0;
		CompilerVariableList variableList = new CompilerVariableList();
		CompilerTypeList queryFormArguments = new CompilerTypeList();
		for (CompilerTypeBase arg : this.arguments) {
			if (arg.isConstant()) {
				queryFormArguments.add(new CompilerInputVariable("$InputVar_" + inputVariableCount));
				inputVariableCount++;
			} else {
				queryFormArguments.add(arg.copy(variableList));
			}
		}
		
		return new QueryForm(this.predicateName, queryFormArguments);
	}
}
