package edu.ucla.cs.wis.bigdatalog.database.statistics;

import edu.ucla.cs.wis.bigdatalog.api.CommandResult;
import edu.ucla.cs.wis.bigdatalog.api.CommandWrapper;
import edu.ucla.cs.wis.bigdatalog.common.Triple;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.BaseRelation;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbString;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.system.ReturnStatus;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class RelationStatistics {

	private CommandWrapper commandWrapper;
	private BaseRelation<?> baseRelation;
	private ColumnStatistics<?> columnStatistics[];
	private long rowCount;
	
	public RelationStatistics(DeALSContext deALSContext, BaseRelation<?> baseRelation) {
		this.baseRelation = baseRelation;		
		this.commandWrapper = new CommandWrapper(deALSContext);
	}
	
	public BaseRelation<?> getBaseRelation() { return this.baseRelation; }
	
	public ColumnStatistics<?>[] getColumnStatistics() { return this.columnStatistics; }
	
	public ColumnStatistics<?> getColumnStatistics(int index) { return this.columnStatistics[index]; }
	
	public long getRowCount() { return this.rowCount; }
	
	public void calculateStatistics() {
		DataType[] schema = this.baseRelation.getSchema();
		
		this.columnStatistics = new ColumnStatistics<?>[schema.length];
		
		this.rowCount = calculateRowCount();
		
		if (this.rowCount > 0) {		
			for (int i = 0; i < schema.length; i++)
				this.columnStatistics[i] = calculate(schema[i], i);
		}		
	}
	
	private long calculateRowCount() {
		Triple<String, String, String> retvalTriple = generateRowCountRule();		
		Tuple[] results = executeRule(retvalTriple.getFirst(), 
				retvalTriple.getSecond(), 
				retvalTriple.getThird());
		if (results == null || results.length == 0)
			return 0;
		
		// should be only 1 column in tuple and it is an integer
		return results[0].getColumn(0).getIntValue();
	}
	
	private ColumnStatistics<?> calculate(DataType type, int position) {
		Tuple[] frequencyResults = null;
		Tuple[] statisticsResults = null;
		ColumnStatistics<?> stats = null;
		
		Triple<String, String, String> retvalTriple = generateFrequencyRule(position);		
		frequencyResults = executeRule(retvalTriple.getFirst(), retvalTriple.getSecond(), retvalTriple.getThird());
				
		if (type != DataType.STRING) {
			retvalTriple = generateStatisticsRule(position);
			statisticsResults = executeRule(retvalTriple.getFirst(), retvalTriple.getSecond(), retvalTriple.getThird());
		} else {
			// string will look at the raw data using a cursor
			Cursor cursor = this.baseRelation.getDatabase().getCursorManager().createScanCursor(this.baseRelation);
			Tuple tuple = cursor.getEmptyTuple();
			int key;
			int min = Integer.MAX_VALUE;
			int max = Integer.MIN_VALUE;
			while (cursor.getTuple(tuple) > 0) {
				key = ((DbString)tuple.getColumn(position)).getKey();
				if (key < min)
					min = key;
				if (key > max)
					max = key;
			}
			Tuple statisticsTuple = new Tuple(2);
			statisticsTuple.setColumn(0, DbInteger.create(max));
			statisticsTuple.setColumn(1, DbInteger.create(min));
			statisticsResults = new Tuple[]{statisticsTuple};
		}
		
		switch(type) {
			case DOUBLE:
				stats = new DoubleColumnStatistics(frequencyResults, statisticsResults[0]);
				break;
			case FLOAT:
				stats = new FloatColumnStatistics(frequencyResults, statisticsResults[0]);
				break;
			case INT:
				stats = new IntegerColumnStatistics(frequencyResults, statisticsResults[0]);
				break;
			case LONG:
				stats = new LongColumnStatistics(frequencyResults, statisticsResults[0]);
				break;
			case STRING:
				stats = new StringColumnStatistics(frequencyResults, statisticsResults[0]);
				break;
		}
		return stats;
	}
	
	public Triple<String, String, String> generateRowCountRule() {
		String prefix = "stats_" + this.baseRelation.getName() + "rowCount";
		
		String queryForm = prefix + "(A).";		
		String rule = prefix + "(count<A>) <- ";
		String export = prefix + "(A).";
		
		rule += this.baseRelation.getName() + "(";
			
		for (int i = 0; i < this.baseRelation.getArity(); i++) {
			if (i > 0) 
				rule += ",";
			if (i == 0)
				rule += "A";
			else
				rule += "_";
		}	
		rule += ").";
		
		return new Triple<>(rule, export, queryForm);
	}
	
	public Triple<String, String, String> generateFrequencyRule(int position) {		
		String prefix = "stats_" + this.baseRelation.getName() + position;
	
		String queryForm = prefix + "(A, A1).";
		String rule = prefix + "(A, count<A>) <- ";
		String export = prefix + "(A, B).";
			
		rule += this.baseRelation.getName() + "(";
			
		for (int i = 0; i < this.baseRelation.getArity(); i++) {
			if (i > 0) 
				rule += ",";
			if (i == position)
				rule += "A";
			else
				rule += "_";
		}
			
		rule += ").";

		return new Triple<>(rule, export, queryForm);
	}

	public Triple<String, String, String> generateStatisticsRule(int position) {		
		String prefix = "stats_" + this.baseRelation.getName() + position;
		
		String queryForm = prefix +  "(A1, A2, A3).";
		String rule = prefix + "(avg<A>, max<A>, min<A>) <- ";
		String export = prefix + "(B, C, D).";
					
		rule += this.baseRelation.getName() + "(";
			
		for (int i = 0; i < this.baseRelation.getArity(); i++) {
			if (i > 0) 
				rule += ",";
			if (i == position)
				rule += "A";
			else
				rule += "_";
		}
			
		rule += ").";

		return new Triple<>(rule, export, queryForm);
	}
	
	public Tuple[] executeRule(String rule, String export, String queryForm) {
		CommandResult commandResult;
		Tuple[] result = null;
		
		commandResult = this.commandWrapper.execute("add-rule " + rule);
		commandResult = this.commandWrapper.execute("add-export " + export);
		commandResult = this.commandWrapper.execute("compile");
		commandResult = this.commandWrapper.execute("query " + queryForm);
		if (commandResult.getReturnStatus() == ReturnStatus.SUCCESS)
			result = commandResult.getQueryResults().getResults();
		
		String predicateName = queryForm.substring(0, queryForm.indexOf("("));
		int arity = queryForm.split(",").length;
		
		commandResult = this.commandWrapper.execute("delete-rule " + predicateName + " " + arity);
		commandResult = this.commandWrapper.execute("delete-export " + queryForm);
		
		return result;
	}
	
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("Relation: " + this.baseRelation.getName() + "\n");
		for (int i = 0; i < this.columnStatistics.length; i++)
			retval.append(this.columnStatistics[i].toString() + "\n");
				
		return retval.toString();
	}
}
