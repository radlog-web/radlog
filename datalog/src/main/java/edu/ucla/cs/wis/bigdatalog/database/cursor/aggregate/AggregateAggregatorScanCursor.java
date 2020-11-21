package edu.ucla.cs.wis.bigdatalog.database.cursor.aggregate;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.CursorManager;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan.AggregatorBPlusTreeStoreScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.AggregateRelation;
import edu.ucla.cs.wis.bigdatalog.database.type.DbAverage;
import edu.ucla.cs.wis.bigdatalog.database.type.DbSet;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;

public class AggregateAggregatorScanCursor 
	extends Cursor<Tuple> 
	implements AggregateCursor<Tuple> {
	
	protected int 										numberOfKeyColumns;
	protected AggregateInfo[] 							aggregateInfos;
	protected final boolean 							needsConversion;
	protected AggregatorBPlusTreeStoreScanCursor<?,?> 	wrappedCursor;
	//protected ArithmeticExpression[] 					expressions;
	protected TypeManager								typeManager;

	public AggregateAggregatorScanCursor(AggregateRelation relation, int numberOfKeyColumns, AggregateInfo[] aggregateInfos, 
			CursorManager cursorManager, TypeManager typeManager) {
		super(relation);
		this.wrappedCursor = cursorManager.createBPlusTreeAggregationStoreScanCursor(relation);
		this.numberOfKeyColumns = numberOfKeyColumns;
		this.aggregateInfos = aggregateInfos;
		this.typeManager = typeManager;

		boolean needsConversion = false;
		for (int i = 0; i < this.aggregateInfos.length; i++) {
			if ((this.aggregateInfos[i].aggregateType == AggregateFunctionType.AVG)
					|| (this.aggregateInfos[i].aggregateType == AggregateFunctionType.COUNT_DISTINCT)) {
				needsConversion = true;
			}
		}
		
		this.needsConversion = needsConversion;
	}
		
	@Override
	public int getTuple(Tuple tuple) {
		if (this.needsConversion) {
			if (this.wrappedCursor.getTuple(tuple) > 0) {
				for (int i = 0; i < this.aggregateInfos.length; i++) {
					switch (this.aggregateInfos[i].aggregateType) {
						case AVG:
							DbAverage average = (DbAverage)tuple.getColumn(i + this.numberOfKeyColumns);
							tuple.columns[i + this.numberOfKeyColumns] = average.computeAverage();
							break;
						case COUNT_DISTINCT:
							DbSet set = (DbSet)tuple.getColumn(i + this.numberOfKeyColumns);
							tuple.columns[i + this.numberOfKeyColumns] = this.typeManager.castToCountDataType(set.getNumberOfEntries());
							break;
						default:
							tuple.columns[i + this.numberOfKeyColumns] = tuple.getColumn(i + this.numberOfKeyColumns);
					}
				}
				return 1;
			}			
			return 0;
		}
		return this.wrappedCursor.getTuple(tuple);
	}

	@Override
	public void moveNext() { }

	@Override
	public void reset() {
		this.wrappedCursor.reset();		
	}
}
