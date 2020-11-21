package edu.ucla.cs.wis.bigdatalog.database.cursor.aggregate;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.IndexCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.type.DbAverage;
import edu.ucla.cs.wis.bigdatalog.database.type.DbSet;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;

public class AggregateIndexCursor 
	extends IndexCursor {
	protected int 				numberOfKeyColumns;
	protected AggregateInfo[] 	aggregateInfos;
	protected final boolean 	needsConversion;
	protected TypeManager		typeManager;
	
	public AggregateIndexCursor(Relation relation, int[] indexedColumns, int numberOfKeyColumns, AggregateInfo[] aggregateInfos, TypeManager typeManager) {
		super(relation, indexedColumns);
		this.numberOfKeyColumns = numberOfKeyColumns;
		this.aggregateInfos = aggregateInfos;
		this.typeManager = typeManager;
		
		boolean needsConversion = false;
		for (int i = 0; i < this.aggregateInfos.length; i++) {
			if ((this.aggregateInfos[i].aggregateType == AggregateFunctionType.AVG)
					|| (this.aggregateInfos[i].aggregateType == AggregateFunctionType.COUNT_DISTINCT)) {
				needsConversion = true;
				break;
			}
		}		
		
		this.needsConversion = needsConversion;
	}
	
	@Override
	public int getTuple(AddressedTuple tuple) {
		if (super.getTuple(tuple) > 0) {
			if (this.needsConversion) {			
				for (int i = 0; i < this.aggregateInfos.length; i++) {
					switch (this.aggregateInfos[i].aggregateType) {
						case AVG:
							DbAverage average = (DbAverage)tuple.getColumn(i + this.numberOfKeyColumns);
							tuple.columns[i + this.numberOfKeyColumns] = average.computeAverage();
							break;
						case COUNT_DISTINCT:
							DbSet set = (DbSet)tuple.getColumn(i + this.numberOfKeyColumns);
							tuple.setColumn(i + this.numberOfKeyColumns, this.typeManager.castToCountDataType(set.getNumberOfEntries()));
							break;
					}
				}
			}
			return 1;
		}
		
		return 0;				
	}
}
