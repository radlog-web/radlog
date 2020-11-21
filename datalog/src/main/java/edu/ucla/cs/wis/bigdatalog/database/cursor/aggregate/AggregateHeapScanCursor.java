package edu.ucla.cs.wis.bigdatalog.database.cursor.aggregate;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.heap.HeapScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.type.DbAverage;
import edu.ucla.cs.wis.bigdatalog.database.type.DbSet;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;

public class AggregateHeapScanCursor 
	extends HeapScanCursor 
	implements AggregateCursor<AddressedTuple> {

	protected int 						numberOfKeyColumns;
	protected AggregateInfo[] 			aggregateInfos;
	protected final boolean 			needsConversion;
	protected TypeManager				typeManager;
	
	public AggregateHeapScanCursor(Relation<AddressedTuple> relation, int numberOfKeyColumns, AggregateInfo[] aggregateInfos, TypeManager typeManager) {
		super(relation);

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
	public int getTuple(AddressedTuple tuple) {
		if (this.needsConversion) {
			if (super.getTuple(tuple) > 0) {
				for (int i = 0; i < this.aggregateInfos.length; i++) {
					switch (this.aggregateInfos[i].aggregateType) {
						case AVG:
							DbAverage average = (DbAverage)tuple.getColumn(i + this.numberOfKeyColumns);
							tuple.columns[i + this.numberOfKeyColumns] = average.computeAverage();
							break;
						case COUNT_DISTINCT:
							DbSet dbSet = (DbSet)tuple.getColumn(i + this.numberOfKeyColumns);
							tuple.columns[i + this.numberOfKeyColumns] = this.typeManager.castToCountDataType(dbSet.getNumberOfEntries());
							break;
						default:
							tuple.columns[i + this.numberOfKeyColumns] = tuple.getColumn(i + this.numberOfKeyColumns);
					}
				}
				return 1;
			}
			return 0;
		}
		return super.getTuple(tuple);			
	}
}
