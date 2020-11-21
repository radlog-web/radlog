package edu.ucla.cs.wis.bigdatalog.database.statistics;

import java.util.ArrayList;
import java.util.HashMap;

import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.relation.BaseRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class StatisticsManager {
	private HashMap<Relation<?>, RelationStatistics> relationStatistics;
	private DeALSContext deALSContext;
	private Database database;

	public StatisticsManager(DeALSContext deALSContext, Database database) {
		this.deALSContext = deALSContext;
		this.database = database;
	}
	
	public void clear() {
		if (relationStatistics != null)
			relationStatistics.clear(); 
	}
	
	public RelationStatistics getRelationStatistics(String relationName) {
		if (relationStatistics == null)
			return null;
		
		for (Relation<?> relation : relationStatistics.keySet())
			if (relation.getName().equals(relationName))
				return relationStatistics.get(relation);

		return null;
	}
	
	public void gatherStatistics() {
		if (relationStatistics == null)
			relationStatistics = new HashMap<>();

		// we create a new list in the foreach to avoid concurrent modification 
		// base relations can be created during instantiation
		for (BaseRelation<?> baseRelation : new ArrayList<>(this.database.getRelationManager().getBaseRelations()))
			getStatistics(baseRelation);
	}
	
	private RelationStatistics getStatistics(BaseRelation<?> baseRelation) {			
		if (relationStatistics.containsKey(baseRelation))
			return relationStatistics.get(baseRelation);
		
		RelationStatistics rs = new RelationStatistics(deALSContext, baseRelation);
		rs.calculateStatistics();
		relationStatistics.put(baseRelation, rs);
		return rs;
	}
	
	public String toString() {
		StringBuilder retval = new StringBuilder();
		for (RelationStatistics rs : relationStatistics.values()) {
			retval.append(rs.toString());
			retval.append("\n");
		}
		return retval.toString();
	}
}
