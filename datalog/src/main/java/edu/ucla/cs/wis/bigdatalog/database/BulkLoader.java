package edu.ucla.cs.wis.bigdatalog.database;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import edu.ucla.cs.wis.bigdatalog.database.relation.BaseRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreBase;
import edu.ucla.cs.wis.bigdatalog.database.type.DbDateTime;
import edu.ucla.cs.wis.bigdatalog.database.type.DbDouble;
import edu.ucla.cs.wis.bigdatalog.database.type.DbFloat;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLong;
import edu.ucla.cs.wis.bigdatalog.exception.RelationManagerException;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.system.ReturnStatus;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class BulkLoader {
	
	public static ReturnStatus load(DeALSContext deALSContext, Database database, String filePath, boolean sortRelation) {
		FileReader fr = null;
		BufferedReader br = null;
		
		// read the second line to get the arity of the predicate by parsing
		try {
			fr = new FileReader(filePath);
			br = new BufferedReader(fr);
			String line = br.readLine();	
			
			while (line != null && line.trim().startsWith("%"))
				line = br.readLine();
			
			if (line == null)
				return ReturnStatus.SUCCESS;
			
			String predicateName = null;
			if (line.toLowerCase().startsWith("all "))
				predicateName = line.substring(line.indexOf("all") + 4).trim();
			
			line = br.readLine();
			if (line == null)
				return ReturnStatus.ERROR;

			String[] attributes = line.split(",");			 
			int arity = attributes.length;

			BaseRelation relation = database.getRelationManager().getBaseRelation(predicateName);
			if (relation == null)
				throw new RelationManagerException("relation [" + predicateName + "|" + arity + "] does not exist.");

			TupleStoreBase tupleStore = relation.getTupleStore();
			
			DataType[] schema = relation.getSchema();
			Tuple tuple = relation.getEmptyTuple();
			int counter = 0;
			do {
				if (line.startsWith("("))
					line = line.substring(line.indexOf("(") + 1);
				if (line.endsWith("."))
					line = line.substring(0, line.length() - 1);
				if (line.endsWith(")"))
					line = line.substring(0, line.length() - 1);
						
				attributes = line.split(",");
	
				for (int i = 0; i < schema.length; i++) {
					if (schema[i] == DataType.STRING) {
						if (attributes[i].charAt(0) != '"') {
							tuple.setColumn(i, database.getTypeManager().createString(attributes[i]));
						} else {
							String trimmed;
							if (attributes[i].charAt(attributes[i].length() - 1) == '"')
								trimmed = attributes[i].substring(1, attributes[i].length() - 1);
							else
								trimmed = attributes[i].substring(1);
							tuple.setColumn(i, database.getTypeManager().createString(trimmed));
						}
					} else if (schema[i] == DataType.INT) {
						tuple.setColumn(i, DbInteger.create((int)Double.parseDouble(attributes[i])));
					} else if (schema[i] == DataType.FLOAT) {
						tuple.setColumn(i, DbFloat.create(Float.parseFloat(attributes[i])));
					} else if (schema[i] == DataType.DOUBLE) {
						tuple.setColumn(i, DbDouble.create(Double.parseDouble(attributes[i])));
					} else if (schema[i] == DataType.LONG) {
						tuple.setColumn(i, DbLong.create(Long.parseLong(attributes[i])));
					} else if (schema[i] == DataType.DATETIME) {
						tuple.setColumn(i, DbDateTime.create(attributes[i]));
					} else {
						// not ready to parse this kind of column yet
						return ReturnStatus.ERROR;
					}
				}
				// skip index lookup and add tuple directly
				tupleStore.add(tuple);
				if (deALSContext.isDebugEnabled()) {
					counter++;
					if ((counter % 10000) == 0)
						System.out.println(counter);
				}
			} while ((line = br.readLine()) != null);

			// sort before rebuilding indexes
			if (sortRelation)
				tupleStore.sort();
						
			// after parsing the file, and loading the relation, build the indexes
			if (relation.getSecondaryIndexes().length > 0) {
				counter = 0;
				relation.rebuildIndexes();
			}						
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null)
				try { br.close(); } catch (IOException e) {}
			if (fr != null)
				try { fr.close(); } catch (IOException e) {}
		}
		
		return ReturnStatus.SUCCESS;
	}

	// accepts csv or tab delimited text files (i.e. no facts or "all" usage)
	public static ReturnStatus load(DeALSContext deALSContext, Database database, String filePath, String predicateName, 
			int arity, boolean sortRelation) {
		FileReader fr = null;
		BufferedReader br = null;
		String line;
		
		BaseRelation relation = database.getRelationManager().getBaseRelation(predicateName);
		if (relation == null || relation.getArity() != arity)
			throw new RelationManagerException("relation [" + predicateName + "|" + arity + "] does not exist.");

		TupleStoreBase tupleStore = relation.getTupleStore();
		DataType[] schema = relation.getSchema();
		Tuple tuple = relation.getEmptyTuple();
		
		// read the second line to get the arity of the predicate by parsing
		try {
			fr = new FileReader(filePath);
			br = new BufferedReader(fr);			
			String[] attributes;
			int counter = 0;
			String trimmed;

			while ((line = br.readLine()) != null) {
				attributes = line.trim().split(",");
	
				for (int i = 0; i < schema.length; i++) {
					switch (schema[i]) { 
						case INT:
							tuple.setColumn(i, DbInteger.create((int)Double.parseDouble(attributes[i])));
							break;
						case DOUBLE:
							tuple.setColumn(i, DbDouble.create(Double.parseDouble(attributes[i])));
							break;
						case LONG:
							tuple.setColumn(i, DbLong.create(Long.parseLong(attributes[i])));
							break;
						case FLOAT:
							tuple.setColumn(i, DbFloat.create(Float.parseFloat(attributes[i])));
							break;
						case STRING:
							if (attributes[i].charAt(0) != '"') {
								tuple.setColumn(i, database.getTypeManager().createString(attributes[i]));
							} else {
								if (attributes[i].charAt(attributes[i].length() - 1) == '"')
									trimmed = attributes[i].substring(1, attributes[i].length() - 1);
								else
									trimmed = attributes[i].substring(1);
								tuple.setColumn(i, database.getTypeManager().createString(trimmed));
							}
							break;
						case DATETIME:
							tuple.setColumn(i, DbDateTime.create(attributes[i]));
							break;
						default:
							// not ready to parse this kind of column yet
							return ReturnStatus.ERROR;
					}
				}

				// skip index lookup and add tuple directly
				tupleStore.add(tuple);
				if (deALSContext.isDebugEnabled()) {
					counter++;
					if ((counter % 100000) == 0)
						System.out.println(counter);
				}
			}

			// sort before rebuilding indexes
			if (sortRelation)
				tupleStore.sort();
						
			// after parsing the file, and loading the relation, build the indexes
			if (relation.getSecondaryIndexes().length > 0) {
				counter = 0;
				relation.rebuildIndexes();
			}						
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null)
				try { br.close(); } catch (IOException e) {}
			if (fr != null)
				try { fr.close(); } catch (IOException e) {}
		}
		
		return ReturnStatus.SUCCESS;
	}
}
