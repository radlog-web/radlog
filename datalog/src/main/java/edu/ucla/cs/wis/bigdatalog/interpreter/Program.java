package edu.ucla.cs.wis.bigdatalog.interpreter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import edu.ucla.cs.wis.bigdatalog.api.RecursionInformation;
import edu.ucla.cs.wis.bigdatalog.common.Quad;
import edu.ucla.cs.wis.bigdatalog.common.Triple;
import edu.ucla.cs.wis.bigdatalog.compiler.ProgramRules;
import edu.ucla.cs.wis.bigdatalog.database.RelationManager;
import edu.ucla.cs.wis.bigdatalog.database.cursor.IndexCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.index.Index;
import edu.ucla.cs.wis.bigdatalog.database.index.key.KeyIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.SecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.relation.BaseRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.RecursiveRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.relation.RelationType;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public abstract class Program<T> {
	protected T root;
	protected ProgramRules programRules;
	
	public Program(){}

	public Program(T root, ProgramRules programRules) {
		this.root = root;
		this.programRules = programRules;
	}
	
	public T getRoot() { return this.root; }
	
	public void setRoot(T rootNode) { this.root = rootNode; }
	
	public ProgramRules getProgramRules() { return this.programRules; }

	public boolean isValid() {  return (this.root != null); }	
	
	public static void resetIndexCounters() {
		//IndexManager.resetIndexCounters();
		//CursorManager.resetCursorCounters();
	}

	public String getExecutionInfo(DeALSContext deALSContext) {
		int[] flags = new int[5];
		for (int i = 0; i < 5; i++)
			flags[i] = 0;

		if (deALSContext.getConfiguration().compareProperty("deals.interpreter.executioninfo.properties", "on"))
			flags[0] = 1;
		if (deALSContext.getConfiguration().compareProperty("deals.interpreter.executioninfo.nodes", "on"))
			flags[1] = 1;
		if (deALSContext.getConfiguration().compareProperty("deals.interpreter.executioninfo.relations", "on"))
			flags[2] = 1;
		if (deALSContext.getConfiguration().compareProperty("deals.interpreter.executioninfo.indexes", "on"))
			flags[3] = 1;
		if (deALSContext.getConfiguration().compareProperty("deals.interpreter.executioninfo.cursors", "on"))
			flags[4] = 1;

		StringBuilder retval = new StringBuilder();
		if (this.root != null) {
			if (flags[0] == 1) {
				retval.append("---------------PROPERTIES START----------------\n");
				retval.append(this.getProperties());
				retval.append("---------------PROPERTIES END----------------\n");
			}
			if (flags[1] == 1) {
				retval.append("---------------NODES START----------------\n");
				retval.append(this.getNodeInfo(this.root));
				retval.append("---------------NODES END----------------\n");
			}
			if (flags[2] == 1) {
				retval.append("---------------RELATIONS START----------------\n");
				retval.append(this.getRelationInfo(deALSContext));
				retval.append("---------------RELATIONS END----------------\n");
			}
			if (flags[3] == 1) {
				retval.append("---------------INDEXES START----------------\n");
				retval.append(this.getIndexInfo(deALSContext));
				retval.append("---------------INDEXES END----------------\n");
			}
			if (flags[4] == 1) {	
				retval.append("---------------CURSORS START----------------\n");
				retval.append(this.getCursorInfo(deALSContext));
				retval.append("---------------CURSORS END----------------\n");
			}
		}
		return retval.toString();
	}

	private String getProperties() {
		StringBuilder retval = new StringBuilder();

		Properties properties = System.getProperties();				

		// alphabetize - we know they're Strings, not Objects
		List<Object> propertyKeys = new ArrayList<>(properties.keySet());

		Collections.sort(propertyKeys, new Comparator<Object>() {
			public int compare(Object s1, Object s2) {
				return ((String)s1).compareTo((String)s2);
			}
		});

		for (Object propertyKey : propertyKeys)
			retval.append(propertyKey + " = " + properties.get(propertyKey) + "\n");

		return retval.toString();
	}

	private String getIndexInfo(DeALSContext deALSContext) {
		StringBuilder retval = new StringBuilder();

		HashMap<Relation<?>, List<Index<?>>> relationIndexes = deALSContext.getDatabase().getIndexManager().getIndexes(); 
		//double nanoseconddivisor = 1000000000d;
		double nanoseconddivisor = 1000000d;
		List<Quad<String, String, Long, String>> indexToTime = new ArrayList<>();
		List<Index<?>> uselessIndexes = new ArrayList<>();

		List<Relation<?>> relations = new ArrayList<>(relationIndexes.keySet());

		Collections.sort(relations, new Comparator<Relation<?>>() {
			public int compare(Relation<?> r1, Relation<?> r2) {
				return r1.getName().compareTo(r2.getName());
			}
		});

		List<String> embeddedFunctions = new ArrayList<>();
		embeddedFunctions.add("split");
		embeddedFunctions.add("getKey");
		embeddedFunctions.add("hash");

		for (Relation<?> relation : relations) {
			List<Index<?>> indexes = relationIndexes.get(relation);
			retval.append("\nIndexes for relation '" + relation.getName() + "'");
			if (indexes.size() == 0) {
				retval.append("No Indexes\n");
			} else {			
				retval.append("\nSearch Indexes:");
				int count = 1;
				for (Index<?> index : indexes) {
					if (index instanceof SecondaryIndex) {
						SecondaryIndex<?> searchIndex = (SecondaryIndex<?>)index;

						retval.append("\n  " + count++ + ") " + searchIndex.getClass().getSimpleName() + " on columns " + Arrays.toString(searchIndex.getIndexedColumns()));
						retval.append(" | # of tuples in index: " + searchIndex.getNumberOfEntries());
						//String[] functions = index.getFunctions();
						/*long totalTime = 0;
						for (int i = 0; i < searchIndex.getTimesCalled().length; i++)
							if (!embeddedFunctions.contains(functions[i]))
								totalTime += searchIndex.getTimeSpent()[i];

						indexToTime.add(new Quad<>(relation.getName(), Arrays.toString(searchIndex.getIndexedColumns()), totalTime, "Search"));
						retval.append(" | total time: " + totalTime / nanoseconddivisor + "\n");

						double percentageOfTime = 0.0;
						for (int i = 0; i < searchIndex.getTimesCalled().length; i++) {
							percentageOfTime = Math.round(((double)searchIndex.getTimeSpent()[i] / ((double)totalTime)) * 100.0);
							retval.append("     " + functions[i]  + "[#: " + searchIndex.getTimesCalled()[i] + " | " + searchIndex.getTimeSpent()[i] / nanoseconddivisor + "[" + percentageOfTime + "]% | NO results: " + searchIndex.getTimesNoResults()[i] + "]\n");

							if ((percentageOfTime == 1.0 || totalTime == 0.0) && searchIndex.getRelation().getRelationType() != RelationType.BASE)
								if (!uselessIndexes.contains(searchIndex))
									uselessIndexes.add(searchIndex);
						}*/
					}
				}
				if (count == 1)
					retval.append("NONE\n");
				count = 1;
				retval.append("\nKey Indexes:");
				for (Index<?> index : indexes) {
					if (index instanceof KeyIndex) {
						KeyIndex<?> keyIndex = (KeyIndex<?>)index;

						retval.append("\n  " + count++ + ") " + keyIndex.getClass().getSimpleName() + " on columns " + Arrays.toString(keyIndex.getKeyColumns()));
						retval.append(" | # of tuples in index: " + keyIndex.getNumberOfEntries());
						//String[] functions = index.getFunctions();
						/*long totalTime = 0;
						for (int i = 0; i < keyIndex.getTimesCalled().length; i++)
							if (!embeddedFunctions.contains(functions[i]))
								totalTime += keyIndex.getTimeSpent()[i];

						indexToTime.add(new Quad<>(relation.getName(), Arrays.toString(keyIndex.getKeyColumns()), totalTime, "Key"));
						retval.append(" | total time: " + totalTime / nanoseconddivisor + "\n");

						double percentageOfTime = 0.0;
						for (int i = 0; i < keyIndex.getTimesCalled().length; i++) {
							percentageOfTime = Math.round(((double)keyIndex.getTimeSpent()[i] / ((double)totalTime)) * 100.0);
							retval.append("     " + functions[i]  + "[# " + keyIndex.getTimesCalled()[i] + " | " + keyIndex.getTimeSpent()[i] / nanoseconddivisor + "[" + percentageOfTime + "]% | NO results: " + keyIndex.getTimesNoResults()[i] + "]\n");

							if ((percentageOfTime == 1.0 || totalTime == 0.0) && keyIndex.getRelation().getRelationType() != RelationType.BASE)
								if (!uselessIndexes.contains(keyIndex))
									uselessIndexes.add(keyIndex);
						}*/
					}
				}
				if (count == 1)
					retval.append("NONE");
				retval.append("\n");
			}
		}

		long totalSearchIndexTypeTiming = 0;
		retval.append("Search Index Timing:\n");
		for (Quad<String, String, Long, String> info : indexToTime) {
			if (info.getFourth().equals("Search")) {
				retval.append("  " + info.getThird() / nanoseconddivisor + "ms " + info.getFirst() + ": " + info.getSecond() + "\n");
				totalSearchIndexTypeTiming += info.getThird();
			}
		}
		retval.append("Total Search Index Timing: " + totalSearchIndexTypeTiming / nanoseconddivisor + " ms\n");
		retval.append("\nKey Index Timing:\n");
		long totalKeyIndexTypeTiming = 0;			
		for (Quad<String, String, Long, String> info : indexToTime) {
			if (info.getFourth().equals("Key")) {
				retval.append("  " + info.getThird() / nanoseconddivisor + "ms " + info.getFirst() + ": " + info.getSecond() + "\n");
				totalKeyIndexTypeTiming += info.getThird();
			}
		}
		retval.append("Total Key Index Timing: " + totalKeyIndexTypeTiming / nanoseconddivisor + " ms\n");
		retval.append("\nTotal Index Timing: " + (totalSearchIndexTypeTiming + totalKeyIndexTypeTiming) / nanoseconddivisor + " ms\n");
		if (uselessIndexes.size() > 0) {
			retval.append("Useless Indexes (they only do 1 operation): \n");
			for (Index<?> uselessIndex : uselessIndexes) {
				retval.append("    " + uselessIndex.getClass().getSimpleName() + " on ");
				retval.append(uselessIndex.getRelation().getName() + " on ");
				if (uselessIndex instanceof KeyIndex)
					retval.append(Arrays.toString(((KeyIndex<?>)uselessIndex).getKeyColumns()) + "\n");
				else
					retval.append(Arrays.toString(((SecondaryIndex<?>)uselessIndex).getIndexedColumns()) + "\n");
			}
		}

		return retval.toString();
	}

	private String getCursorInfo(DeALSContext deALSContext) {
		StringBuilder retval = new StringBuilder();

		HashMap<Relation<?>, List<Cursor<?>>> relationCursors = deALSContext.getDatabase().getCursorManager().getCursors(); 
		//double nanoseconddivisor = 1000000000d;
		double nanoseconddivisor = 1000000d;
		List<Triple<String, String, Long>> cursorToTime = new ArrayList<>();
		List<Cursor<?>> uselessCursors = new ArrayList<>();

		List<Relation<?>> relations = new ArrayList<>(relationCursors.keySet());

		Collections.sort(relations, new Comparator<Relation<?>>() {
			public int compare(Relation<?> r1, Relation<?> r2) {
				return r1.getName().compareTo(r2.getName());
			}
		});

		for (Relation<?> relation : relations) {
			List<Cursor<?>> cursors = relationCursors.get(relation);
			retval.append("Cursors for relation '" + relation.getName() + "'");
			if (cursors.size() == 0) {
				retval.append("No Cursors\n");
			} else {
				int count = 1;		
				for (Cursor<?> cursor : cursors) {				
					retval.append("\n  " + count++ + ") " + cursor.getClass().getSimpleName());
					if (cursor instanceof IndexCursor) {
						IndexCursor ic = (IndexCursor)cursor;
						retval.append(" on columns : " +Arrays.toString(ic.getIndex().getIndexedColumns()) + " <index id:" + ic.getIndex().hashCode() + ">");
					}
					//String[] functions = cursor.getFunctions();
					/*long totalTime = 0;
					for (int i = 0; i < cursor.getTimesCalled().length; i++) {
						// don't count split, since this is included in the get
						if (functions[i].equals("split"))
							continue;

						// for index cursors, don't count match - its called inside getTuple()
						if (functions[i].equals("match") 
								&& ((cursor instanceof IndexCursor) || (cursor instanceof CachedIndexCursor)))
							continue;

						totalTime += cursor.getTimeSpent()[i];
					}

					cursorToTime.add(new Triple<>(relation.getName(), cursor.getClass().getSimpleName(), totalTime));
					retval.append(" [total time: " + totalTime / nanoseconddivisor + "]\n");
					if (totalTime > 0) {
						double percentageOfTime = 0.0;
						for (int i = 0; i < cursor.getTimesCalled().length; i++) {
							percentageOfTime = Math.round(((double)cursor.getTimeSpent()[i] / ((double)totalTime)) * 100.0);
							retval.append("     " + functions[i]  + "[#: " + cursor.getTimesCalled()[i] + " | " + cursor.getTimeSpent()[i] / nanoseconddivisor + "[" + percentageOfTime + "]% | NO results: " + cursor.getTimesNoResults()[i] + "]\n");

						//	if ((percentageOfTime == 1.0 || totalTime == 0.0) && cursor.getRelation().getRelationType() != RelationType.BASE)
							//	if (!uselessCursors.contains(cursor))
								//	uselessCursors.add(cursor);
						}
					}*/
				}				
				retval.append("\n");
			}
		}

		long totalCursorTiming = 0;
		retval.append("Cursor Timing:\n");
		for (Triple<String, String, Long> info : cursorToTime) {
			retval.append("  " + info.getThird() / nanoseconddivisor + "ms " + info.getFirst() + ": " + info.getSecond() + "\n");
			totalCursorTiming += info.getThird();
		}
		retval.append("Total Cursor Timing: " + totalCursorTiming / nanoseconddivisor + " ms\n");

		if (uselessCursors.size() > 0) {
			retval.append("Useless Indexes (they only do 1 operation): \n");
			for (Cursor<?> uselessCursor : uselessCursors) {
				retval.append("    " + uselessCursor.getClass().getSimpleName() + " on ");
				retval.append(uselessCursor.getRelation().getName() + " on ");
			}
		}

		return retval.toString();
	}

	private String getRelationInfo(DeALSContext deALSContext) {
		StringBuilder retval = new StringBuilder();
		//double nanoseconddivisor = 1000000d;

		RelationManager relationManager = deALSContext.getDatabase().getRelationManager();

		//List<Triple<String, Long, String>> relationToTime = new ArrayList<>();

		List<BaseRelation<?>> usedBaseRelations = new ArrayList<>();
		for (BaseRelation<?> baseRelation : relationManager.getBaseRelations()) {
			if (this.isUsedInProgram(baseRelation)) {
				//retval.append(baseRelation.toStringDetails());
				usedBaseRelations.add(baseRelation);				
			}
		}

		int count = 1;
		for (BaseRelation<?> relation : usedBaseRelations) {
			retval.append("\nRelation '" + relation.getName() + "'");
			retval.append("\n  " + count++ + ") " + relation.getClass().getSimpleName() + " | schema: " + Arrays.toString(relation.getSchema()));
			retval.append(" | # of tuples in relation: " + relation.getTupleStore().getNumberOfTuples());
			retval.append("\n" + relation.toStringDetails());
			//String[] functions = relation.getFunctions();
			/*
			long totalTime = 0;
			for (int i = 0; i < relation.getTimesCalled().length; i++)
					totalTime += relation.getTimeSpent()[i];

			relationToTime.add(new Triple<>(relation.getName(), totalTime, "base relation"));
			retval.append(" | total time: " + totalTime / nanoseconddivisor + "\n");

			double percentageOfTime = 0.0;
			for (int i = 0; i < relation.getTimesCalled().length; i++) {
				percentageOfTime = Math.round(((double)relation.getTimeSpent()[i] / ((double)totalTime)) * 100.0);
				retval.append("     " + functions[i]  + "[#: " + relation.getTimesCalled()[i] + " | " + relation.getTimeSpent()[i] / nanoseconddivisor + "[" + percentageOfTime + "]% | NO results: " + relation.getTimesNoResults()[i] + "]\n");
			}*/
		}		
		count = 1;
		for (DerivedRelation relation : relationManager.getDerivedRelations()) {
			if (relation.getRelationType() != RelationType.RECURSIVE) {
				retval.append("\nRelation '" + relation.getName() + "'");				
				retval.append("\n  " + count++ + ") " + relation.getClass().getSimpleName() + " | schema: " + Arrays.toString(relation.getSchema()));
				retval.append(" | # of tuples in relation: " + relation.getTupleStore().getNumberOfTuples());
				retval.append("\n" + relation.toStringDetails());
				//String[] functions = relation.getFunctions();
				/*
				long totalTime = 0;
				for (int i = 0; i < relation.getTimesCalled().length; i++)
					totalTime += relation.getTimeSpent()[i];

				relationToTime.add(new Triple<>(relation.getName(), totalTime, "base relation"));
				retval.append(" | total time: " + totalTime / nanoseconddivisor + "\n");
				double percentageOfTime = 0.0;
				for (int i = 0; i < relation.getTimesCalled().length; i++) {
					percentageOfTime = Math.round(((double)relation.getTimeSpent()[i] / ((double)totalTime)) * 100.0);
					retval.append("     " + functions[i]  + "[#: " + relation.getTimesCalled()[i] + " | " + relation.getTimeSpent()[i] / nanoseconddivisor + "[" + percentageOfTime + "]% | NO results: " + relation.getTimesNoResults()[i] + "]\n");
				}*/
			}
		}
		count = 1;
		for (RecursiveRelation relation : relationManager.getRecursiveRelations()) {
			retval.append("\nRelation '" + relation.getName() + "'");			
			retval.append("\n  " + count++ + ") " + relation.getClass().getSimpleName() + " | schema: " + Arrays.toString(relation.getSchema()));
			retval.append(" | # of tuples in relation: " + relation.getTupleStore().getNumberOfTuples());
			retval.append("\n" + relation.toStringDetails());
			//String[] functions = relation.getFunctions();
			/*	
			long totalTime = 0;
			for (int i = 0; i < relation.getTimesCalled().length; i++)
				totalTime += relation.getTimeSpent()[i];

			relationToTime.add(new Triple<>(relation.getName(), totalTime, "base relation"));
			retval.append(" | total time: " + totalTime / nanoseconddivisor + "\n");
			double percentageOfTime = 0.0;
			for (int i = 0; i < relation.getTimesCalled().length; i++) {
				percentageOfTime = Math.round(((double)relation.getTimeSpent()[i] / ((double)totalTime)) * 100.0);
				retval.append("     " + functions[i]  + "[#: " + relation.getTimesCalled()[i] + " | " + relation.getTimeSpent()[i] / nanoseconddivisor + "[" + percentageOfTime + "]% | NO results: " + relation.getTimesNoResults()[i] + "]\n");
			}*/
		}

		return retval.toString();
	}

	abstract protected String getNodeInfo(T node);

	@SuppressWarnings("unchecked")
	protected String getNodeInfo2(Node<?> node) {
		StringBuilder retval = new StringBuilder();
		for (int i = 0; i < node.getNumberOfChildren(); i++)
			retval.append(getNodeInfo((T) node.getChild(i)));

		return retval.toString();
	}

	protected boolean isUsedInProgram(BaseRelation<?> baseRelation) {
		return this.isUsedInProgram(baseRelation, this.root);
	}

	abstract protected boolean isUsedInProgram(BaseRelation<?> baseRelation, T node);

	@SuppressWarnings("unchecked")
	protected boolean isUsedInProgram2(BaseRelation<?> baseRelation, Node<?> andNode) {
		for (int i = 0; i < andNode.getNumberOfChildren(); i++) {
			if (isUsedInProgram(baseRelation, (T) andNode.getChild(i)))
				return true;
		}
		return false;
	}
	
	abstract public Status execute(Relation<?> resultRelation);

	abstract public Status execute();

	abstract public Argument getArgument(int position);

	abstract public boolean initialize();

	abstract public void cleanUp();

	abstract List<RecursionInformation> getRecursionInformationByClique();
	
	abstract public void compressBoundArguments();
}
