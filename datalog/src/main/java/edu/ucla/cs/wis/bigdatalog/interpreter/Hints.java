package edu.ucla.cs.wis.bigdatalog.interpreter;

import java.util.HashMap;
import java.util.Map;

public class Hints {
  public enum JoinHint {
    BroadcastHashJoin, ShuffleHashJoin, SortMergeJoin
  }

  public JoinHint joinHint;

  public Hints() {
  }

  public Hints(String[] hints) {
    for (String h: hints) {
      switch (h) {
        case "shuffle": joinHint = JoinHint.ShuffleHashJoin; break;
        case "broadcast": joinHint = JoinHint.BroadcastHashJoin; break;
        case "sortMerge": joinHint = JoinHint.SortMergeJoin; break;
        default: throw new UnsupportedOperationException("Hint: " + h + " not recognized");
      }
    }
  }

  public Map<String, String> toStringMap() {
    Map<String, String> map = new HashMap<>();
    map.put("joinType", joinHint.toString());
    return map;
  }
}
