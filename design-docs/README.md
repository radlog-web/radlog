Welcome to the design docs of the system.

This readme will guide you through the process of
how a Datalog program is compiled into a distributed execution plan
on Apache Spark.

## Steps to compile a Datalog Program
<pre>

Datalog Program (Relation Definition + Datalog Rules)
<br><br>

1) Relation Definition ---> CreateTempView
                        |
                 Input Data Path  
<br><br>

2) Datalog Rules ---> PCG Tree (And-Or-Graph) ---> Operator Program ---> Unresolved SparkPlan
                  |                            |                     |
             DeALs Analyzer             ProgramGenerator     LogicalPlanGenerator
<br>
   Unresolved SparkPlan ---> Analyzed Logical Plan ---> Optimized Logical Plan ---> Physical Plan
                         |                          |                           |
                  BigDatalogAnalyzer          SparkOptimizer*            BigDatalogPlanner
<br>
*Some rules in SparkOptimizer are disabled.
</pre>

## Example (Transitive Closure Query - TC)
<pre>
== Relation Definition == <br><br>
database({arc(A: integer, B: integer)}).

<br><br>== Datalog Rules ==<br><br>
tc(A,B) <- arc(A,B).
tc(A,B) <- tc(A,C), arc(C,B).
query tc(A, B).

<br><br>== PCG Tree ==<br><br>
PCGOrNode(tc_ff_ff(A, B), #Children: 1, predicateType: DERIVED, isRecursive: Yes)
 Clique 1
  Parents: 
   1) PCGOrNode(tc_ff_ff(A, C), #Children: 1, predicateType: DERIVED, isRecursive: Yes)
   2) PCGOrNode(tc_ff_ff(A, B), #Children: 1, predicateType: DERIVED, isRecursive: Yes)
  Clique Predicates: 
  1) CliquePredicate(tc_ff, binding: ff, #ExitRules: 1, #RecursiveRules: 1)
   Exit Rules:
    PCGAndNode(tc_ff_ff(A, B), rule#: 1, #Children: 1, predicateType: DERIVED)
     PCGOrNode(arc_ff_ff(A, B), #Children: 1, predicateType: BASE)
      arc(A:integer, B:integer)
   Recursive Rules:
    PCGAndNode(tc_ff_ff(A, B), rule#: 2, #Children: 2, predicateType: DERIVED)
     PCGOrNode(tc_ff_ff(A, C), #Children: 1, predicateType: DERIVED, isRecursive: Yes)
     PCGOrNode(arc_bf_bf(C, B), #Children: 1, predicateType: BASE)
      arc(A:integer, B:integer)
 End Clique 1

<br><br>== Operator Program ==<br><br>
0: tc(Var_1, Var_2) <RECURSIVE_CLIQUE>
Exit Rules: 
 1: arc(A, B) <BASE_RELATION>
Recursive Rules: 
 1: (A, B) <PROJECT>
  2: (0.C = 1.A) <JOIN>
   3: tc(A, C) <RECURSIVE_RELATION>
   3: arc(A, B) <BASE_RELATION>

<br><br>== Unresolved SparkPlan ==<br><br>
'SubqueryAlias tc
+- 'Project [unresolvedalias('tc1.A AS Var_1#14, None), unresolvedalias('arc2.B AS Var_2#15, None)]
   +- 'Recursion tc, [Driver], [1, 0]
      :- 'UnresolvedRelation `arc`
      +- 'Project ['tc1.A, 'arc2.B]
         +- 'Join Inner, ('tc1.C = 'arc2.A)
            :- SubqueryAlias tc1
            :  +- LinearRecursiveRelation tc, [A#12, C#13], [1, 0]
            +- 'BroadcastHint
               +- 'SubqueryAlias arc2
                  +- 'Project [*]
                     +- 'UnresolvedRelation `arc`

<br><br>== Optimized Logical Plan ==<br><br>
Project [A#12 AS Var_1#14, B#1 AS Var_2#15]
+- Recursion tc, [Driver], [1, 0]
   :- LogicalRDD [A#0, B#1]
   +- Project [A#12, B#1]
      +- Join Inner, (C#13 = A#0)
         :- LinearRecursiveRelation tc, [A#12, C#13], [1, 0]
         +- BroadcastHint
            +- Project [A#0, B#1]
               +- LogicalRDD [A#0, B#1]

<br><br>== Physical Plan ==<br><br>
Project [A#12 AS Var_1#14, B#1 AS Var_2#15]
+- Recursion [A#12,B#1] (Driver) [tc][1,0]
   :- ExitExchange tc, hashpartitioning(A#0, 3)
   :  +- Scan ExistingRDD[A#0,B#1]
   +- RecExchange tc, false, hashpartitioning(A#12, 3)
      +- Project [A#12, B#1]
         +- BroadcastHashJoin [C#13], [A#0], Inner, BuildRight
            :- LinearRecursiveRelation [A#12,C#13](tc)
            +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
               +- Project [A#0, B#1]
                  +- Scan ExistingRDD[A#0,B#1]
  
</pre>

## Runner classes

Runner classes provides simple wrappers over a BigDatalogSession 
to facilitate execution of a Datalog or RaSQL program.
1. Create a BigDatalogSession to connect to a Spark Cluster or start in local.
2. Compile the program into the execution plan.
3. Execute the plan.

Check `DatalogRunner` or `RaSQLRunner` for details.
