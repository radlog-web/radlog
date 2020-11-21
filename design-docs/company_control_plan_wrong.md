## Plan of Company Control Query
Note: Thsi query plan will fail during execution.

<pre>
<br><br>== Datalog ObjectText ==<br><br>
database({ownedshares(X: integer, Y: integer, P: integer)}).
cshares(Y, Z, mmax<P>) <- ownedshares(Y, Z, P).
cshares(X, Z, mcount<(Y, P)>) <- bought(X, Y), cshares(Y, Z, P).
bought(X, Y) <- cshares(X, Y, P), X ~= Y, P > 50.

<br><br>== Datalog Query ==<br><br>
cshares(Y, Z, P).
INFO [main] ParseResultRewriter: Parsed Rule is rewritten from: 1) cshares(Y, Z, mmax<P>) <- ownedshares(Y, Z, P).
to: 1) cshares(Y, Z, mmax<P>) <- ownedshares(Y, Z, P).
INFO [main] ParseResultRewriter: Parsed Rule is rewritten from: 2) cshares(X, Z, mcount<(Y, P)>) <- bought(X, Y), cshares(Y, Z, P).
to: 2) cshares(X, Z, mcount<(Y, P)>) <- bought(X, Y), cshares(Y, Z, P).
INFO [main] ParseResultRewriter: Parsed Rule is rewritten from: 3) bought(X, Y) <- cshares(X, Y, P), X ~= Y, P > 50.
to: 3) bought(X, Y) <- cshares(X, Y, P), X ~= Y, P > 50.
ERROR [main] TypeInferrer: head:cshares(Y, Z, mmax<P>) - Type Assignments:[Y:integer, Z:integer, (mmax=>long)P:integer]
ERROR [main] TypeInferrer: head:cshares(X, Z, mcount<(Y, P)>) - Type Assignments:[X:integer, Z:integer, (mcount=>long)Y:integer, P:long]
INFO [main] ProgramGenerator: Compiled PCG Tree:
PCGOrNode(cshares_fff_fff(Y, Z, P), #Children: 1, predicateType: DERIVED, isRecursive: Yes, rewritingMethod: Naive)
 Clique 1
  Parents: 
   1) PCGOrNode(cshares_fff_fff(X, Y, P), #Children: 1, predicateType: DERIVED, isRecursive: Yes)
   2) PCGOrNode(bought_ff_ff(X, Y), #Children: 1, predicateType: DERIVED, isRecursive: Yes)
   3) PCGOrNode(cshares_fff_bff(Y, Z, P), #Children: 1, predicateType: DERIVED, isRecursive: Yes)
   4) PCGOrNode(fs_aggregate_cshares_2_ffff_ffff(X, Z, mcount<(Y, P)>, FSAggr_1), #Children: 1, predicateType: BUILT_IN, isRecursive: Yes)
   5) PCGOrNode(cshares_fff_fff(Y, Z, P), #Children: 1, predicateType: DERIVED, isRecursive: Yes, rewritingMethod: Naive)
  Clique Predicates: 
  1) CliquePredicate(bought_ff, binding: ff, #ExitRules: 0, #RecursiveRules: 1)
   Exit Rules: NONE
   Recursive Rules:
    PCGAndNode(bought_ff_ff(X, Y), rule#: 3, #Children: 3, predicateType: DERIVED)
     PCGOrNode(cshares_fff_fff(X, Y, P), #Children: 1, predicateType: DERIVED, isRecursive: Yes)
     PCGOrNode(X ~=_bb_bb Y, #Children: 0, predicateType: BUILT_IN, isRecursive: No)
     PCGOrNode(P >_bb_bb 50, #Children: 0, predicateType: BUILT_IN, isRecursive: No)
  2) CliquePredicate(fs_aggregate_cshares_2_ffff, binding: ffff, #ExitRules: 0, #RecursiveRules: 1)
   Exit Rules: NONE
   Recursive Rules:
    PCGAndNode(fs_aggregate_cshares_2_ffff_ffff(X, Z, (Y, P), nil), rule#: 2.1, #Children: 2, predicateType: DERIVED)
     PCGOrNode(bought_ff_ff(X, Y), #Children: 1, predicateType: DERIVED, isRecursive: Yes)
     PCGOrNode(cshares_fff_bff(Y, Z, P), #Children: 1, predicateType: DERIVED, isRecursive: Yes)
  3) CliquePredicate(cshares_fff, binding: fff, #ExitRules: 1, #RecursiveRules: 1)
   Exit Rules:
    PCGAndNode(cshares_fff_fff(Y, Z, FSAggr_1), rule#: 1.2, #Children: 1, predicateType: DERIVED)
     PCGOrNode(fs_aggregate_cshares_1_ffff_ffff(Y, Z, mmax<P>, FSAggr_1), #Children: 1, predicateType: BUILT_IN, isRecursive: No)
      PCGAndNode(fs_aggregate_cshares_1_ffff_ffff(Y, Z, P, nil), rule#: 1.1, #Children: 1, predicateType: DERIVED)
       PCGOrNode(ownedshares_fff_fff(Y, Z, P), #Children: 1, predicateType: BASE)
        ownedshares(X:integer, Y:integer, P:integer)
   Recursive Rules:
    PCGAndNode(cshares_fff_fff(X, Z, FSAggr_1), rule#: 2.2, #Children: 1, predicateType: DERIVED)
     PCGOrNode(fs_aggregate_cshares_2_ffff_ffff(X, Z, mcount<(Y, P)>, FSAggr_1), #Children: 1, predicateType: BUILT_IN, isRecursive: Yes)
 End Clique 1

 <br><br>== Operator Program ==<br><br>

0: cshares(Var_1, Var_2, Var_3) <MUTUAL_RECURSIVE_CLIQUE>(Recursion: NONLINEAR, Evaluation Type: MonotonicSemiNaive)
Exit Rules: 
 1: (X, Y, mmax(P) as FSAggr_1) <AGGREGATE_FS>
  2: ownedshares(X, Y, P) <BASE_RELATION>
Recursive Rules: 
 1: (Var_4, Z, mcount((Var_5, P)) as FSAggr_1) <AGGREGATE_FS>
  2: (Var_4, Z, Var_5, P) <PROJECT>
   3: (0.Var_5 = 1.Y) <JOIN>
    4: bought(Var_4, Var_5) <MUTUAL_RECURSIVE_CLIQUE>(Recursion: NONLINEAR, Evaluation Type: SemiNaive)
    Exit Rules: 
    Recursive Rules: 
     5: (X, Y) <PROJECT>
      6: (X ~= Y) && (P > 50) <FILTER>
       7: cshares(X, Y, P) <RECURSIVE_RELATION>
    4: cshares(Y, Z, P) <RECURSIVE_RELATION>
<br><br>== SparkPlan generated from Operator Program ==<br><br>
'SubqueryAlias cshares
+- 'Project [unresolvedalias('Var_4 AS Var_1#36, None), unresolvedalias('Z AS Var_2#37, None), unresolvedalias('FSAggr_1 AS Var_3#38, None)]
   +- 'AggregateRecursion cshares, [Driver, Mutual], [1, 0, 0]
      :- 'SubqueryAlias fs_aggregate_cshares_1
      :  +- 'MonotonicAggregate ['ownedshares.X, 'ownedshares.Y], [unresolvedalias('ownedshares.X AS X#16, None), unresolvedalias('ownedshares.Y AS Y#17, None), unresolvedalias('mmax('ownedshares.P) AS FSAggr_1#18, None)], [1, 0, 0]
      :     +- 'UnresolvedRelation `ownedshares`
      +- 'SubqueryAlias fs_aggregate_cshares_2
         +- 'MonotonicAggregate ['bought.Var_4, 'cshares2.Z], [unresolvedalias('bought.Var_4 AS Var_4#33, None), unresolvedalias('cshares2.Z AS Z#34, None), unresolvedalias('mcount('bought.Var_5, 'cshares2.P) AS FSAggr_1#35, None)], [1, 0, 0]
            +- 'Project ['bought.Var_4, 'cshares2.Z, 'bought.Var_5, 'cshares2.P]
               +- 'Join Inner, ('bought.Var_5 = 'cshares2.Y)
                  :- 'SubqueryAlias bought
                  :  +- 'Project [unresolvedalias('cshares1.X AS Var_4#28, None), unresolvedalias('cshares1.Y AS Var_5#29, None)]
                  :     +- 'Recursion bought, [Mutual], [1, 0]
                  :        +- 'Project ['cshares1.X, 'cshares1.Y]
                  :           +- 'Filter (NOT ('cshares1.X = 'cshares1.Y) && ('cshares1.P > 50))
                  :              +- SubqueryAlias cshares1
                  :                 +- AggregateRelation cshares, [X#25, Y#26, P#27], [1, 0, 0]
                  +- SubqueryAlias cshares2
                     +- NonLinearRecursiveRelation cshares, [Y#30, Z#31, P#32], [1, 0, 0]


<br><br>== Parsed Logical Plan ==<br><br>
'SubqueryAlias cshares
+- 'Project [unresolvedalias('Var_4 AS Var_1#36, None), unresolvedalias('Z AS Var_2#37, None), unresolvedalias('FSAggr_1 AS Var_3#38, None)]
   +- 'AggregateRecursion cshares, [Driver, Mutual], [1, 0, 0]
      :- 'SubqueryAlias fs_aggregate_cshares_1
      :  +- 'MonotonicAggregate ['ownedshares.X, 'ownedshares.Y], [unresolvedalias('ownedshares.X AS X#16, None), unresolvedalias('ownedshares.Y AS Y#17, None), unresolvedalias('mmax('ownedshares.P) AS FSAggr_1#18, None)], [1, 0, 0]
      :     +- 'UnresolvedRelation `ownedshares`
      +- 'SubqueryAlias fs_aggregate_cshares_2
         +- 'MonotonicAggregate ['bought.Var_4, 'cshares2.Z], [unresolvedalias('bought.Var_4 AS Var_4#33, None), unresolvedalias('cshares2.Z AS Z#34, None), unresolvedalias('mcount('bought.Var_5, 'cshares2.P) AS FSAggr_1#35, None)], [1, 0, 0]
            +- 'Project ['bought.Var_4, 'cshares2.Z, 'bought.Var_5, 'cshares2.P]
               +- 'Join Inner, ('bought.Var_5 = 'cshares2.Y)
                  :- 'SubqueryAlias bought
                  :  +- 'Project [unresolvedalias('cshares1.X AS Var_4#28, None), unresolvedalias('cshares1.Y AS Var_5#29, None)]
                  :     +- 'Recursion bought, [Mutual], [1, 0]
                  :        +- 'Project ['cshares1.X, 'cshares1.Y]
                  :           +- 'Filter (NOT ('cshares1.X = 'cshares1.Y) && ('cshares1.P > 50))
                  :              +- SubqueryAlias cshares1
                  :                 +- AggregateRelation cshares, [X#25, Y#26, P#27], [1, 0, 0]
                  +- SubqueryAlias cshares2
                     +- NonLinearRecursiveRelation cshares, [Y#30, Z#31, P#32], [1, 0, 0]

<br><br>== Analyzed Logical Plan ==<br><br>
Var_1: int, Var_2: int, Var_3: int
SubqueryAlias cshares
+- Project [Var_4#33 AS Var_1#36, Z#34 AS Var_2#37, FSAggr_1#35 AS Var_3#38]
   +- AggregateRecursion cshares, [Driver, Mutual], [1, 0, 0]
      :- SubqueryAlias fs_aggregate_cshares_1
      :  +- MonotonicAggregate [X#0, Y#1], [X#0 AS X#16, Y#1 AS Y#17, mmax(P#2) AS FSAggr_1#18], [1, 0, 0]
      :     +- SubqueryAlias ownedshares
      :        +- LogicalRDD [X#0, Y#1, P#2]
      +- SubqueryAlias fs_aggregate_cshares_2
         +- MonotonicAggregate [Var_4#28, Z#31], [Var_4#28 AS Var_4#33, Z#31 AS Z#34, mcount(Var_5#29, P#32) AS FSAggr_1#35], [1, 0, 0]
            +- Project [Var_4#28, Z#31, Var_5#29, P#32]
               +- Join Inner, (Var_5#29 = Y#30)
                  :- SubqueryAlias bought
                  :  +- Project [X#25 AS Var_4#28, Y#26 AS Var_5#29]
                  :     +- Recursion bought, [Mutual], [1, 0]
                  :        +- Project [X#25, Y#26]
                  :           +- Filter (NOT (X#25 = Y#26) && (cast(P#27 as bigint) > 50))
                  :              +- SubqueryAlias cshares1
                  :                 +- AggregateRelation cshares, [X#25, Y#26, P#27], [1, 0, 0]
                  +- SubqueryAlias cshares2
                     +- NonLinearRecursiveRelation cshares, [Y#30, Z#31, P#32], [1, 0, 0]

<br><br>== Optimized Logical Plan ==<br><br>
Project [Var_4#33 AS Var_1#36, Z#34 AS Var_2#37, FSAggr_1#35 AS Var_3#38]
+- AggregateRecursion cshares, [Driver, Mutual], [1, 0, 0]
   :- MonotonicAggregate [X#0, Y#1], [X#0 AS X#16, Y#1 AS Y#17, mmax(P#2) AS FSAggr_1#18], [1, 0, 0]
   :  +- LogicalRDD [X#0, Y#1, P#2]
   +- MonotonicAggregate [Var_4#28, Z#31], [Var_4#28 AS Var_4#33, Z#31 AS Z#34, mcount(Var_5#29, P#32) AS FSAggr_1#35], [1, 0, 0]
      +- Project [Var_4#28, Z#31, Var_5#29, P#32]
         +- Join Inner, (Var_5#29 = Y#30)
            :- Project [X#25 AS Var_4#28, Y#26 AS Var_5#29]
            :  +- Recursion bought, [Mutual], [1, 0]
            :     +- Project [X#25, Y#26]
            :        +- Filter (NOT (X#25 = Y#26) && (cast(P#27 as bigint) > 50))
            :           +- AggregateRelation cshares, [X#25, Y#26, P#27], [1, 0, 0]
            +- NonLinearRecursiveRelation cshares, [Y#30, Z#31, P#32], [1, 0, 0]

<br><br>== Physical Plan ==<br><br>
*Project [Var_4#33 AS Var_1#36, Z#34 AS Var_2#37, FSAggr_1#35 AS Var_3#38]
+- AggregateRecursion [Var_4#33,Z#34,FSAggr_1#35] (Driver,Mutual) [cshares][1,0,0]
   :- ExitAggrExchange(key=[X#0,Y#1], functions=[mmax(P#2)], output=[X#0,Y#1,mmax#43], iterType=Tungsten) hashpartitioning(X#0 AS X#16, 3)
   :  +- *HashAggregate(keys=[X#0, Y#1], functions=[partial_mmax(P#2)], output=[X#0, Y#1, mmax#43])
   :     +- Scan ExistingRDD[X#0,Y#1,P#2]
   +- RecAggrExchange(key=[Var_4#28,Z#31], functions=[mcount(Var_5#29, P#32)], output=[Var_4#33,Z#34,FSAggr_1#35], iterType=MCount) hashpartitioning(Var_4#28 AS Var_4#33, 3)
      +- *HashAggregate(keys=[Var_4#28, Z#31, Var_5#29], functions=[partial_mcount(Var_5#29, P#32)], output=[Var_4#28, Z#31, Var_5#29, mcount#46])
         +- *Project [Var_4#28, Z#31, Var_5#29, P#32]
            +- ShuffledHashJoin [Var_5#29], [Y#30], Inner, BuildRight
               :- Exchange hashpartitioning(Var_5#29, 3)
               :  +- *Project [X#25 AS Var_4#28, Y#26 AS Var_5#29]
               :     +- Recursion [X#25,Y#26] (Mutual) [bought][1,0]
               :        +- RecExchange bought, true, hashpartitioning(X#25, 3)
               :           +- *Project [X#25, Y#26]
               :              +- *Filter (NOT (X#25 = Y#26) && (cast(P#27 as bigint) > 50))
               :                 +- *AggregateRelation [X#25,Y#26,P#27](cshares)
               +- *NonLinearRecursiveRelation [Y#30,Z#31,P#32](cshares)
</pre>