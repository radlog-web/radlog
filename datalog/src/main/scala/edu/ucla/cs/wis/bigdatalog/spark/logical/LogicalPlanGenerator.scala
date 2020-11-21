/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// scalastyle: off
package edu.ucla.cs.wis.bigdatalog.spark.logical

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, ListBuffer, Stack}
import edu.ucla.cs.wis.bigdatalog.database.`type`.DbTypeBase
import edu.ucla.cs.wis.bigdatalog.interpreter.{EvaluationType, Hints, OperatorProgram}
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.{Argument, InterpreterFunctor, Variable}
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument._
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator._
import edu.ucla.cs.wis.bigdatalog.partitioning.generalizedpivoting.GeneralizedPivotingSolver
import edu.ucla.cs.wis.bigdatalog.spark.logical.RecOption.RecOption
import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.{StructField, StructType}



/**
 * This class converts a plan from DeAL (Deductive Application Language) logical operators in a top-down manner
 * into a Catalyst logical plan of relational and recursive (BigDatalog) operators.
 *
 * To use another front-end Compiler X, start by replacing the use of DeAL logical operators here with their equivalent operators produced by Compiler X.
 */
class LogicalPlanGenerator(operatorProgram: OperatorProgram, sparkSession: SparkSession) extends Logging {
  var subQueryCounter = new AtomicInteger(1)
  val renamed = HashMap.empty[Operator, String]
  val COUNT_DISTINCT = "countd" // DeAL naming for count distinct
  var cliqueOperatorStack = new Stack[CliqueOperator]
  val recursiveRelationNames = new HashSet[String]
  val conf = sparkSession.sparkContext.getConf
  val sqlConf = sparkSession.sessionState.conf

  val gpsi: GeneralizedPivotSetInfo = {
    if (!operatorProgram.getArguments.hasConstant) {
      new GeneralizedPivotSetInfo(GeneralizedPivotingSolver.getGeneralizedPivotSet(operatorProgram.getProgramRules), operatorProgram)
    }
    else null
  }

  val recursivePlanDetails: RecursivePlanDetails = new RecursivePlanDetails(operatorProgram.getRoot)

  // aliasMap is to track if a variable is gaining new aliases when going from bottom to up of the query plan
  // given the assumption that the reference to the same variable kept the same in any operator
  // thus the variable reference itself can be used as the key
  private val aliasMap = new mutable.HashMap[Variable, Alias]

  private def unresolvedAlias(alias: Alias): org.apache.spark.sql.catalyst.analysis.UnresolvedAlias = {
    org.apache.spark.sql.catalyst.analysis.UnresolvedAlias(alias)
  }

  private def unresolvedAliasVariable(alias: Alias, variable: Variable): org.apache.spark.sql.catalyst.analysis.UnresolvedAlias = {
    val res = org.apache.spark.sql.catalyst.analysis.UnresolvedAlias(alias)
    aliasMap.put(variable, alias)
    res
  }

  private def buildSchemaFromCliqueOperatorArgumentsList(cliqueOp: CliqueOperator): StructType = {
    // victor: we retrieve the name from arg name from CliqueOperator,
    // but correct data types from RecursiveRulesOperator
    // mutual recursion may not have ExitRulesOperator
    val recOp = cliqueOp.getRecursiveRulesOperator
    val cliqueOpArgs = cliqueOp.getArguments

    val structFields = recOp.getArguments.zip(cliqueOpArgs) map {
      case (arg, argument) =>
        // assume all cliqueOp arg is Variable
        val name = argument.asInstanceOf[Variable].getName
        arg match {
          case v: Variable =>
            val outputType = TypeUtils.getSparkDataType(v)
            // val name = v.toStringVariableName
            StructField(name, outputType, nullable = false)
          case aa: AliasedArgument =>
            // val name = aa.getAlias.asInstanceOf[Variable].getName
            aa.getArgument match {
              case aggrArgu: AggregateArgument =>
                aggrArgu.getTerm match {
                  case functor: InterpreterFunctor =>
                    // [Hack] if more than one arg of the AggrFunction, determine the outputType by function name
                    val outputType = aggrArgu.getName.toLowerCase match {
                      case "msum" => org.apache.spark.sql.types.DoubleType
                      case "mcount" => org.apache.spark.sql.types.IntegerType
                      case other => throw new UnsupportedOperationException(s"AggrFunction: $other")
                    }
                    StructField(name, outputType, nullable = false)
                  case v: Variable =>
                    // [Hack] use the arg type as outputType
                    val outputType = TypeUtils.getSparkDataType(v)
                    StructField(name, outputType, nullable = false)
                  case other =>
                    throw new UnsupportedOperationException(s"$other in $aggrArgu")
                }
              case v: Variable =>
                val outputType = TypeUtils.getSparkDataType(v)
                StructField(name, outputType, nullable = false)
              case b: DbTypeBase =>
                val outputType = TypeUtils.getSparkDataType(b.getDataType)
                StructField(name, outputType, nullable = false)
              case e: edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.Expression =>
                val outputType = TypeUtils.getSparkDataType(e.getDataType)
                StructField(name, outputType, nullable = false)
              case other =>
                throw new UnsupportedOperationException(s"$other in $aa")
            }
        }
    }

    StructType(structFields)
  }

  def getPlan(operator: Operator): LogicalPlan = {
    operator.getOperatorType match {
        // in DeAL a recursion node sits above a recursive clique node
      case OperatorType.RECURSION =>
        throw new UnsupportedOperationException()
        // val rootOp = operator.getChild(0)
        // SubqueryAlias(operator.getName, getPlan(rootOp, new RecursivePlanDetails(rootOp)))
      case OperatorType.RECURSIVE_CLIQUE | OperatorType.MUTUAL_RECURSIVE_CLIQUE =>
        val cliqueOperator = operator.asInstanceOf[CliqueOperator]

        cliqueOperatorStack.push(cliqueOperator)

        // we get the schema and register the table in the catalog so recursive relations planned after this node can the same table
        logInfo("Try to get schema for cliqueOperator: " + cliqueOperator.getName)
        val schema = buildSchemaFromCliqueOperatorArgumentsList(cliqueOperator)

        sparkSession.internalCreateDataFrame(sparkSession.sparkContext.emptyRDD[InternalRow], schema)
          .createOrReplaceTempView(cliqueOperator.getName)

        logInfo("cliqueOperator Registered Temp Table: " + cliqueOperator.getName + " schema: " + schema)

        // TODO: disable gpsi in determining the partitioning as it leads to incorrect results in delivery-stratified query
        // var partitioning = if (gpsi == null) Nil else gpsi.getGPSForRecursion(cliqueOperator.getName, cliqueOperator.getArity)
        // if (partitioning == Nil)
        val partitioning = getDefaultPartitioning(cliqueOperator.getArity)

        val recOptions = ArrayBuffer[RecOption]()
        // potential bug: only recognizes top-level mutual recursion as Driver
        if (operator.getOperatorType == OperatorType.RECURSIVE_CLIQUE || cliqueOperatorStack.length == 1) {
          recOptions += RecOption.Driver
        }
        if (operator.getOperatorType == OperatorType.MUTUAL_RECURSIVE_CLIQUE) {
          recOptions += RecOption.Mutual
        }

        val exitRulesOperator = cliqueOperator.getExitRulesOperator
        logInfo("cliqueOperator ExitRulesOperator: " + exitRulesOperator)

        // TODO: we currently only allow Recursion Driver to have Exit Rule Plan
        // check [[RecursionBase.doExecute()]] for the execution logic
        if (recOptions.contains(RecOption.Driver)) {
          if (exitRulesOperator == null) {
            throw new IllegalArgumentException("Recursion Driver lacks Exit Rule Plan.")
          }
        } else {
          if (exitRulesOperator != null) {
            throw new IllegalArgumentException("Non-Driver Recursion contains Exit Rule Plan.")
          }
        }

        // we need to register the relation when we prepare the physical operator
        var exitRulesPlan: LogicalPlan = operator.getOperatorType match {
          case OperatorType.RECURSIVE_CLIQUE =>
            getPlan(exitRulesOperator)
          case OperatorType.MUTUAL_RECURSIVE_CLIQUE =>
            if (exitRulesOperator == null) {
              null
            } else {
              getPlan(exitRulesOperator)
            }
          case other => throw new UnsupportedOperationException("Got " + other)
        }

        val recursiveRulesPlan = getPlan(cliqueOperator.getRecursiveRulesOperator)

        // generate AggregateRecursion, MutualRecursion or Recursion operator based on EvaluationType
        val recursion = cliqueOperator.getEvaluationType match {
          case EvaluationType.MonotonicSemiNaive =>
            AggregateRecursion(cliqueOperator.getName, recOptions, exitRulesPlan, recursiveRulesPlan, partitioning)
          case EvaluationType.SemiNaive =>
            Recursion(cliqueOperator.getName, recOptions, exitRulesPlan, recursiveRulesPlan, partitioning)
          case other => throw new UnsupportedOperationException("Got " + other)
        }

        cliqueOperatorStack.pop()

        // if the arguments of the recursion are aliased, we need to create a projection operator
        // if (operator.getArguments.exists(_.isInstanceOf[AliasedArgument]))
        val op: LogicalPlan = Project(getCliqueOutput(cliqueOperator, recursion.right), recursion)

        SubqueryAlias(operator.getName, op)
      case OperatorType.UNION =>
        val childPlans = operator.getChildren().map(child => getPlan(child)).toBuffer

        // if the UNION operator has a constant, we need to push it into the projection below it
        if (operator.getArguments().hasConstant) {
          var count = -1
          val constants = operator.getArguments().filter(p => p.isConstant)
            .map(constant => constant.asInstanceOf[DbTypeBase]).map(dbType => {
            count += 1
            unresolvedAlias(Alias(Literal.create(TypeUtils.getDbTypeBaseValue(dbType), TypeUtils.getSparkDataType(dbType.getDataType)), s"c_$count")())
          })

          for (i <- 0 until childPlans.size) {
            childPlans.get(i) match {
              case p: Project if (operator.getArity != p.projectList.size) => {
                childPlans.set(i, Project(p.projectList ++ constants, p.child))
              }
              case _ =>
            }
          }
        }

        var plan = childPlans.get(0)
        for (i <- 1 until childPlans.size) {
          plan = Union(plan, childPlans.get(i))

          val operatorName = operator.getName()
          if (recursivePlanDetails.containsBaseRelation(operator.getName)) {
            val name = operator.getName + this.subQueryCounter.getAndIncrement()
            plan = SubqueryAlias(name, plan)
            operator.setName(name)
            renamed.put(operator, operator.getName())
          } else {
            plan = SubqueryAlias(operator.getName(), plan)
          }
          recursivePlanDetails.addBaseRelation(operatorName, plan)
        }

        // Youfu Li added this line to fix the bug that attributes in union operator can not be resolved.
        SparkEnv.add2UnionSet(operator.getName)

        if (conf.getBoolean("spark.datalog.uniondistinct.enabled", true)) {
            Distinct(plan)
        } else {
            plan
        }
      case OperatorType.JOIN =>
        // set 'preferSortMergeJoin' to enable SortMergeJoin
        val joinType = operatorProgram.getHints.joinHint
        joinType match {
          case Hints.JoinHint.SortMergeJoin => sqlConf.setConfString("spark.sql.join.preferSortMergeJoin", "true")
          case _ => sqlConf.setConfString("spark.sql.join.preferSortMergeJoin", "false")
        }
        val childPlans = operator.getChildren().map(child => {
          (getPlan(child), new UnresolvedAttributeQualifier(child, renamed, aliasMap))
        }).toList
        JoinPlanGenerator.generate(operator.asInstanceOf[JoinOperator], childPlans, joinType, recursivePlanDetails)
      case OperatorType.PROJECT =>
        val subPlan = getPlan(operator.getChild(0))
        val output = getProjectOutput(operator)

        val projectPlan: LogicalPlan = Project(output, subPlan)
        // if (operator.asInstanceOf[ProjectionOperator].isDistinct && cliqueOperatorStack.isEmpty)
        //  projectPlan = Distinct(projectPlan)

        projectPlan
      case OperatorType.AGGREGATE | OperatorType.AGGREGATE_FS =>
        val subPlan = this.getPlan(operator.getChild(0))
        // generate a projection from operator and replace aggregate variable with aggregate
        // rules can have variables twice because of bad program writers
        val aggregateExpressions = ListBuffer.empty[NamedExpression]
        val groupByArguments = ListBuffer.empty[NamedExpression]
        val groupByExpressions = ListBuffer.empty[NamedExpression]
        var idx: Int = 0
        var arg: Argument = null
        var aggregate: Expression = null
        var aliasName: String = null
        var isMavg: Boolean = false

        for (aliasedArg <- operator.getArguments) {
          aliasedArg match {
            case aa: AliasedArgument =>
              aliasName = aa.getAlias.asInstanceOf[Variable].toStringVariableName
              arg = aa.getArgument
            case v: Variable =>
              aliasName = v.toStringVariableName
              arg = v
            case other =>
              throw new UnsupportedOperationException(s"Get $other (Type: ${other.getClass.getSimpleName}) in aggregate arguments")
          }

          arg match {
            case aa: AggregateArgument =>
              def getAAExpression(t: Argument): Seq[Expression] = {
                t match {
                  case v: Variable => Seq(new UnresolvedAttributeQualifier(operator.getChild(0), renamed, aliasMap).toUnresolvedAttribute(v))
                  case d: DbTypeBase => Seq(Literal.create(TypeUtils.getDbTypeBaseValue(d), TypeUtils.getSparkDataType(d.getDataType)))
                  case f: InterpreterFunctor => f.getArguments.innerArguments.flatMap(getAAExpression(_))
                  case other => null
                }
              }
              if(aa.getName == "mmin" && isMavg) {
                aa.setName("mavg")
              }
              val exprs: Seq[Expression] = getAAExpression(aa.getTerm)
              val aggregateName = if (aa.getName.equals(COUNT_DISTINCT)) "count" else aa.getName
              if (aggregateName == "mcount" | aggregateName == "msum") {
                if (exprs.size != 2) throw new SparkException(s"msum/mcount aggregate expects 2 arguments but received ${exprs.size}.")
                // in the exit rule, we convert msum to mmax and pull out the sub-grouping attributes
                val argumentToAggregate = exprs(1)
                val subGroupingKey = exprs(0).asInstanceOf[NamedExpression]
                aggregate = UnresolvedFunction(aggregateName, Seq(subGroupingKey, argumentToAggregate), false)

                // TODO: victor - do not know why need commented code
                // if (!isRecursive(operator)) {
                // val argumentToAggregate = exprs(1)
                // val subGroupingKey = exprs(0).asInstanceOf[NamedExpression]
                // groupByArguments += subGroupingKey
                // UnresolvedFunction("mmax", Seq(argumentToAggregate), false)
              } else if (aggregateName == "cmin" | aggregateName == "cmax") {
                val min_max_aliasedArguement = operator.getArgument(operator.getArguments.size() -1).asInstanceOf[AliasedArgument]
                val min_max_Argument = min_max_aliasedArguement.getArgument.asInstanceOf[AggregateArgument]
                val min_max_expr: Seq[Expression] = getAAExpression(min_max_Argument.getTerm)
                // in the exit rule, we convert msum to mmax and pull out the sub-grouping attributes
                val argumentToAggregate = min_max_expr(0)
                val subGroupingKey = exprs(0).asInstanceOf[NamedExpression]
                aggregate = UnresolvedFunction(aggregateName, Seq(subGroupingKey, argumentToAggregate), false)
              } else {
                aggregate = UnresolvedFunction(aggregateName, exprs, aa.getName.equals(COUNT_DISTINCT))
              }
              aggregateExpressions += unresolvedAlias(Alias(aggregate, aliasName)(qualifier = Some(operator.getName)))
            case v: Variable => if (!v.isAnonymous) {
              val uv = new UnresolvedAttributeQualifier(operator.getChild(0), renamed, aliasMap).toUnresolvedAttribute(v)
              groupByArguments += uv
              // victor: we need to add alias to groupByArguments
              // Comments from Youfu Li: not sure, it is not adding anything, changed it to just add "uv"
              if (aliasMap.contains(v)) {
                groupByExpressions += uv
              } else {
                groupByExpressions += unresolvedAlias(Alias(uv, aliasName)(qualifier = Some(operator.getName)))
              }
//              groupByExpressions += unresolvedAlias(Alias(uv, aliasName)(qualifier = Some(operator.getName)))
              if(v.getName == "J1") {
                isMavg = true
              }
            }
            case d: DbTypeBase =>
              throw new UnsupportedOperationException(s"DbTypeBase $d used in GroupBy")
              // TODO: victor - not sure the following original code is correct or not
              // val literal = Literal.create(Utilities.getDbTypeBaseValue(d), Utilities.getSparkDataType(d.getDataType))
              // groupByArguments += UnresolvedAlias(Alias(literal, aliasName)())
          }
          idx += 1
        }

        val plan: LogicalPlan = if (groupByArguments.isEmpty) {
          Aggregate(Nil, aggregateExpressions, subPlan)
        } else {
          if (operator.getOperatorType == OperatorType.AGGREGATE) {
            Aggregate(groupByArguments, groupByExpressions ++ aggregateExpressions, subPlan)
          } else {
            // var partitioning = getPartitioning(operator.getName, operator.getArity)
            // if (partitioning eq Nil)
            val partitioning = getDefaultPartitioning(operator.getArity)
            MonotonicAggregate(groupByArguments, groupByExpressions ++ aggregateExpressions, subPlan, partitioning)
          }
        }

        SubqueryAlias(operator.getName, plan)
      case OperatorType.FILTER =>
        val subPlan = getPlan(operator.getChild(0))

        var expressions: Expression = null
        operator.asInstanceOf[FilterOperator].getExpressions
          .map(c => new UnresolvedAttributeQualifier(operator.getChild(0), renamed, aliasMap).toExpression(c))
          .foreach(expr => { expressions = if (expressions == null) expr else And(expressions, expr)})

        Filter(expressions, subPlan)
      case OperatorType.BASE_RELATION =>
        var relation: LogicalPlan = UnresolvedRelation(TableIdentifier(operator.getName), None)
        val operatorName = operator.getName()
        if (recursivePlanDetails.containsBaseRelation(operatorName)) {
          val name = operator.getName + this.subQueryCounter.getAndIncrement()
          relation = SubqueryAlias(name, Project(Seq(UnresolvedStar(None)), relation))
          operator.setName(name)
          renamed.put(operator, operator.getName())
        }
        recursivePlanDetails.addBaseRelation(operatorName, relation)
        relation
      case OperatorType.RECURSIVE_RELATION =>
        val table = sparkSession.sessionState.catalog.lookupRelation(TableIdentifier(operator.getName))
        if (table == null) {
          throw new SparkException("No recursive relation with name '" + operator.getName + "'")
        }

        // we resolve these attributes here since this is a virtual table
        val output = operator.getArguments.zipWithIndex.map(argWithIndex => {
          val variable = argWithIndex._1.asInstanceOf[Variable]
          val dataType = table.output(argWithIndex._2).dataType
          AttributeReference(variable.getName, dataType, nullable = false)()
        })

        // var partitioning = getPartitioning(operator.getName, operator.getArity)
        // if (partitioning eq Nil)
        val  partitioning = getDefaultPartitioning(operator.getArity)

        val relation: LogicalPlan = if (recursiveRelationNames.contains(operator.getName)) {
          val nonLinear = NonLinearRecursiveRelation(operator.getName, output, partitioning)
          // if we use a broadcast join, this relation could be broadcast before the recursion operator has a chance to set it
          // victor: bigDatalogSession.sessionState.setRecursiveRDD(nonLinear.name, bigDatalogSession.sparkContext.emptyRDD[InternalRow])
          nonLinear
        } else {
          val matchingClique = cliqueOperatorStack.filter(_.getName.equals(operator.getName))
          if (matchingClique.nonEmpty && matchingClique.forall(_.getEvaluationType == EvaluationType.MonotonicSemiNaive)) {
            val aggregateRelation = AggregateRelation(operator.getName, output, partitioning)
            aggregateRelation
          } else {
            val linear = LinearRecursiveRelation(operator.getName, output, partitioning)
            linear
          }
        }

        recursiveRelationNames += operator.getName

        val name = operator.getName + this.subQueryCounter.getAndIncrement()
        val plan = SubqueryAlias(name, relation)
        operator.setName(name)
        renamed.put(operator, operator.getName())
        plan
      case OperatorType.NEGATION =>
        getPlan(operator.getChild(0))
      case OperatorType.SORT =>
        val subPlan = getPlan(operator.getChild(0))
        val sortOperator = operator.asInstanceOf[SortOperator]
        val output = subPlan.output
        val sortExprs = sortOperator.getSortOrders.map(so => {
          val attr = output(so.index)
          if (so.ascending) {
            SortOrder(attr, Ascending)
          } else {
            SortOrder(attr, Descending)
          }
        })

        Sort(sortExprs, true, subPlan)
      case OperatorType.LIMIT =>
        val subPlan = getPlan(operator.getChild(0))
        val arg = operator.getArgument(0)
        val limitArg : Expression = arg match {
          case d: DbTypeBase =>
            Literal.create(TypeUtils.getDbTypeBaseValue(d), TypeUtils.getSparkDataType(d.getDataType))
          case _ => null
        }
        Limit(limitArg, subPlan)
      case OperatorType.TUPLE =>
        val output = operator.getArguments.map(x => {
          val arg = x.asInstanceOf[AliasedArgument].getAlias
          AttributeReference(arg.toString, TypeUtils.getSparkDataType(arg.getDataType), false)()
        })

        val values: Array[Any] = operator.getArguments.map(arg => arg match {
          case aa: AliasedArgument => {
            val arg = aa.getArgument.asInstanceOf[DbTypeBase]
            TypeUtils.createDbTypeBaseConverter(arg.getDataType)(arg)
          }
        }).toArray

        SubqueryAlias(operator.getName,
          LocalRelation(output, Seq(new GenericInternalRow(values))))
      case OperatorType.ASSIGNMENT =>
        // victor - currently only works for simple assignment, e.g. B = 1
        val args = operator.getArguments

        val variable = args.get(0).asInstanceOf[Variable]
        val output = AttributeReference(variable.toString, TypeUtils.getSparkDataType(variable), false)() :: Nil

        val v = args.get(1).asInstanceOf[DbTypeBase]
        val row = Array(TypeUtils.createDbTypeBaseConverter(v.getDataType)(v))

        LocalRelation(output, Seq(new GenericInternalRow(row)))
      case _ => null
    }
  }

  private def getCliqueOutput(operator: Operator, recursiveOp: LogicalPlan): Seq[NamedExpression] = {
    // skip any SubqueryAlias op
    var child = recursiveOp
    // Youfu Li added this part to support the Distinct of Union operator
    if (child.isInstanceOf[Distinct]) {
      child = child.asInstanceOf[Distinct].child
      while (child.isInstanceOf[SubqueryAlias]) {
        child = child.children.head
      }
      if (child.isInstanceOf[Union]) {
        child = child.asInstanceOf[Union].children.head
      }
//      while (child.isInstanceOf[SubqueryAlias]) {
//        child = child.children.head
//      }
//      if (child.isInstanceOf[MonotonicAggregate]) {
//        child = child.asInstanceOf[MonotonicAggregate].child
//      }
    }

    if (child.isInstanceOf[Filter]) {
      child = child.asInstanceOf[Filter].child
    }

    while (child.isInstanceOf[SubqueryAlias]) {
      child = child.children.head
    }

    // the only possible recursive side's op with valid attributes is Project or MonotonicAggregate
    val childOutput = child match {
      case p: Project =>
        p.projectList
      case m: MonotonicAggregate =>
        m.aggregateExpressions
      case r: LinearRecursiveRelation =>
        r.output
      case r: NonLinearRecursiveRelation =>
        r.output
      case r: AggregateRelation =>
        r.output
      case other =>
        throw new UnsupportedOperationException(other.toString)
    }

    // CliqueOp attributes often renamed as (Var_1, Var_2), thus we need to insert a project op
    // and add alias to the output attributes of childOp (recursionOp)
    val output = operator.getArguments.zip(childOutput).map {
      case (v: Variable, UnresolvedAlias(alias: Alias, _)) =>
        unresolvedAlias(Alias(UnresolvedAttribute(alias.name), v.getName)())
      case (v: Variable, attr) =>
        unresolvedAlias(Alias(attr, v.getName)())
      case other =>
        throw new UnsupportedOperationException(s"$other")
    }
    output
  }

  private def getProjectOutput(operator: Operator): Seq[NamedExpression] = {
    val variableFunc: UnresolvedAttributeQualifier = new UnresolvedAttributeQualifier(operator.getChild(0), renamed, aliasMap)

    var idx: Int = 0
    val output = operator.getArguments.map {
      case v: Variable =>
        variableFunc.toUnresolvedAttribute(v)
      case d: DbTypeBase =>
        idx += 1
        Alias(Literal.create(TypeUtils.getDbTypeBaseValue(d), TypeUtils.getSparkDataType(d.getDataType)), s"c_$idx")()
      case aa: AliasedArgument =>
        val v = aa.getAlias.asInstanceOf[Variable]
        unresolvedAliasVariable(Alias(variableFunc.toExpression(aa.getArgument), UniqueName.gen(v.getName))(), v)
      case av: AliasedVariable =>
        val v = av.getVariable
        unresolvedAliasVariable(Alias(variableFunc.toExpression(av.getVariable), UniqueName.gen(av.getAlias))(), v)
    }
    output
  }

  private def getPartitioning(name: String, arity: Int): Seq[Int] = {
    val userDefined = conf.get("spark.datalog.partitioning." + name, "")
    // user defined partition is used first
    if (userDefined.length == 0) {
      if (gpsi != null && !gpsi.isEmpty) {
        gpsi.getGPSForRecursion(name, arity)
      } else {
        Seq()
      }
    } else {
      // format will be [0 or 1,..., 0 or 1]
      userDefined.substring(1, userDefined.length - 1).split(",").map(_.trim.toInt)
    }
  }

  def getDefaultPartitioning(length: Int): Seq[Int] = {
    // partition on the first argument
    Array(1) ++ Array.fill[Int](length - 1)(0)
  }
}
