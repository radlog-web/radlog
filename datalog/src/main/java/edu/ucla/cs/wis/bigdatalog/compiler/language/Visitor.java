package edu.ucla.cs.wis.bigdatalog.compiler.language;

import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.TerminalNode;

import edu.ucla.cs.wis.bigdatalog.compiler.*;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.*;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.*;
import edu.ucla.cs.wis.bigdatalog.compiler.type.*;
import edu.ucla.cs.wis.bigdatalog.compiler.type.expression.CompilerArithmeticExpression;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.*;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class Visitor extends DeALBaseVisitor<CompilerTypeBase> {
	protected ParserManager parserManager;

	public void setParserManager(ParserManager parserManager) {
		this.parserManager = parserManager;
	}

	@Override
	public CompilerTypeBase visitParseQueryForm(@NotNull DeALParser.ParseQueryFormContext ctx) {
		return this.visitQueryForm(ctx.queryForm());
	}

	@Override
	public CompilerTypeBase visitParseQuery(@NotNull DeALParser.ParseQueryContext ctx) {
		return this.visitPredicate(ctx.predicate());
	}

	@Override
	public CompilerTypeBase visitParseRule(@NotNull DeALParser.ParseRuleContext ctx) {
		return this.visitPredicateRule(ctx.predicateRule());
	}

	@Override
	public CompilerTypeBase visitParseGroundTerm(@NotNull DeALParser.ParseGroundTermContext ctx) {
		return this.visitGroundTerm(ctx.groundTerm());
	}

	@Override
	public CompilerTypeBase visitParseGroundPredicate(@NotNull DeALParser.ParseGroundPredicateContext ctx) {
		return this.visitFact(ctx.fact());
	}

	@Override
	public CompilerTypeBase visitParseLoadDatabaseFacts(@NotNull DeALParser.ParseLoadDatabaseFactsContext ctx) {
		CompilerTypeList factList = new CompilerTypeList();
		for (DeALParser.FactContext fCtx : ctx.fact())
			factList.add(this.visitFact(fCtx));

		return factList;
	}

	@Override
	public CompilerTypeBase visitParseLoadDatabaseObjects(@NotNull DeALParser.ParseLoadDatabaseObjectsContext ctx) {
		return this.visitDatabaseObjectDeclarations(ctx
				.databaseObjectDeclarations());
	}

	@Override
	public CompilerTypeBase visitDatabaseObjectDeclarations(@NotNull DeALParser.DatabaseObjectDeclarationsContext ctx) {
		CompilerTypeList declarations = new CompilerTypeList();
		ModuleDeclaration module = null;
		if (ctx.module != null)
			module = (ModuleDeclaration) this.visitModuleDeclaration(ctx.module);
		else
			module = loadModuleDeclaration("default", ctx.databaseDeclaration());

		declarations.add(module);

		if (ctx.tail != null) {
			CompilerTypeList moreDeclarations = (CompilerTypeList) this.visitDatabaseObjectDeclarations(ctx.tail);
			declarations.appendList(moreDeclarations);
		}
		return declarations;
	}

	@Override
	public CompilerTypeBase visitDatabaseDeclaration(@NotNull DeALParser.DatabaseDeclarationContext ctx) {
		if (ctx.dbbps != null)
			return this.visitDatabaseBasePredicates(ctx.dbbps);

		if (ctx.bpkis != null)
			return this.visitBasePredicateKeyOrIndex(ctx.bpkis);

		// if (ctx.bpis != null) return this.visitBasePredicateIndex(ctx.bpis);
		if (ctx.ex != null)
			return this.visitExport(ctx.ex);

		if (ctx.f != null)
			return this.visitFact(ctx.f);

		return this.visitPredicateRule(ctx.pr);
	}

	@Override
	public CompilerTypeBase visitModuleDeclaration(@NotNull DeALParser.ModuleDeclarationContext ctx) {
		return loadModuleDeclaration(ctx.anyString().getText(),	ctx.databaseDeclaration());
	}

	private ModuleDeclaration loadModuleDeclaration(String moduleName, List<DeALParser.DatabaseDeclarationContext> databaseDeclarationContexts) {
		ModuleDeclaration moduleDeclaration = new ModuleDeclaration(moduleName);
		CompilerTypeBase declaration;
		for (DeALParser.DatabaseDeclarationContext dbCtx : databaseDeclarationContexts) {
			declaration = this.visitDatabaseDeclaration(dbCtx);
			switch (declaration.getType()) {
			case SCHEMA_KEY:
				moduleDeclaration.addBasePredicateKey((BasePredicateKey) declaration);
				break;
			case SCHEMA_INDEX:
				moduleDeclaration.addBasePredicateIndex((BasePredicateIndex) declaration);
				break;
			case EXPORT:
				moduleDeclaration.addExport((Export) declaration);
				break;
			case RULE:
				moduleDeclaration.addRule((Rule) declaration);
				break;
			case PREDICATE:
				moduleDeclaration.addFact((Predicate) declaration);
				break;
			default: // database case
				CompilerTypeList relations = (CompilerTypeList) declaration;
				for (int j = 0; j < relations.size(); j++)
					moduleDeclaration.addBasePredicate((BasePredicate) relations.get(j));
			}
		}
		return moduleDeclaration;
	}

	@Override
	public CompilerTypeBase visitDatabaseBasePredicates(@NotNull DeALParser.DatabaseBasePredicatesContext ctx) {
		CompilerTypeList relations = new CompilerTypeList();

		for (DeALParser.BasePredicateContext bpCtx : ctx.basePredicate())
			relations.add(this.visitBasePredicate(bpCtx));

		return relations;
	}

	@Override
	public CompilerTypeBase visitBasePredicate(@NotNull DeALParser.BasePredicateContext ctx) {
		CompilerTypeList bpsasList = new CompilerTypeList();
		for (DeALParser.BasePredicateStructuralAttributeContext ctxBpsa : ctx
				.basePredicateStructuralAttribute())
			bpsasList.add(this.visitBasePredicateStructuralAttribute(ctxBpsa));

		List<BasePredicateStructuralAttribute> bpsas = new ArrayList<>();
		for (int i = 0; i < bpsasList.size(); i++)
			bpsas.add((BasePredicateStructuralAttribute) bpsasList.get(i));

		return new BasePredicate(ctx.pn.getText(), bpsas);
	}

	@Override
	public CompilerTypeBase visitBasePredicateStructuralAttribute(@NotNull DeALParser.BasePredicateStructuralAttributeContext ctx) {
		if (ctx.dt != null) {
			DataType dataType = DataType.getDataType(ctx.dt.getText());
			if (ctx.n != null)
				return new BasePredicateStructuralAttribute(dataType, ctx.n.getText(), null);

			return new BasePredicateStructuralAttribute(dataType, "", null);
		}

		CompilerTypeList schemaTypesList = new CompilerTypeList();
		for (DeALParser.BasePredicateStructuralAttributeContext ctxBpsa : ctx.basePredicateStructuralAttribute())
			schemaTypesList.add(this.visitBasePredicateStructuralAttribute(ctxBpsa));

		BasePredicateStructuralAttribute[] bpsas = new BasePredicateStructuralAttribute[schemaTypesList.size()];
		for (int i = 0; i < schemaTypesList.size(); i++)
			bpsas[i] = (BasePredicateStructuralAttribute) schemaTypesList.get(i);

		if (ctx.n != null) {
			if (ctx.l2 == null)
				return new BasePredicateStructuralAttribute(DataType.COMPLEX, ctx.n.getText(), bpsas);

			return new BasePredicateStructuralAttribute(DataType.LIST, ctx.n.getText(), bpsas);
		}
		return new BasePredicateStructuralAttribute(DataType.LIST, "", bpsas);
	}

	@Override
	public CompilerTypeBase visitBasePredicateKeyOrIndex(@NotNull DeALParser.BasePredicateKeyOrIndexContext ctx) {
		List<Integer> ots = new ArrayList<>();
		for (TerminalNode tn : ctx.INTEGER())
			ots.add(Integer.parseInt(tn.getText()));

		if (ctx.KEY() == null)
			return new BasePredicateIndex(ctx.pn.getText(), ots);
		return new BasePredicateKey(ctx.pn.getText(), ots);
	}

	@Override
	public CompilerTypeBase visitExport(@NotNull DeALParser.ExportContext ctx) {
		return new Export((QueryForm) this.visitQueryForm(ctx.queryForm()));
	}

	@Override
	public CompilerTypeBase visitFact(@NotNull DeALParser.FactContext ctx) {
		Predicate predicate = new Predicate(ctx.IDENTIFIER().getText(),
				(CompilerTypeList) this.visitGroundArguments(ctx.groundArguments()));
		predicate.setAsBasePredicate();
		return predicate;
	}

	@Override
	public CompilerTypeBase visitGroundTerm(@NotNull DeALParser.GroundTermContext ctx) {
		if (ctx.gafunc != null) {
			if (ctx.fn != null)
				return new CompilerFunctor(ctx.fn.getText(), (CompilerTypeList) this.visitGroundArguments(ctx.gafunc));
			
			return new CompilerFunctor("", (CompilerTypeList) this.visitGroundArguments(ctx.gafunc));
		}

		if (ctx.galist != null)
			return this.visitGroundListTerms(ctx.galist);

		if (ctx.iv != null)
			return new CompilerInt(Integer.parseInt(ctx.iv.getText()));

		if (ctx.dv != null)
			return new CompilerDouble(Double.parseDouble(ctx.dv.getText()));

		return this.visitAnyString(ctx.s);
	}

	@Override
	public CompilerTypeBase visitGroundArguments(@NotNull DeALParser.GroundArgumentsContext ctx) {
		CompilerTypeList args = new CompilerTypeList();
		for (DeALParser.GroundTermContext ctxGT : ctx.groundTerm())
			args.add(this.visitGroundTerm(ctxGT));

		return args;
	}

	@Override
	public CompilerTypeBase visitGroundListTerms(@NotNull DeALParser.GroundListTermsContext ctx) {
		CompilerList tail = null;
		if (ctx.groundListTerms2() != null)
			tail = (CompilerList) this.visitGroundListTerms2(ctx.groundListTerms2());
		return new CompilerList(this.visitGroundTerm(ctx.groundTerm()), tail);
	}

	@Override
	public CompilerTypeBase visitGroundListTerms2(@NotNull DeALParser.GroundListTerms2Context ctx) {
		if (ctx.groundTerm() != null)
			return new CompilerList(this.visitGroundTerm(ctx.groundTerm()));

		return this.visitGroundListTerms(ctx.groundListTerms());
	}

	@Override
	public CompilerTypeBase visitPredicateRule(@NotNull DeALParser.PredicateRuleContext ctx) {
		this.parserManager.resetVariableList();

		List<Predicate> body = null;

		if (ctx.body != null) {
			CompilerTypeList bodyList = (CompilerTypeList) this.visitRuleBody(ctx.body);
			if (bodyList != null && bodyList.size() > 0) {
				body = new ArrayList<>();

				for (int i = 0; i < bodyList.size(); i++)
					body.add((Predicate) bodyList.get(i));
			}
		}

		Rule rule = new Rule(String.valueOf(this.parserManager.getRuleIndex()),
				(Predicate) this.visitRuleHead(ctx.head), body);

		if (ctx.annotations() != null)
			rule.setAnnotations(ctx.annotations().getText());

		return rule;
	}

	@Override
	public CompilerTypeBase visitRuleHead(@NotNull DeALParser.RuleHeadContext ctx) {
		if (ctx.hp != null)
			return this.visitHeadPredicate(ctx.hp);

		CompilerTypeList args = new CompilerTypeList();
		args.add(new CompilerString(ctx.an.getText()));
		for (DeALParser.TermContext tCtx : ctx.term())
			args.add(this.visitTerm(tCtx));

		String predicateName = ctx.action.getText();

		if (predicateName.equals(BuiltInPredicate.EMPTY_PREDICATE_NAME)	&& args.size() == 2)
			return new BuiltInPredicate(BuiltInPredicate.EMPTY_PREDICATE_NAME,
					args, BuiltInPredicateType.EMPTY);

		if (predicateName.equals(BuiltInPredicate.SINGLE_PREDICATE_NAME) && args.size() == 3)
			return new BuiltInPredicate(BuiltInPredicate.SINGLE_PREDICATE_NAME,
					args, BuiltInPredicateType.SINGLE);

		if (predicateName.equals(BuiltInPredicate.MULTI_PREDICATE_NAME)	&& args.size() == 4)
			return new BuiltInPredicate(BuiltInPredicate.MULTI_PREDICATE_NAME,
					args, BuiltInPredicateType.MULTI);

		if (predicateName.equals(BuiltInPredicate.RETURN_PREDICATE_NAME) && args.size() == 4)
			return new BuiltInPredicate(BuiltInPredicate.RETURN_PREDICATE_NAME,
					args, BuiltInPredicateType.RETURN);

		throw new CompilerException("User Defined Aggregate Predicate has invalid number of arguments.");
	}

	@Override
	public CompilerTypeBase visitRuleBody(@NotNull DeALParser.RuleBodyContext ctx) {
		CompilerTypeList args = new CompilerTypeList();
		for (DeALParser.LiteralContext ctxL : ctx.literal())
			args.add(this.visitLiteral(ctxL));

		return args;
	}

	@Override
	public CompilerTypeBase visitLiteral(@NotNull DeALParser.LiteralContext ctx) {
		if (ctx.action == null) {
			if (ctx.o != null) {
				CompilerTypeList args = new CompilerTypeList();
				args.add(this.visitTerm(ctx.t1));
				args.add(this.visitTerm(ctx.t2));
				return new BuiltInPredicate(ctx.o.getText(), args, BuiltInPredicateType.BINARY);
			}

			Predicate predicate = (Predicate) this.visitPredicate(ctx.p);
			if (ctx.neg != null)
				predicate.setAsNegative();
			return predicate;
		}

		String action = ctx.action.getText().toLowerCase();

		if (action.equals("true")) {
			return new BuiltInPredicate(BuiltInPredicate.TRUE_PREDICATE_NAME,
					new CompilerTypeList(), BuiltInPredicateType.TRUE);
		} else if (action.equals("false")) {
			return new BuiltInPredicate(BuiltInPredicate.FALSE_PREDICATE_NAME,
					new CompilerTypeList(), BuiltInPredicateType.FALSE);
		} else if (action.equals("if")) {
			if (ctx.b3 == null)
				return new IfThenPredicate((CompilerTypeList) this.visitRuleBody(ctx.b1),
						(CompilerTypeList) this.visitRuleBody(ctx.b2));

			return new IfThenElsePredicate((CompilerTypeList) this.visitRuleBody(ctx.b1),
					(CompilerTypeList) this.visitRuleBody(ctx.b2),
					(CompilerTypeList) this.visitRuleBody(ctx.b3));
		} else if (action.equals("choice")) {
			CompilerTypeList args = new CompilerTypeList();
			args.add(this.visitChoiceArgument(ctx.ca1));
			args.add(this.visitChoiceArgument(ctx.ca2));

			return new BuiltInPredicate("$$_choice_" + this.parserManager.getNextChoiceCount(), args,
					BuiltInPredicateType.CHOICE);
		} else if (action.equals("limit")) {
			CompilerTypeList args = new CompilerTypeList();
			if (ctx.variable() == null || ctx.variable().isEmpty())
				args.add(new CompilerInt(Integer.parseInt(ctx.INTEGER().getText())));
			else
				args.add(this.parserManager.getVariable(ctx.variable().get(0).getText()));

			return new BuiltInPredicate("limit", args, BuiltInPredicateType.LIMIT);
		} else if (action.equals("sort")) {
			CompilerTypeList args = new CompilerTypeList();
			for (int i = 0; i < ctx.variable().size(); i++)
				args.add(CompilerFunctor.createFunctor(new CompilerTypeBase[] {
						this.parserManager.getVariable(ctx.variable().get(i).getText()),
						new CompilerString(ctx.SORT_ORDER().get(i).getText()) }));
			return new BuiltInPredicate("sort", args, BuiltInPredicateType.SORT);
		}

		return null;
	}

	@Override
	public CompilerTypeBase visitChoiceArgument(@NotNull DeALParser.ChoiceArgumentContext ctx) {
		CompilerTypeList args = new CompilerTypeList();
		for (DeALParser.VariableContext ctxV : ctx.variable())
			args.add(this.parserManager.getVariable(ctxV.getText()));

		return new CompilerFunctor("", args);
	}

	@Override
	public CompilerTypeBase visitHeadPredicate(@NotNull DeALParser.HeadPredicateContext ctx) {
		Predicate headPredicate;
		String predicateName = ctx.IDENTIFIER().getText();
		CompilerTypeList args = new CompilerTypeList();

		for (DeALParser.HeadTermContext ctxHT : ctx.headTerm())
			args.add(this.visitHeadTerm(ctxHT));

		int arity = args.size();

		if (DeALParser.isBuiltInPredicate(predicateName, arity))
			headPredicate = new BuiltInPredicate(predicateName, args, BuiltInPredicateType.GENERIC);
		else
			headPredicate = new Predicate(predicateName, args);

		return headPredicate;
	}

	@Override
	public CompilerTypeBase visitHeadTerm(@NotNull DeALParser.HeadTermContext ctx) {
		if (ctx.ae != null)
			return this.visitArithmeticExpression(ctx.ae);

		if (ctx.hat != null)
			return this.visitHeadAggregateTerm(ctx.hat);

		if (ctx.hnat != null)
			return this.visitNonArithmeticTerm(ctx.hnat);

		return new CompilerNil();
	}

	@Override
	public CompilerTypeBase visitHeadAggregateTerm(@NotNull DeALParser.HeadAggregateTermContext ctx) {
		if (ctx.fsagg != null) {
			String fsAgg = ctx.fsagg.getText();
			if (fsAgg.equals(FSAggregate.FSMAX_NAME)
					|| fsAgg.equals(FSAggregate.FSMAX_NAME2)
					|| fsAgg.equals(FSAggregate.FSMIN_NAME)
					|| fsAgg.equals(FSAggregate.FSMIN_NAME2))
				return new FSAggregate(fsAgg, this.parserManager.getVariable(ctx.vt.getText()));

			return new FSAggregate(fsAgg, this.visitHeadFscntSubterms(ctx.hfs));
		}

		if (BuiltInAggregateType.getBuiltInAggregateType(ctx.aggr.getText()) != null)
			return new BuiltInAggregate(ctx.aggr.getText(),	this.visitTerm(ctx.hs));

		return new UserDefinedAggregate(ctx.aggr.getText(), this.visitTerm(ctx.hs));
	}

	@Override
	public CompilerTypeBase visitHeadFscntSubterms(@NotNull DeALParser.HeadFscntSubtermsContext ctx) {
		if (ctx.vt1 == null)
			return this.visitArithmeticTerm(ctx.at);

		if (ctx.vt2 == null)
			return CompilerFunctor.createFunctor(new CompilerTypeBase[] {this.parserManager.getVariable(ctx.vt1.getText()),
					this.visitArithmeticTerm(ctx.at) });

		if (ctx.vts == null)
			return CompilerFunctor.createFunctor(new CompilerTypeBase[] {this.parserManager.getVariable(ctx.vt1.getText()),
					this.parserManager.getVariable(ctx.vt2.getText()) });

		CompilerTypeBase firstArg = this.parserManager.getVariable(ctx.vt1.getText());

		CompilerTypeList variables = (CompilerTypeList) this.visitVariables(ctx.vts);

		CompilerTypeList funcArgs = new CompilerTypeList();
		funcArgs.add(this.parserManager.getVariable(ctx.vt2.getText()));
		funcArgs.appendList(variables);

		CompilerFunctor func = new CompilerFunctor("", funcArgs);

		return CompilerFunctor.createFunctor(new CompilerTypeBase[] { firstArg, func });
	}

	@Override
	public CompilerTypeBase visitVariables(@NotNull DeALParser.VariablesContext ctx) {
		CompilerTypeList args = new CompilerTypeList();
		for (DeALParser.VariableContext vCtx : ctx.variable())
			args.add(this.parserManager.getVariable(vCtx.getText()));

		return args;
	}

	@Override
	public CompilerTypeBase visitPredicate(@NotNull DeALParser.PredicateContext ctx) {
		Predicate predicate;

		String predicateName = ctx.IDENTIFIER().getText();
		CompilerTypeList args = new CompilerTypeList();
		for (DeALParser.TermContext ctxT : ctx.term())
			args.add(this.visitTerm(ctxT));

		int arity = args.size();

		if (DeALParser.isBuiltInPredicate(predicateName, arity))
			predicate = new BuiltInPredicate(predicateName.toLowerCase(), args, BuiltInPredicateType.GENERIC);
		else
			predicate = new Predicate(predicateName, args);

		return predicate;
	}

	@Override
    public CompilerTypeBase visitBasicExpression(@NotNull DeALParser.BasicExpressionContext ctx) {
        if (ctx.at != null)
            return this.visitArithmeticTerm(ctx.at);

        return this.visitArithmeticExpression(ctx.ae);
    }

	@Override
	public CompilerTypeBase visitUnaryArithmeticExpression(@NotNull DeALParser.UnaryArithmeticExpressionContext ctx) {
		// unary (aka log) case
		if (ctx.l != null)
			return new CompilerArithmeticExpression(ctx.l.getText(), this.visitBasicExpression(ctx.be));

		return this.visitBasicExpression(ctx.be);
//		if (ctx.at != null)
//			return this.visitArithmeticTerm(ctx.at);
//
//		return this.visitArithmeticExpression(ctx.ae);
	}

	@Override
	public CompilerTypeBase visitMultiplicativeArithmeticExpression(@NotNull DeALParser.MultiplicativeArithmeticExpressionContext ctx) {
		// positive case
		if (ctx.s == null) {
			if (ctx.uae2 == null)
				return this.visitUnaryArithmeticExpression(ctx.uae1);

			// CompilerTypeList args = new CompilerTypeList();
			// args.add(this.visitUnaryArithmeticExpression(ctx.uae1));
			// args.add(this.visitUnaryArithmeticExpression(ctx.uae2));
			return new CompilerArithmeticExpression(ctx.p.getText(),
					this.visitUnaryArithmeticExpression(ctx.uae1),
					this.visitUnaryArithmeticExpression(ctx.uae2));
		}

		// negative cases
		CompilerTypeBase ae = this.visitUnaryArithmeticExpression(ctx.uae);
		if (ae.getType() == CompilerType.COMPILER_INT)
			return new CompilerInt(-((CompilerInt) ae).getValue());
		else if (ae.getType() == CompilerType.COMPILER_FLOAT)
			return new CompilerFloat(-((CompilerFloat) ae).getValue());
		else if (ae.getType() == CompilerType.COMPILER_DOUBLE)
			return new CompilerDouble(-((CompilerDouble) ae).getValue());
		
		// CompilerInteger zero = new CompilerInteger(0);
		// CompilerTypeList args = new CompilerTypeList();
		// args.add(zero);
		// args.add(ae);
		// return new CompilerFunctor("-", args);
		return new CompilerArithmeticExpression("-", new CompilerInt(0), ae);
	}

	@Override
	public CompilerTypeBase visitArithmeticExpression(@NotNull DeALParser.ArithmeticExpressionContext ctx) {
		if (ctx.mae2 == null)
			return this.visitMultiplicativeArithmeticExpression(ctx.mae1);

		//CompilerTypeList args = new CompilerTypeList();
		//args.add(this.visitMultiplicativeArithmeticExpression(ctx.mae1));
		//args.add(this.visitMultiplicativeArithmeticExpression(ctx.mae2));
		return new CompilerArithmeticExpression(ctx.p.getText(), 
				this.visitMultiplicativeArithmeticExpression(ctx.mae1), 
				this.visitMultiplicativeArithmeticExpression(ctx.mae2));
		//return new CompilerFunctor(ctx.p.getText(), args);
	}

	@Override
	public CompilerTypeBase visitArithmeticTerm(@NotNull DeALParser.ArithmeticTermContext ctx) {
		if (ctx.v != null)
			return this.parserManager.getVariable(ctx.v.getText());

		if (ctx.iv != null)
			return new CompilerInt(Integer.parseInt(ctx.iv.getText()));

		return new CompilerDouble(Double.parseDouble(ctx.dv.getText()));
	}

	@Override
	public CompilerTypeBase visitName(@NotNull DeALParser.NameContext ctx) {
		if (ctx.keyword() != null) {
			// upper case is variable, lower case is string
			String keyword = ctx.keyword().getText();
			if (Character.isUpperCase(keyword.charAt(0)))
				return this.parserManager.getVariable(keyword);

			return CompilerString.create(keyword);
		}

		if (ctx.VARIABLENAME() != null)
			return this.parserManager.getVariable(ctx.VARIABLENAME().getText());

		return CompilerString.create(ctx.IDENTIFIER().getText());
	}

	@Override
	public CompilerTypeBase visitAnyString(@NotNull DeALParser.AnyStringContext ctx) {
		if (ctx.name() != null)
			return this.visitName(ctx.name());

		return CompilerString.create(ctx.STRING().getText());
	}

	@Override
	public CompilerTypeBase visitNonArithmeticTerm(@NotNull DeALParser.NonArithmeticTermContext ctx) {
		if (ctx.ft != null)
			return visitFunctorTerm(ctx.ft);

		if (ctx.lt != null)
			return this.visitListTerm(ctx.lt);

		// return CompilerString.create(ctx.s.getText());
		return this.visitAnyString(ctx.s);
	}

	@Override
	public CompilerTypeBase visitFunctorTerm(@NotNull DeALParser.FunctorTermContext ctx) {
		CompilerTypeList args = new CompilerTypeList();
		for (DeALParser.TermContext ctxT : ctx.term())
			args.add(this.visitTerm(ctxT));

		if (ctx.n != null)
			return new CompilerFunctor(ctx.n.getText(), args);

		return new CompilerFunctor("", args);
	}

	@Override
	public CompilerTypeBase visitListTerm(@NotNull DeALParser.ListTermContext ctx) {
		if (ctx.listTermArguments() == null)
			return new CompilerList();

		return visitListTermArguments(ctx.listTermArguments());
	}

	@Override
	public CompilerTypeBase visitListTermArguments(@NotNull DeALParser.ListTermArgumentsContext ctx) {
		CompilerList tail = null;

		if (ctx.variable() != null)
			tail = new CompilerList(this.parserManager.getVariable(ctx.variable().getText()));
		else if (ctx.listTermArguments() != null)
			tail = (CompilerList) this.visitListTermArguments(ctx.listTermArguments());

		return new CompilerList(this.visitTerm(ctx.term()), tail);
	}

	@Override
	public CompilerTypeBase visitQueryForm(@NotNull DeALParser.QueryFormContext ctx) {
		this.parserManager.resetVariableList();
		CompilerTypeList args = new CompilerTypeList();
		DeALParser.QueryFormArgumentsContext qfac = ctx.queryFormArguments();
		if (qfac != null)
			args = (CompilerTypeList) this.visitQueryFormArguments(qfac);
		// args.add(ctx.queryFormArguments().qftn);
		// args.add(ctx.qfa.);
		QueryForm queryForm = new QueryForm(ctx.IDENTIFIER().getText(), args);
		if (DeALParser.isBuiltInPredicate(queryForm.getPredicateName(),	queryForm.getArity())) {
			queryForm.setAsBuiltInPredicate();
			queryForm.setAsGenericPredicate();
		}

		return queryForm;
	}

	@Override
	public CompilerTypeBase visitQueryFormArguments(@NotNull DeALParser.QueryFormArgumentsContext ctx) {
		CompilerTypeList args = new CompilerTypeList();
		for (DeALParser.QueryFormTermContext ctxQFT : ctx.queryFormTerm())
			args.add(this.visitQueryFormTerm(ctxQFT));

		return args;
	}

	public CompilerTypeBase visitQueryFormTerm(@NotNull DeALParser.QueryFormTermContext ctx) {
		if (ctx.qfa != null) {
			if (ctx.fn != null)
				return new CompilerFunctor(ctx.fn.getText(), (CompilerTypeList) this.visitQueryFormArguments(ctx.qfa));
			return new CompilerFunctor("", (CompilerTypeList) this.visitQueryFormArguments(ctx.qfa));
		}

		if (ctx.vt != null)
			return this.parserManager.getVariable(ctx.vt.getText());

		if (ctx.ivt != null)
			return new CompilerInputVariable(ctx.ivt.getText());

		if (ctx.dv != null)
			return new CompilerDouble(Double.parseDouble(ctx.dv.getText()));

		if (ctx.iv != null)
			return new CompilerInt(Integer.parseInt(ctx.iv.getText()));

		if (ctx.qfaList != null)
			return new CompilerList((CompilerTypeList) this.visitQueryFormArguments(ctx.qfaList));

		if (ctx.s != null)
			return this.visitAnyString(ctx.s);

		return new CompilerList();
	}
}
