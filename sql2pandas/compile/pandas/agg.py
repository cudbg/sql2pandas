import json
from ...context import Context
from ..translator import *
from ...exprs import *
from ..agg import *
from .translator import *


class PandasGroupByBottomTranslator(GroupByBottomTranslator, PandasTranslator):
  def produce(self, ctx):
    self.v_outdf = ctx.new_var("df")

    ctx.request_vars(dict(df=None))
    self.child_translator.produce(ctx)


  def consume(self, ctx):
    """
    Evaluate expressions in GROUP BY clause and
    aggregation function arguments in SELECT clause


    if all agg functions take one argument
    
      df.groupby([attrs]).agg(dict(outattr=(attrarg, funcname)))


    TODO: if there are agg functions that take multiple arguments:

      df.groupby([attrs]).apply(lambda df: newdf or value or series)


    """
    v_df = ctx['df']
    ctx.pop_vars()

    ctx.add_line("")
    ctx.add_line("# Start Groupby %s" % self.op)

    assignments = []

    # Compute grouping expressions
    grouping_keys = []
    v_gexprs = self.compile_exprs(ctx, self.op.group_exprs, v_df)
    for i, e in enumerate(v_gexprs):
      grouping_keys.append(ctx.new_var("_gexpr"))
      assignments.append((grouping_keys[-1], e))
      #ctx.add_line("{df}['{key}'] = {e}",
      #  df=v_df,
      #  key=grouping_keys[-1],
      #  e=e)




    # Pre-process df to compute project expressions and 
    # exprs as arguments to agg functions
    #
    #
    # Expressions of this form are split in 3 steps
    #   sum(a+b) + count(c*2) as alias
    # 
    # 1. eval inputs to agg funcs
    #   df[i1] = df[a]+df[b]
    #   df[i2] = df[c]*2
    #
    # 2. replace agg func args and perform groupby!
    #   df2 = df.agg(a1=(i1, sum), a2=(i2, count))
    #
    # 3. expressions over aggfuncs
    #   df[alias] = df2[a1] + df2[a2]
    #
    postprocess_exprs = []
    agg_kwargs = []
    cols_to_remove = [] 
    for a, e in zip(self.op.aliases, self.op.project_exprs):
      if self.can_inline(e):
        if e.is_type(Attr):
          agg_kwargs.append("{a}=('{col}', 'first')".format(a=a, col=e.aname))
        else:
          tmpname = ctx.new_var("_agg_tmp")
          assignments.append((tmpname, self.compile_expr_inline(ctx, e, v_df)))
          agg_kwargs.append("{a}=('{col}', 'first')".format(a=a, col=tmpname))
      else:
        aggfuncs = e.collect([AggFunc])
        for agg in aggfuncs:
          if len(agg.args) != 1:
            raise Exception("agg.py: Only support 1-arg agg functions")

          if agg.args[0].is_type(Attr):
            col = agg.args[0].aname
          else:
            v_e = self.compile_expr(ctx, agg.args[0], v_df)
            col = ctx.new_var("_agg_tmp")
            assignments.append((col, v_e))

          v_agg_arg = ctx.new_var("_agg_arg")
          cols_to_remove.append(v_agg_arg)
          agg_kwargs.append("{key}=('{col}', '{func}')".format(key=v_agg_arg, col=col, func=agg.name))
          if agg.p:
            agg.replace(Attr(v_agg_arg, idx=len(agg_kwargs)-1))
          else:
            e = Attr(v_agg_arg, idx=len(agg_kwargs)-1)
        postprocess_exprs.append((a,e))

    tmpdf = (Context()
      .func("{df}.assign", [], dict(assignments), df=v_df)
      .compiler.compile_to_code())

    # apply expressions in arguments of agg functions
    kwargs = dict(
      outdf=self.v_outdf,
      tmpdf=tmpdf,
      grouping_keys=json.dumps(grouping_keys),
      agg_kwargs=", ".join(agg_kwargs),
      cols=json.dumps(cols_to_remove))
    with ctx.indent("{outdf} = ({tmpdf}", **kwargs):
      ctx.add_lines([
        ".groupby({grouping_keys})",
        ".agg({agg_kwargs}))"], **kwargs)
    
    #ctx.print(self.v_outdf)
    #ctx.add_line(".drop({cols}, axis=1)" ], **kwargs)

    # apply expressions over agg functions
    assignkwargs = dict([(a, self.compile_expr(ctx, e, self.v_outdf)) 
      for a, e in postprocess_exprs])

    tmpdf = Context().func("{df} = {df}.assign", [], assignkwargs, df=self.v_outdf).compiler.compile()
    kwargs['tmpdf'] = tmpdf
    #tmpdf = ctx.func("{df} = {df}.assign", [], assignkwargs, df=self.v_outdf)
    ctx.add_line("{outdf} = {tmpdf}.drop({cols}, axis=1)", **kwargs)
    #ctx.print(self.v_outdf)

    ctx.add_line("# End Groupby")
    ctx.add_line("")


class PandasGroupByTopTranslator(GroupByTopTranslator, PandasTranslator):

  def produce(self, ctx):
    if self.child_translator:
      self.child_translator.produce(ctx)
    else:
      self.consume(ctx) 

  def consume(self, ctx):
    ctx['df'] = self.bottom.v_outdf
    self.parent_translator.consume(ctx)

