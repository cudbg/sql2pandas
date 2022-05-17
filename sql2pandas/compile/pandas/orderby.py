import json
from ...ops import GroupBy
from ..translator import *
from ..orderby import *
from .translator import *

class PandasOrderByBottomTranslator(OrderByBottomTranslator, PandasTranslator):

  def produce(self, ctx):
    ctx.request_vars(dict(df=None))
    self.child_translator.produce(ctx)

  def consume(self, ctx):
    self.v_in = ctx['df']
    ctx.pop_vars()

    

class PandasOrderByTopTranslator(OrderByTopTranslator, PandasTranslator):

  def produce(self, ctx):
    if self.child_translator:
      self.child_translator.produce(ctx)
    else:
      self.consume(ctx)


  def consume(self, ctx):
    v_outdf = ctx.new_var("df")
    v_in = self.bottom.v_in

    # a query can order by the results of expressions.
    # we first evaluate each of the expressions into temporary
    # columns, sort by those columns, and then drop them
    aliases = []
    for i, e in enumerate(self.compile_exprs(ctx, self.op.order_exprs, v_in)):
      aliases.append(ctx.new_var("_orderkey_"))
      if self.bottom.op.ascdescs[i] != "asc":
        e = "-{e}".format(e=e)
      ctx.add_line("{df}['{alias}'] = {e}",
          df = v_in,
          alias=aliases[-1],
          e=e)
    
    # call DataFrame's sort, then drop the tmp columns
    ctx.add_line("{outdf} = {df}.sort_values({keys}).drop({keys}, axis=1)",
        outdf=v_outdf,
        df=v_in,
        keys=json.dumps(aliases))

    ctx['df'] = v_outdf
    self.parent_translator.consume(ctx)

