import json
from ...context import Context
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
    #
    # The only special case is if all expressions are Attr references, then we can inline everything:
    if not all([e.is_type(Attr) for e in self.op.order_exprs]):
      raise Exception("Order by should only be on computed attributes, not expressions")

    ordering = [e.aname for e in self.op.order_exprs]
    ascending = [int(ad == "asc") for ad in self.bottom.op.ascdescs]

    kwargs = dict(outdf=v_outdf, df=v_in, order=json.dumps(ordering), asc=json.dumps(ascending))
    with ctx.indent("{outdf} = ({df}.sort_values({order}, ascending={asc})", **kwargs):
      ctx.add_line(".drop({order}, axis=1))", **kwargs)
    #ctx.add_line("{outdf} = {df}.sort_values({ordering}, ascending={ascending})",

    #else: 
    #  ascdescs = self.bottom.op.ascdescs
    #  aliases = []
    #  kwargs = {}
    #  for i, e in enumerate(self.compile_exprs(ctx, self.op.order_exprs, v_in)):
    #    aliases.append(ctx.new_var("_orderkey_"))
    #    e = "-"+e if ascdescs[i] != "asc" else e
    #    kwargs[aliases[-1]] = e

    #  # create tmp columns
    #  tmpdf = (Context()
    #      .func("{df}.assign", [], kwargs, df=v_in)
    #      .compiler.compile())
    #  
    #  # call DataFrame's sort, then drop the tmp columns
    #  kwargs = dict(outdf=v_outdf, tmpdf=tmpdf, keys=json.dumps(aliases))
    #  with ctx.indent("{outdf} = ({tmpdf}", **kwargs):
    #    ctx.add_lines([
    #      ".sort_values({keys})",
    #      ".drop({keys}, axis=1))"], **kwargs)

    ctx['df'] = v_outdf
    self.parent_translator.consume(ctx)

