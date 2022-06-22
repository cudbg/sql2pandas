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
    ascending = ",".join([ad == "asc" and "True" or "False" for ad in self.bottom.op.ascdescs])
    ascending = "[{asc}]".format(asc=ascending)

    kwargs = dict(outdf=v_outdf, df=v_in, order=json.dumps(ordering), asc=ascending)
    with ctx.indent("{outdf} = ({df}", **kwargs):
      ctx.add_lines([
        ".sort_values({order}, ascending={asc})",
        ".drop({order}, axis=1))"
      ], **kwargs)


    ctx['df'] = v_outdf
    self.parent_translator.consume(ctx)

