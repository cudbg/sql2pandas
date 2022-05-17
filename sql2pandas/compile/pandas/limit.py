from ...ops import GroupBy
from ..limit import *
from .translator import *


class PandasLimitTranslator(LimitTranslator, PandasTranslator):

  def produce(self, ctx):
    ctx.request_vars(dict(df=None))
    self.v_outdf = ctx.new_var("df")

    self.child_translator.produce(ctx)

  def consume(self, ctx):
    self.v_df = ctx['df']
    ctx.pop_vars()

    ctx.add_line("{outdf} = {df}[{s}:{e}]",
      outdf=self.v_outdf,
      df=self.v_df,
      s=self.op._offset,
      e=self.op._offset+self.op._limit)
    ctx['df'] = v_outdf
    self.parent_translator.consume(ctx)


