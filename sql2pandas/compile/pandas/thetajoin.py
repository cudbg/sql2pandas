from ...ops import ThetaJoin
from ..thetajoin import *
from .translator import *


class PandasThetaJoinLeftTranslator(ThetaJoinLeftTranslator, PandasTranslator):
  """
  Inner loop
  """
  def produce(self, ctx):
    ctx.request_vars(dict(df=None))
    self.child_translator.produce(ctx)

  def consume(self, ctx):
    self.v_ldf = ctx['df']
    ctx.pop_vars()

    self.v_joinkey = ctx.new_var("_joinkey")
    ctx.add_line("{df}['{key}'] = 0", df=self.v_ldf, key=self.v_joinkey)
    self.parent_translator.consume(ctx)


class PandasThetaJoinRightTranslator(ThetaJoinRightTranslator, PandasRightTranslator):
  """
  Outer loop
  """

  def produce(self, ctx):
    self.v_outdf = ctx.new_var("df")
    ctx.request_vars(dict(df=None))
    self.child_translator.produce(ctx)

  def consume(self, ctx):
    v_e = ctx.new_var("theta_cond")
    self.v_rdf = ctx['df']
    ctx.pop_vars()

    lines = [
      "{rdf}['{key}'] = 0",
      "{outdf} = {ldf}.merge({rdf}, on='{key}', how='outer')",
      "{outdf} = {outdf}.drop('{key}', axis=1)"
    ]
    ctx.add_lines(lines,
      outdf=self.v_outdf,
      ldf=self.left.v_ldf,
      rdf=self.v_rdf,
      key=self.left.v_joinkey)

    if str(self.op.cond) != "True":
      v_e = self.compile_expr(ctx, self.op.cond, self.v_outdf)
      ctx.add_line("{outdf} = {outdf}.loc[{cond}]",
        outdf=self.v_outdf,
        cond=v_e
      )

    ctx.add_line("")
    ctx['df'] = self.v_outdf
    self.parent_translator.consume(ctx)


