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

    ctx.add_line("")
    ctx.add_line("# Start Thetajoin %s" % self.op)

    kwargs = dict(
      outdf=self.v_outdf,
      ldf=self.left.v_ldf,
      rdf=self.v_rdf,
      key=ctx.new_var("_joinkey"))
    kwargs['right'] = "{rdf}.assign({key}=0)".format(**kwargs)

    with ctx.indent("{outdf} = ({ldf}.assign({key}=0)", **kwargs):
      ctx.add_lines([
        ".merge({right}, on='{key}', how='outer')",
        ".drop('{key}', axis=1))"], 
        **kwargs)

    if str(self.op.cond) != "True":
      kwargs['cond'] = self.compile_expr(ctx, self.op.cond, self.v_outdf)
      ctx.add_line("{outdf} = {outdf}.loc[{cond}]", **kwargs)

    ctx.add_line("# End Thetajoin")
    ctx.add_line("")


    ctx['df'] = self.v_outdf
    self.parent_translator.consume(ctx)


