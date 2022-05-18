from ..translator import *
from ..scan import *
from .translator import *

class PandasSubQueryTranslator(SubQueryTranslator):
  pass

  # def produce(self, ctx):
  #   ctx.request_vars(dict(df=None))
  #   self.child_translator.produce(ctx)

  # def consume(self, ctx):
  #   self.v_in = ctx['df']
  #   ctx.pop_vars()

  #   v_out = ctx.new_var(self.op.alias)
  #   ctx.declare(v_out, self.v_in)
  #   ctx['df'] = v_out
  #   self.parent_translator.consume(ctx)



class PandasScanTranslator(ScanTranslator, PandasTranslator):
  def produce(self, ctx):
    v_df = ctx.new_var(self.op.alias)

    ctx.declare(v_df, "db['{n}']".format(n=self.op.tablename))
    ctx["df"] = v_df

    if self.child_translator:
      self.child_translator.produce(ctx)
    else:
      self.parent_translator.consume(ctx)


class PandasDummyScanTranslator(ScanTranslator, PandasTranslator):
  def produce(self, ctx):
    v_df = ctx.new_var("dummy_df")
    ctx["df"] = v_df

    if self.child_translator:
      self.child_translator.produce(ctx)
    else:
      self.parent_translator.consume(ctx)
