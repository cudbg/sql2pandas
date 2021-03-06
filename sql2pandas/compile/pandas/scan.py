from ..translator import *
from ..scan import *
from .translator import *

class PandasSubQueryTranslator(SubQueryTranslator):
  pass


class PandasScanTranslator(ScanTranslator, PandasTranslator):
  def produce(self, ctx):
    v_df = ctx.new_var(self.op.alias)

    if v_df != self.op.tablename:
      ctx.declare(v_df, self.op.tablename)
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
