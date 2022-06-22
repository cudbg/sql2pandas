from ...ops import GroupBy
from ..translator import *
from ..root import *
from .translator import *

class PandasSinkTranslator(SinkTranslator, PandasTranslator):
  def produce(self, ctx):
    self.child_translator.produce(ctx)


class PandasYieldTranslator(YieldTranslator, PandasSinkTranslator):
  def consume(self, ctx):
    pass
    #ctx.add_line("yield {df}", df=ctx['df'])


class PandasCollectTranslator(PandasYieldTranslator, PandasSinkTranslator):
  def consume(self, ctx):
    ctx.print(ctx['df'])

class PandasPrintTranslator(PrintTranslator, PandasSinkTranslator):
  def consume(self, ctx):
    ctx.add_line("print({df})", df=ctx['df'])

class PandasToStringTranslator(ToStringTranslator, PandasSinkTranslator):
  def consume(self, ctx):
    ctx.add_line("{df}.to_string()", df=ctx['df'])




