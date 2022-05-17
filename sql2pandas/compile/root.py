from ..ops import GroupBy
from .translator import *

class SinkTranslator(Translator):
  def __init__(self, *args, **kwargs):
    super(SinkTranslator, self).__init__(*args, **kwargs)

  def produce(self, ctx):
    self.child_translator.produce(ctx)


class YieldTranslator(SinkTranslator):
  def __init__(self, *args, **kwargs):
    super(YieldTranslator, self).__init__(*args, **kwargs)


class CollectTranslator(SinkTranslator):
  def __init__(self, *args, **kwargs):
    super(CollectTranslator, self).__init__(*args, **kwargs)


class PrintTranslator(SinkTranslator):
  def __init__(self, *args, **kwargs):
    super(PrintTranslator, self).__init__(*args, **kwargs)




