from ...ops import *
from ..pipeline import *
from .translator import *
from .hashjoin import *
from .thetajoin import *
from .agg import *
from .project import *
from .scan import *
from .orderby import *
from .where import *
from .limit import *
from .root import *

class PandasPipeline(Pipeline):

  def produce(self, ctx):
    self[-1].produce(ctx)


class PandasPipelines(Pipelines):
  def __init__(self, ast):
    super(PandasPipelines, self).__init__(ast, PandasPipeline)

  def create_bottom(self, op, *args):
    if op.is_type(GroupBy):
      return PandasGroupByBottomTranslator(op, *args)
    if op.is_type(OrderBy):
      return PandasOrderByBottomTranslator(op, *args)
    raise Exception("No Bottom Translator for %s" % op)

  def create_top(self, op, *args):
    if op.is_type(GroupBy):
      return PandasGroupByTopTranslator(op, *args)
    if op.is_type(OrderBy):
      return PandasOrderByTopTranslator(op, *args)
    raise Exception("No Top Translator for %s" % op)

  def create_left(self, op, *args):
    if op.is_type(HashJoin):
      return PandasHashJoinLeftTranslator(op, *args)
    if op.is_type(ThetaJoin):
      return PandasThetaJoinLeftTranslator(op, *args)
    raise Exception("No Left Translator for %s" % op)


  def create_right(self, op, *args):
    if op.is_type(HashJoin):
      return PandasHashJoinRightTranslator(op, *args)
    if op.is_type(ThetaJoin):
      return PandasThetaJoinRightTranslator(op, *args)
    raise Exception("No Right Translator for %s" % op)


  def create_normal(self, op, *args):
    translators = [
        (Project, PandasProjectTranslator),
        (Limit, PandasLimitTranslator),
        (Yield, PandasYieldTranslator),
        (Print, PandasPrintTranslator),
        (Collect, PandasCollectTranslator),
        (SubQuerySource, PandasSubQueryTranslator),
        (Scan, PandasScanTranslator),
        (DummyScan, PandasDummyScanTranslator),
        (Filter, PandasFilterTranslator)
    ]

    for opklass, tklass in translators:
      if op.is_type(opklass):
        return tklass(op, *args)

    raise Exception("No Translator for %s" % op)
