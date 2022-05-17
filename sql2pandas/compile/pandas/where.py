from ..translator import *
from ..where import *
from .translator import *


class PandasFilterTranslator(FilterTranslator, PandasTranslator):

  def consume(self, ctx):
    v_in = ctx['df']

    # compile the expression into an "if" statement
    v_cond = self.compile_expr(ctx, self.op.cond, v_in)
    ctx.add_line("{df} = {df}.loc[{cond}]", 
        df=v_in,
        cond=v_cond)
    self.parent_translator.consume(ctx)

