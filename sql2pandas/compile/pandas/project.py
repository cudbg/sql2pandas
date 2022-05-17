from ..project import *
from ..translator import *
from .translator import *

class PandasProjectTranslator(ProjectTranslator, PandasTranslator):

  def produce(self, ctx):
    """
    Setup output tuple variable and generate code to initialize it 
    as an empty tuple with the correct schema..  

    There is a special case when if there is no child operator, such as
    
            SELECT 1
            
    where produce should pretend it is an access method that emits a 
    single empty tuple to its own consume method.
    """

    if self.op.c == None:
      # TODO: create new dataframe with no rows and correct schema??
      ctx['df'] = ctx.new_var("df")
      self.consume(ctx)
      return

    ctx.request_vars(dict(df=None))
    self.child_translator.produce(ctx)

  def consume(self, ctx):
    self.v_in = ctx['df']
    ctx.pop_vars()

    # generate expr inlined statements
    # df.assign(attr1=expr1, attr2=expr2, ...)

    v_exprs = self.compile_exprs(ctx, self.op.exprs, self.v_in) 
    aliases = self.op.aliases

    pairs = ["%s=%s" % (a,e) 
        for a, e in zip(aliases, v_exprs)]
    ctx.add_line("{df} = {df}.assign({kwargs})",
      kwargs=",".join(pairs),
      df=self.v_in
    )

    ctx['df'] = self.v_in
    self.parent_translator.consume(ctx)

