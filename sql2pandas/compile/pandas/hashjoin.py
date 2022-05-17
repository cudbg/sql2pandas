from ...ops import HashJoin
from ..hashjoin import *
from .translator import *


class PandasHashJoinLeftTranslator(HashJoinLeftTranslator, PandasTranslator):
  """
  The left translator scans the left child and populates the hash table
  """
  def produce(self, ctx):
    """
    Produce's job is to 
    1. allocate variable names and create hash table
    2. request the child operator's variable name for the current row
    3. ask the child to produce
    """

    ctx.request_vars(dict(df=None))
    self.child_translator.produce(ctx)


  def consume(self, ctx):
    """
    Given variable name for left row, compute left key and add a copy of the current row
    to the hash table
    """
    self.v_ldf = ctx['df']
    ctx.pop_vars()
    
    v_lkey = self.compile_expr(ctx, self.op.join_attrs[0], v_ldf)
    ctx.add_line("{df}['_joinkey'] = {lkey}",
        df=v_ldf,
        lkey=v_lkey)


class PandasHashJoinRightTranslator(HashJoinRightTranslator, PandasRightTranslator):
  """
  The right translator scans the right child, and probes the hash table
  """

  def produce(self, ctx):
    """
    Allocates intermediate join tuple and asks the child to produce tuples (for the probe)
    """
    self.v_outdf = ctx.new_var("df")
    ctx.request_vars(dict(df=None))
    self.child_translator.produce(ctx)


  def consume(self, ctx):
    """
    Given variable name for right row, 
    1. compute right key, 
    2. probe hash table, 
    3. create intermediate row to pass to parent's consume

    Note that because the hash key may not be unique, it's good hygiene
    to check the join condition again when probing.
    """
    # reference to the left translator's hash table variable
    v_ldf = self.left.v_ldf
    v_rdf = ctx['df']
    ctx.pop_vars()

    v_rkey = self.compile_expr(ctx, self.op.join_attrs[1], v_rrow)
    ctx.add_line("{df}['_joinkey'] = {rkey}", df=v_rdf, rkey=v_rkey)
    ctx.add_line("{outdf} = {ldf}.join({rdf}, on='_joinkey'",
        outdf=self.v_outdf,
        ldf=v_ldf,
        rdf=v_rdf)
    ctx['df'] = self.v_outdf



