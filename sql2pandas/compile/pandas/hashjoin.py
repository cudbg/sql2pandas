from dataclasses import dataclass
import json
from ...ops import HashJoin
from ..hashjoin import *
from .translator import *


class JoinExpr(object):

  def __init__(self, ctx, e, v_df, side, t):
    """
    side:  left or right
    """
    self.ctx = ctx
    self.e = e
    self.side = side

    self.key = "_%s_joinkey" % side
    self.kwargs = dict()
    self.drop = []

    if e.is_type(Attr):
      ctx.add_line("{df}.set_index('{name}')", df=v_df, name=e.aname)
      self.kwargs['%s_index' % side] = bool(1)
    else: 
      v_e = t.compile_expr(ctx, e, v_df)
      ctx.add_line("{df}['{lkey}'] = {e}",
        df=v_df,
        lkey=self.key,
        e=v_e)
      self.kwargs['%s_on' % side] = self.key
      self.drop.append(self.key)


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
    self.v_lkey = '_joinkey'
    ctx.pop_vars()
    ctx.add_line("")
    ctx.add_line("# Start Hash Join %s" % self.op)


    l_expr = self.op.join_attrs[0]
    self.join_expr = JoinExpr(ctx, l_expr, self.v_ldf, "left", self)


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
    v_rkey = "_joinkey"
    ctx.pop_vars()

    r_expr = self.op.join_attrs[1]
    self.join_expr = JoinExpr(ctx, r_expr, v_rdf, "right", self)
    kwargs = dict(suffixes=['_l', '_r'])
    kwargs.update(self.join_expr.kwargs)
    kwargs.update(self.left.join_expr.kwargs)
    drop = self.left.join_expr.drop + self.join_expr.drop
    ctx.add_line("{outdf} = {ldf}.merge({rdf}, **{kwargs})",
        outdf=self.v_outdf,
        ldf=v_ldf,
        rdf=v_rdf,
        kwargs=kwargs)
    if drop:
      ctx.add_line("{outdf} = {outdf}.drop({drop}, axis=1)",
          outdf=self.v_outdf,
          drop=json.dumps(drop))
    ctx['df'] = self.v_outdf


    ctx.add_line("# End Hash Join")
    ctx.add_line("")
    self.parent_translator.consume(ctx)


