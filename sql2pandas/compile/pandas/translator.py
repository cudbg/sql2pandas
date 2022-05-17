"""
Implements expression compilation helpers used by the rest of the translators
"""
from ..translator import *

class PandasTranslator(Translator):
  op_sql2pandas = {
      "=": "==",
      "<>": "!=",
      "and": "&",
      "or": "|",
      "not": "~"
  }




  def can_inline(self, e):
    return len(e.collect([AggFunc])) == 0

  def compile_expr(self, ctx, expr, *args):
    """
    Compiles code to evaluate expression, and saves it in a new variable
    @return var name containing expression result
    """
    if self.can_inline(expr):
      return self.compile_expr_inline(ctx, expr, *args)

    funcs = [
        (Attr, self.attr),
        (Between, self.between),
        (Paren, self.paren),
        (AggFunc, self.agg_func),
        (ScalarFunc, self.scalar_func),
        (Literal, self.literal),
        (Expr, self.expr)
    ]

    for opklass, f in funcs:
      if isinstance(expr, opklass): 
        return f(ctx, expr, *args)

    raise Exception("No Translator for Expr %s" % expr)

  def compile_expr_inline(self, ctx, expr, *args):
    """
    Expressions that don't contain agg functions can be directly inlined.

    For the following expression:
           +
          / \
        a    1

    compile_expr will typically generate "v0 = a + 1" and return "v0"
    this method directly returns "a+1" so it can be inlined.

    @return compiled expression that can be inlined by the caller
    """
    funcs = [
        (Attr, self.attr_inline),
        (Between, self.between_inline),
        (Paren, self.paren_inline),
        (ScalarFunc, self.scalar_func_inline),
        (Literal, self.literal_inline),
        (Expr, self.expr_inline)
    ]

    for opklass, f in funcs:
      if isinstance(expr, opklass): 
        return f(ctx, expr, *args)
    raise Exception("No Translator for Expr %s" % expr)


  def compile_exprs(self, ctx, exprs, v_in, *args):
    """
    @return [varname,] list of expression results
    """
    v_outs = []
    for e in exprs:
      v_outs.append(self.compile_expr(ctx, e, v_in, *args))
    return v_outs

  def compile_new_tuple(self, ctx, schema, varname=None):
    """
    Helper function to initialize a new tuple with @schema
    @return var name that references the new tuple
    """
    v_out = ctx.new_var(varname or "tmp_row")
    ctx.set(v_out, "dict()")
    return v_out
    
  #
  # The following are helper functions for compiling Expr objects
  # 


  def expr(self, ctx, e, v_in):
    """
    @ctx compiler context
    @e expression
    @v_in variable name of input tuple

    Assigns a new variable to the result of @e and returns it
    """
    v_out = ctx.new_var("e_bi_out")

    # releasestart 3
    # # A3: implement me
    # #     make sure to support unary and binary expressions
    # #     a unary expression is when e.r is None
    # raise Exception("Not Implemented")
    # releaseend
    # solstart 3
    v_l = ctx.new_var("expr")
    v_r = ctx.new_var("expr")
    op = PandasTranslator.op_sql2pandas.get(e.op.strip().lower(), e.op)

    v_l = self.compile_expr(ctx, e.l, v_in)
    if e.r:
      v_r = self.compile_expr(ctx, e.r, v_in)

    # write expression result to output variable
    if e.r:
      ctx.set(v_out, "({l}) {op} ({r})", l=v_l, op=op, r=v_r)
    else:
      ctx.set(v_out, "{op}({l})", op=op, l=v_l)

    return v_out

  def expr_inline(self, ctx, e, v_in):
    """
    @return inlined compiled expression
    """
    op = PandasTranslator.op_sql2pandas.get(e.op.strip().lower(), e.op)
    l = self.compile_expr_inline(ctx, e.l, v_in)

    if e.r:
      r = self.compile_expr_inline(ctx, e.r, v_in)
      return "(%s) %s (%s)" % (l, op, r)
    return "%s(%s)" % (op, l)

  def paren(self, ctx, e, v_in):
    return self.compile_expr(ctx, e.c, v_in)

  def paren_inline(self, ctx, e, v_in):
    return "(%)" % self.compile_expr_inline(ctx, e, v_in)

  def between(self, ctx, e, v_in):
    v_out = ctx.new_var("e_btwn_out")
    v_e = self.compile_expr(ctx, e.expr, v_in)
    v_l = self.compile_expr(ctx, e.lower, v_in)
    v_u = self.compile_expr(ctx, e.upper, v_in)
    ctx.set(v_out, "{e}.between({l}, {u})",
      e=v_e,
      l=v_l, u=v_u)
    return v_out

  def between_inline(self, ctx, e, v_in):
    v_e = self.compile_expr_inline(ctx, e.expr, v_in)
    v_l = self.compile_expr_inline(ctx, e.lower, v_in)
    v_u = self.compile_expr_inline(ctx, e.upper, v_in)
    return "{e}.between({l}, {u})".format(
      e=v_e,
      l=v_l,
      u=v_u)

  def agg_func(self, ctx, e, v_in):
    """
    This is "sloppy" compilation because it still relies on 
    the UDFRegistry and calls the UDF in interpreted mode.

    See UDFTranslator to be able to directly generate code for UDFs.
    """
    if len(e.args) != 1:
      raise Exception("PandasTranslator doesn't support non-unary agg funcs")


    v_out = ctx.new_var("e_agg_out")
    v_expr = self.compile_expr(ctx, e.args[0], v_in)
    # TODO: support native number operations of the form
    #       Series.sum()/Series.min()/etc
    return "{name}({args})".format(
        name=e.name,
        args=v_expr)

  def scalar_func(self, ctx, e, v_in):
    v_out = ctx.new_var("e_scalar_out")

    if len(e.args) == 1:
      v_arg = self.compile_expr(ctx, e.args[0], v_in)
      ctx.set(v_out, "{arg}.apply('{func}')",
          arg=v_arg,
          func=e.name)
    else:
      vlist = [self.compile_expr(ctx, arg, v_in) for arg in e.args]
      vlist = ", ".join(vlist)
      ctx.set(v_out, "{func}({args})",
          func=e.name, arg=vlist)

    #line = "%s = UDFRegistry.registry()['%s'](%s)" % (v_out, e.name, vlist)
    #ctx.add_line(line)
    return v_out

  def scalar_func_inline(self, ctx, e, v_in):
    if len(e.args) == 1:
      return "{arg}.apply('{func}')".format(
        arg=self.compile_expr(ctx, e.args[0], v_in),
        func=e.name)
    else:
      vs = ["(%s)" % self.compile_expr_inline(ctx, arg, v_in) for arg in e.args]
      return "{func}({args})".format(
        func=e.name,
        args=vs)
    #return "UDFRegistry.registry()['%s'](%s)" % (e.name, ", ".join(vs))

  def literal(self, ctx, e, v_in):
    return self.literal_inline(ctx, e, v_in)

  def literal_inline(self, ctx, e, v_in):
    return str(e)

  def attr(self, ctx, e, v_in):
    e = self.attr_inline(ctx, e, v_in)
    return e

  def attr_inline(self, ctx, e, v_in):
    return "{df}.iloc[:,{i}]".format(
        df=v_in,
        i=e.idx
    )
    return "{df}['{aname}']".format(
      df=v_in,
      aname=e.aname)


class PandasRightTranslator(RightTranslator, PandasTranslator):
  pass
