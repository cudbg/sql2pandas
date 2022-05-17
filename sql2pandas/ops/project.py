from ..baseops import *
from ..exprs import *
from ..db import Database
from ..schema import *
from ..tuples import *
from ..util import cache, OBTuple
from itertools import chain

########################################################
#
# Projection Operator
#
########################################################

class Project(UnaryOp):
  def __init__(self, c, exprs, aliases=[]):
    """
    @p            parent operator
    @exprs        list of Expr objects
    @aliases      name of the fields defined by the above exprs
    """
    super(Project, self).__init__(c)
    self.exprs = exprs
    self.aliases = list(aliases) or []

  def init_schema(self):
    """
    Figure out the alias and type of each expression in the target list
    and use it to set my schema
    """
    self.schema = Schema([])
    for alias, expr in zip(self.aliases, self.exprs):
      typ = expr.get_type()
      self.schema.attrs.append(Attr(alias, typ))
    return self.schema

  def __str__(self):
    args = ", ".join(["%s AS %s" % (e, a) 
      for (e, a) in  zip(self.exprs, self.aliases)])
    return "Project(%s)" % args





