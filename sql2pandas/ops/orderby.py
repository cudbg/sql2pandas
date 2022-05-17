from ..baseops import *
from ..exprs import *
from ..db import Database
from ..schema import *
from ..tuples import *
from ..util import cache, OBTuple
from itertools import chain

class OrderBy(UnaryOp):
  """
  """

  def __init__(self, c, order_exprs, ascdescs):
    """
    @c            child operator
    @order_exprs  list of Expression objects
    @ascdescs     list of "asc" or "desc" strings, same length as @order_exprs 
    """
    super(OrderBy, self).__init__(c)
    self.order_exprs = order_exprs
    self.ascdescs = ascdescs

  def __str__(self):
    args = ", ".join(["%s %s" % (e, ad) 
      for (e, ad) in  zip(self.order_exprs, self.ascdescs)])
    return "ORDERBY(%s)" % args


