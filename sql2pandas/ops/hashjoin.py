from ..baseops import *
from ..exprs import *
from ..db import Database
from ..schema import *
from ..tuples import *
from ..util import cache, OBTuple
from .join import *
from itertools import chain

   
class HashJoin(Join):
  """
  Hash Join
  """
  def __init__(self, l, r, join_attrs):
    """
    @l    left table of the join
    @r    right table of the join
    @join_attrs two attributes to join on, hash join checks if the 
                attribute values from the left and right tables are
                the same.  Suppose:
                
                  l = iowa, r = iowa, join_attrs = ["STORE", "storee"]

                then we return all pairs of (l, r) where 
                l.STORE = r.storee
    """
    super(HashJoin, self).__init__(l, r)
    self.join_attrs = join_attrs



