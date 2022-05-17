from ..baseops import *

class Sink(UnaryOp):
  def init_schema(self):
    self.schema = self.c.schema
    return self.schema


class Yield(Sink):
  pass

class Collect(Sink):
  pass

class Print(Sink):
  pass


