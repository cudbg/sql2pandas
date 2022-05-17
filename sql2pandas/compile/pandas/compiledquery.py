from ...context import *
from ...udfs import *
from ..compiledquery import *
from ..compiler import Compiler
from .pipeline import *

class PandasCompiledQuery(CompiledQuery):

  def create_pipelined_plan(self, plan):
    return PandasPipelines(plan)

  def compile_to_func(self, fname="f"):
    """
    Wrap the compiled query code with a function definition.
    """
    comp = Compiler()
    comp.add_lines([
      "import numpy as np",
      "import pandas as pd"
    ])
    with comp.indent("def %s(**db):" % fname):
      lines = self.ctx.compiler.compile_to_lines()
      comp.add_lines(lines)

    return comp.compile()

  def print_code(self, funcname="compiled_q"):
    code = self.compile_to_func(funcname)
    #code = """'''\n%s\n'''\n\n%s\n""" % (
    #    self.plan.pretty_print(), code)
    lines = code.split("\n")
    lines = ["%03d %s" % (i+1, l) for i, l in enumerate(lines)]
    print()
    print("\n".join(lines))
    print()


  def __call__(self, db=dict()):
    """
    db is a dict of tablename -> List<dict>
    """
    return self.f(db)


