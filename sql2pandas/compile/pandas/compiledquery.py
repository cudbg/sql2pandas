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
      "import pandas as pd",
      "import io",
      "csv = '''a,b,c,d,e,f,g\n0,0,0,0,a,2,c\n1,1,1,0,b,4,d\n2,2,0,0,c,6,e\n3,3,1,0,d,8,cde\n4,4,0,0,abc,10,a\n5,0,1,0,cde,12,b\n6,1,0,0,a,14,c\n7,2,1,0,b,16,abc\n8,3,0,0,c,18,c\n9,4,1,0,d,20,d\n10,0,0,0,abc,22,e\n11,1,1,0,cde,24,cde\n12,2,0,0,a,26,a\n13,3,1,0,b,28,b\n14,4,0,0,c,30,c\n15,0,1,0,d,32,abc\n16,1,0,0,abc,34,c\n17,2,1,0,cde,36,d\n18,3,0,0,a,38,e\n19,4,1,0,b,40,cde'''",
      "data = pd.read_csv(io.StringIO(csv))"
    ])
    #with comp.indent("def %s(db):" % fname):
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
    execSymbTable = {}
    try:
      exec(self.code, globals(), execSymbTable)
    except Exception as e:
      import traceback; traceback.print_exc()
      raise e


