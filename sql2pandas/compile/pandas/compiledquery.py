from collections import defaultdict
from ...context import *
from ...udfs import *
from ..compiledquery import *
from ..compiler import Compiler
from ...db import Database
from .pipeline import *

class PandasCompiledQuery(CompiledQuery):

  def create_pipelined_plan(self, plan):
    return PandasPipelines(plan)

  def compile_scans(self):
    db = Database.db()
    lines = []
    scans = self.plan.collect("Scan")
    d = defaultdict(list)
    for scan in scans:
      d[scan.tablename].append(scan.alias)

    for tablename, aliases in d.items():
      csv = db[tablename].to_pandas().to_csv(index=False)

      if tablename in aliases:
        aliases = [tablename] + [a for a in aliases if a != tablename]
      else:
        aliases = [tablename] + aliases

      lines.append("{var} = pd.read_csv(io.StringIO('''{csv}'''))".format(var=aliases[0], csv=csv))

      for alias in aliases[1:]:
        lines.append("{alias} = {tname}".format(alias=alias, tablename=tname))
    return lines




  def compile_to_func(self):
    """
    Wrap the compiled query code with a function definition.
    """
    # TODO: find table sources, write them as CSV strings loaded into
    # custom variables, and embed into output program
    # then change Source translator to reference the custom variables
    comp = Compiler()
    comp.add_lines([
      "import pandas as pd",
      "import io"] + 
      self.compile_scans() )
    lines = self.ctx.compiler.compile_to_lines()
    comp.add_lines(lines)

    return comp.compile()

  def print_code(self):
    code = self.compile_to_func()
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


