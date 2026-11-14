import ast, pathlib

src = pathlib.Path("gnosis_mesh.py").read_text()
tree = ast.parse(src)

def strip_docstrings(node):
    body = node.body
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str):
        node.body = body[1:] or [ast.Pass()]

class Scrubber(ast.NodeTransformer):
    def visit_FunctionDef(self, node):
        self.generic_visit(node); strip_docstrings(node); return node
    visit_AsyncFunctionDef = visit_FunctionDef
    def visit_ClassDef(self, node):
        self.generic_visit(node); strip_docstrings(node); return node
    def visit_Module(self, node):
        self.generic_visit(node); strip_docstrings(node); return node

out = ast.unparse(Scrubber().visit(tree))
lines = [l for l in out.splitlines() if l.strip()]
pathlib.Path("scripture.py").write_text("\n".join(lines) + "\n")
print("scripture.py written:", len(lines), "lines")