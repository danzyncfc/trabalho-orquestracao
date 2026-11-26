import ast
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock


def load_tasks():
    source = Path("dags/shopbrasil_prices.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    workflow = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                    and node.name == "shopbrasil_prices")
    functions = [node for node in workflow.body if isinstance(node, ast.FunctionDef)]
    for function in functions:
        function.decorator_list = []
    namespace = {
        "Decimal": Decimal,
        "LOGGER": Mock(),
        "CATALOG_URL": "https://fakestoreapi.com/products",
        "requests": SimpleNamespace(get=Mock(), RequestException=OSError),
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]), "<dag tasks>", "exec"), namespace)
    return namespace


class ShopBrasilTest(unittest.TestCase):
    def setUp(self):
        self.tasks = load_tasks()

    def test_groups_and_metrics_for_new_category(self):
        groups = self.tasks["agrupar_por_categoria"]([
            {"category": "eletrônicos", "price": 10},
            {"category": "eletrônicos", "price": 20},
            {"category": "nova", "price": 7},
        ])
        result = [self.tasks["calcular_metricas"](group) for group in groups]
        self.assertEqual(result, [
            {"category": "eletrônicos", "avg_price": "15", "min_price": "10",
             "max_price": "20", "product_count": 2},
            {"category": "nova", "avg_price": "7", "min_price": "7",
             "max_price": "7", "product_count": 1},
        ])

    def test_catalog_projects_the_required_fields_without_network(self):
        request = self.tasks["requests"].get
        request.return_value.status_code = 200
        request.return_value.json.return_value = [
            {"category": "livros", "price": 4, "title": "Título sintético"},
        ]
        self.assertEqual(self.tasks["buscar_produtos"](), [{"category": "livros", "price": 4}])
        request.assert_called_once_with("https://fakestoreapi.com/products", timeout=20,
                                        allow_redirects=False)

    def test_catalog_raises_transient_failure_to_the_scheduler(self):
        request = self.tasks["requests"].get
        request.return_value.status_code = 503
        request.return_value.raise_for_status.side_effect = OSError("HTTP 503")
        with self.assertRaisesRegex(OSError, "503"):
            self.tasks["buscar_produtos"]()
