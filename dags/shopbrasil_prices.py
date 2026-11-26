
import logging
from contextlib import closing
from datetime import timedelta
from decimal import Decimal

import pendulum
import requests

from airflow.decorators import dag, task
from airflow.models import BaseOperator
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

CATALOG_URL = "https://fakestoreapi.com/products"
LOGGER = logging.getLogger(__name__)

def catalog_retry(context):
    LOGGER.warning("Catálogo indisponível!!\nO Airflow agendou nova tentativa! [Run: %s]", context["run_id"])

def catalog_failure(context):
    LOGGER.error("ALERTA SIMULADO!!!!\nTENTATIVAS ESGOTADAS de coleta do catálogo! [Run: %s]", context["run_id"])

def catalog_success(context):
    LOGGER.info("Coleta do catálogo concluída!! [Run: %s]", context["run_id"])

class ValidarProdutosOperator(BaseOperator):
    def __init__(self, *, products, **kwargs):
        super().__init__(**kwargs)
        self.products = products
        products >> self

    def execute(self, context):
        products = self.products.resolve(context)
        if not isinstance(products, list) or not products:
            raise ValueError("O catálogo precisa retornar uma lista não vazia.")

        for product in products:
            if not isinstance(product, dict):
                raise ValueError("Cada produto precisa ser um objeto JSON.")
            category = product.get("category")
            price = product.get("price")
            if not isinstance(category, str) or not category.strip():
                raise ValueError("Produto sem categoria válida.")
            if type(price) not in (int, float) or not Decimal(str(price)).is_finite() or price < 0:
                raise ValueError("Produto com preço inválido.")
        return products

@dag(
    dag_id = "shopbrasil_prices",
    description = "Calcula métricas diárias de preços do catálogo",
    start_date = pendulum.datetime(2026, 11, 25, tz="America/Sao_Paulo"),
    schedule = "0 6 * * *",
    catchup = False,
    max_active_runs = 1,
    is_paused_upon_creation = False,
    tags = ["shopbrasil", "etl"],
)
def shopbrasil_prices():
    @task(
        retries = 3,
        retry_delay = timedelta(seconds=30),
        retry_exponential_backoff = True,
        max_retry_delay = timedelta(minutes=5),
        on_retry_callback = catalog_retry,
        on_failure_callback = catalog_failure,
        on_success_callback = catalog_success,
    )
    def buscar_produtos():
        try:
            response = requests.get(CATALOG_URL, timeout=20, allow_redirects=False)
            if response.status_code != 200:
                response.raise_for_status()
                raise ValueError(f"Resposta inesperada da API: HTTP {response.status_code}")
            products = response.json()
        except (requests.RequestException, ValueError):
            LOGGER.exception("Erro ao consultar catálogo...")
            raise

        # somente campos usados vão pro XCom.
        if not isinstance(products, list):
            return None
        return [
            {"category": product.get("category"), "price": product.get("price")}
            if isinstance(product, dict) else None
            for product in products
        ]

    @task
    def agrupar_por_categoria(products):
        prices_by_category = {}
        for product in products:
            category = product["category"].strip()
            prices_by_category.setdefault(category, []).append(product["price"])
        return [
            {"category": category, "prices": prices}
            for category, prices in sorted(prices_by_category.items())
        ]

    @task(pool="ecommerce_pool")
    def calcular_metricas(category_group):
        prices = [Decimal(str(price)) for price in category_group["prices"]]
        return {
            "category": category_group["category"],
            "avg_price": str(sum(prices) / len(prices)),
            "min_price": str(min(prices)),
            "max_price": str(max(prices)),
            "product_count": len(prices),
        }

    @task
    def gravar_resultados(metrics):
        snapshot = [
            (row["category"], row["avg_price"], row["min_price"], row["max_price"], row["product_count"])
            for row in metrics
        ]
        if not snapshot:
            raise ValueError("A publicação exige pelo menos uma categoria.")

        context = get_current_context()
        history = [(context["run_id"], context["logical_date"], *row) for row in snapshot]

        hook = PostgresHook(postgres_conn_id="shopbrasil_analytics")
        with closing(hook.get_conn()) as connection:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute("DELETE FROM price_snapshot")
                    cursor.executemany(
                        """
                        INSERT INTO price_snapshot
                            (category, avg_price, min_price, max_price, product_count)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        snapshot,
                    )
                    cursor.execute("DELETE FROM price_history WHERE run_id = %s", (context["run_id"],))
                    cursor.executemany(
                        """
                        INSERT INTO price_history
                            (run_id, logical_date, category, avg_price, min_price, max_price, product_count)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        history,
                    )

        LOGGER.info("%s categorias persistidas no PostgreSQL!", len(snapshot))

    with TaskGroup("ingestion"):
        products = buscar_produtos()
        validated = ValidarProdutosOperator(task_id="validar_produtos", products=products)

    with TaskGroup("analysis"):
        groups = agrupar_por_categoria(validated.output)
        metrics = calcular_metricas.expand(category_group=groups)
        gravar_resultados(metrics)


dag = shopbrasil_prices()
