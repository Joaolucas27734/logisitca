# services/shopify.py

import requests
from typing import Optional
from services.config import get_secret

# --------------------------------------------------
# SESSÃO HTTP REUTILIZÁVEL
# --------------------------------------------------
_session: Optional[requests.Session] = None


def _get_config():
    """
    Carrega configuração da Shopify somente quando necessário (lazy).
    """
    shop_name = get_secret(["shopify", "shop_name"])
    access_token = get_secret(["shopify", "access_token"])
    api_version = get_secret(["shopify", "api_version"], "2024-10")

    if not shop_name or not access_token:
        raise RuntimeError(
            "Shopify não configurada. "
            "Defina [shopify] em ENV ou st.secrets."
        )

    base_url = f"https://{shop_name}/admin/api/{api_version}"

    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    return base_url, headers


def _get_session() -> requests.Session:
    """
    Retorna sessão HTTP reutilizável com headers da Shopify.
    """
    global _session

    if _session is None:
        base_url, headers = _get_config()
        session = requests.Session()
        session.headers.update(headers)
        _session = session

    return _session


# ==================================================
# 🔍 BUSCAR PEDIDO PELO NÚMERO VISÍVEL
# ==================================================
def get_order_by_number(order_number: str) -> Optional[dict]:
    if not order_number:
        return None

    session = _get_session()
    base_url, _ = _get_config()

    url = f"{base_url}/orders.json"
    params = {
        "status": "any",
        "limit": 5,
        "name": f"#{order_number}",
    }

    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()

    orders = resp.json().get("orders", [])
    if not orders:
        return None

    order = orders[0]

    if str(order.get("name", "")).replace("#", "") != str(order_number):
        return None

    return order


# ==================================================
# 🚚 CRIAR FULFILLMENT
# ==================================================
def create_fulfillment(
    order_id: int,
    tracking_number: Optional[str] = None,
    tracking_company: str = "Correios",
    notify_customer: bool = True,
) -> dict:

    session = _get_session()
    base_url, _ = _get_config()

    # 1️⃣ Buscar fulfillment_orders
    f_orders_url = f"{base_url}/orders/{order_id}/fulfillment_orders.json"
    resp = session.get(f_orders_url, timeout=30)
    resp.raise_for_status()

    fulfillment_orders = resp.json().get("fulfillment_orders", [])
    if not fulfillment_orders:
        raise RuntimeError(
            f"Nenhum fulfillment_order encontrado para o pedido {order_id}"
        )

    # 2️⃣ Criar fulfillment
    for f_order in fulfillment_orders:
        fulfillment_order_id = f_order["id"]

        payload = {
            "fulfillment": {
                "line_items_by_fulfillment_order": [
                    {"fulfillment_order_id": fulfillment_order_id}
                ],
                "notify_customer": notify_customer,
            }
        }

        if tracking_number:
            payload["fulfillment"]["tracking_info"] = {
                "number": tracking_number,
                "company": tracking_company,
            }

        url = f"{base_url}/fulfillments.json"
        result = session.post(url, json=payload, timeout=30)

        # 3️⃣ Fallback 422 (DSers / location)
        if result.status_code == 422:
            location_id = f_order.get("assigned_location_id")
            if location_id:
                move_url = (
                    f"{base_url}/fulfillment_orders/"
                    f"{fulfillment_order_id}/move.json"
                )
                move_payload = {
                    "fulfillment_order": {"new_location_id": location_id}
                }

                move_resp = session.post(
                    move_url, json=move_payload, timeout=30
                )

                if move_resp.status_code in (200, 201):
                    result = session.post(url, json=payload, timeout=30)

        if result.status_code in (200, 201):
            return result.json()

        raise RuntimeError(
            f"Erro ao criar fulfillment "
            f"(status {result.status_code}): {result.text}"
        )

    raise RuntimeError(
        f"Não foi possível criar fulfillment para o pedido {order_id}"
    )
