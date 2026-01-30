# services/shopify.py

import time
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
        "Accept": "application/graphql-response+json",
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

    manual_location_id = get_secret(["shopify", "manual_location_id"])
    if not manual_location_id:
        raise RuntimeError("SHOPIFY manual_location_id não configurado.")

    # 1️⃣ Buscar fulfillment_orders
    f_orders_url = f"{base_url}/orders/{order_id}/fulfillment_orders.json"
    resp = session.get(f_orders_url, timeout=30)
    resp.raise_for_status()

    fulfillment_orders = resp.json().get("fulfillment_orders", [])

    # 🔒 usar SOMENTE fulfillment_orders OPEN
    fulfillment_orders = [
        f for f in fulfillment_orders
        if f.get("status") == "open"
    ]

    if not fulfillment_orders:
        raise RuntimeError(
            f"Nenhum fulfillment_order OPEN disponível para o pedido {order_id}"
        )

    # 2️⃣ Para cada fulfillment_order OPEN
    for f_order in fulfillment_orders:
        fulfillment_order_id = f_order["id"]

        # 🔁 MOVER SEMPRE para location manual
        move_url = (
            f"{base_url}/fulfillment_orders/"
            f"{fulfillment_order_id}/move.json"
        )

        move_payload = {
            "fulfillment_order": {
                "new_location_id": manual_location_id
            }
        }

        session.post(move_url, json=move_payload, timeout=30)

        # 3️⃣ Criar fulfillment
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

        if result.status_code in (200, 201):
            return result.json()

        raise RuntimeError(
            f"Erro ao criar fulfillment "
            f"(status {result.status_code}): {result.text}"
        )

    raise RuntimeError(
        f"Não foi possível criar fulfillment para o pedido {order_id}"
    )

# ==================================================
# ✏️ ATUALIZAR RASTREIO DE UM FULFILLMENT EXISTENTE
# ==================================================
def update_fulfillment_tracking(
    fulfillment_id: int,
    tracking_number: str,
    tracking_company: str = "Correios",
    notify_customer: bool = False,
):
    session = _get_session()
    base_url, _ = _get_config()

    url = f"{base_url}/fulfillments/{fulfillment_id}/update_tracking.json"

    payload = {
        "fulfillment": {
            "tracking_number": tracking_number,
            "tracking_company": tracking_company,
            "notify_customer": notify_customer,
        }
    }

    resp = session.post(url, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def encontrar_fulfillment_por_rastreio(order: dict, rastreio_atual: str):
    """
    Procura o fulfillment que já possui o rastreio atual.
    """
    if not order or not rastreio_atual:
        return None

    for f in order.get("fulfillments", []):
        tracking_numbers = f.get("tracking_numbers") or []
        if rastreio_atual in tracking_numbers:
            return f

    return None

# ==================================================
# ❌ CANCELAR FULFILLMENT (DSERS)
# ==================================================
def cancelar_fulfillment(fulfillment_id: int):
    session = _get_session()
    base_url, _ = _get_config()

    url = f"{base_url}/fulfillments/{fulfillment_id}/cancel.json"

    resp = session.post(url, timeout=30)
    resp.raise_for_status()
    return resp.json()

def get_fulfillment_orders(order_id: int):
    session = _get_session()
    base_url, _ = _get_config()

    url = f"{base_url}/orders/{order_id}/fulfillment_orders.json"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()

    return resp.json().get("fulfillment_orders", [])

def aplicar_rastreio_inteligente(order, rastreio):
    """
    Atualiza rastreio de forma segura:
    - Manual → atualiza
    - DSers / outro → cancela e recria
    """
    # tenta achar fulfillment pelo rastreio atual (se houver)
    fulfillments = order.get("fulfillments", [])

    fulfillment = next(
        (f for f in reversed(fulfillments) if f.get("status") != "cancelled"),
        None
    )

    if not fulfillment:
        raise RuntimeError("Nenhum fulfillment encontrado")

    if fulfillment.get("service") != "manual":
        # 🔁 DSers / automático
        cancelar_fulfillment(fulfillment["id"])

        fulfillment_orders = []
        for _ in range(5):
            fulfillment_orders = get_fulfillment_orders(order["id"])
            if fulfillment_orders:
                break
            time.sleep(1)

        fo = next(
            (f for f in fulfillment_orders if f["status"] == "open"),
            None
        )

        if not fo:
            raise RuntimeError("Nenhum fulfillment_order aberto")

        create_fulfillment(
            order_id=order["id"],
            tracking_number=rastreio,
            tracking_company="Correios",
            notify_customer=False
        )

    else:
        # ✏️ Manual → atualiza direto
        update_fulfillment_tracking(
            fulfillment_id=fulfillment["id"],
            tracking_number=rastreio,
            tracking_company="Correios",
            notify_customer=False
        )
