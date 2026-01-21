# services/shopify_sync.py

import time
import requests
import base64

from services.config import get_secret
from services.sheets import limpar_e_preparar_planilha, inserir_linhas_em_bloco
from datetime import datetime, timezone

def extrair_next_link(link_header):
    if not link_header:
        return None

    partes = link_header.split(",")
    for parte in partes:
        if 'rel="next"' in parte:
            return parte.split("<")[1].split(">")[0]

    return None

def sincronizar_pedidos_pagos(on_progress=None):
    shop_name = get_secret(["shopify", "shop_name"])
    token = get_secret(["shopify", "access_token"])
    api_version = get_secret(["shopify", "api_version"], "2024-10")

    if not shop_name or not token:
        raise RuntimeError("Shopify não configurada corretamente")

    headers = {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
    }

    base_url = f"https://{shop_name}/admin/api/{api_version}/orders.json"

    params = {
        "financial_status": "paid",
        "status": "any",
        "limit": 250,
        "created_at_min": "2023-01-01T00:00:00Z",
    }

    # 🔥 LIMPA UMA ÚNICA VEZ
    limpar_e_preparar_planilha()

    url = base_url
    pagina = 0
    total = 0
    todas_linhas = []

    while url:
        pagina += 1

        resp = requests.get(url, headers=headers, params=params, timeout=60)
        resp.raise_for_status()

        pedidos = resp.json().get("orders", [])
        if not pedidos:
            break

        linhas = [mapear_pedido_para_linha(o) for o in pedidos]
        todas_linhas.extend(linhas)

        total += len(linhas)

        if on_progress:
            on_progress(
                pagina=pagina,
                pedidos_pagina=len(linhas),
                total_pedidos=total
            )

        url = extrair_next_link(resp.headers.get("Link"))
        params = {}

        time.sleep(0.4)

        if not url:
            break

    def parse_data(valor):
        if not valor:
            return datetime.min.replace(tzinfo=timezone.utc)
        try:
            return datetime.fromisoformat(valor.replace("Z", "+00:00"))
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    
    # ✅ ORDENA POR DATA (MAIS RECENTES NO TOPO)
    todas_linhas.sort(
        key=lambda l: parse_data(l[0]),
        reverse=True
    )

    # ✅ ESCREVE UMA ÚNICA VEZ (SEM ESTOURAR QUOTA)
    inserir_linhas_em_bloco(todas_linhas)


def mapear_pedido_para_linha(order):
    data_pedido = order.get("processed_at") or order.get("created_at") or ""

    customer = order.get("customer") or {}

    cliente = "SEM NOME"
    nome = customer.get("first_name", "")
    sobrenome = customer.get("last_name", "")
    if nome or sobrenome:
        cliente = f"{nome} {sobrenome}".strip()

    email = customer.get("email", "")
    pedido = str(order.get("order_number", "")).replace(".0", "")
    produto = ""
    variante = ""
    quantidade = 0

    if order.get("line_items"):
        item = order["line_items"][0]
        produto = item.get("title", "")
        variante = item.get("variant_title", "")
        quantidade = item.get("quantity", 0)

    rastreio = ""
    link = ""

    if order.get("fulfillments"):
        codigos = []
        for f in order["fulfillments"]:
            if f.get("tracking_numbers"):
                codigos.extend(f["tracking_numbers"])

        rastreio = " | ".join(codigos)

        if rastreio:
            token_b64 = base64.b64encode(rastreio.encode()).decode()
            link = f"https://lojasportech.com/pages/rastreio?t={token_b64}"

    frete = ""
    shipping_lines = order.get("shipping_lines") or []
    if shipping_lines:
        frete = shipping_lines[0].get("title", "")

    shipping = order.get("shipping_address") or {}

    cidade = shipping.get("city", "")
    estado = shipping.get("province_code", "")


    return [
        data_pedido,
        cliente,
        produto,
        variante,
        quantidade,
        email,
        str(order.get("id", "")),
        pedido,
        "",              # ID
        rastreio,
        frete,
        link,
        "",              # OBS
        "",              # STATUS LOGÍSTICO
        "",              # DATA DO EVENTO
        "",              # HASH
        "",              # DATA ÚLTIMA LEITURA
        "",              # RISCO
        "",              # NOTIFICADO?
        cidade,
        estado,
        "",              # REENVIO?
        ""               # REEMBOLSO?
    ]
