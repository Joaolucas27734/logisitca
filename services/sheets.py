# services/sheets.py

import gspread
import pandas as pd
import time
import os, json, base64
from services.config import get_secret
from google.oauth2.service_account import Credentials
from gspread.spreadsheet import Spreadsheet
from gspread.exceptions import APIError
from services.cache import cache
from gspread.utils import rowcol_to_a1

# ==================================================
# 🔒 CONSTANTES DE GOVERNANÇA
# ==================================================
STATUS_REENVIO_VALIDOS = {"EM ESPERA", "FEITO", "PROCESSADO"}

ABA_REENVIO = "Pedidos | Reenvio"

# ==================================================
# 🔐 AUTENTICAÇÃO GOOGLE
# ==================================================
def _get_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # ==================================================
    # 🔐 1️⃣ RENDER — JSON PURO
    # ==================================================
    raw_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    if raw_json:
        info = json.loads(raw_json)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        return gspread.authorize(creds)

    # ==================================================
    # 🔐 2️⃣ GITHUB ACTIONS — BASE64
    # ==================================================
    raw_b64 = os.getenv("GCP_SERVICE_ACCOUNT_BASE64")
    if raw_b64:
        info = json.loads(base64.b64decode(raw_b64).decode())
        info["private_key"] = info["private_key"].replace("\\n", "\n")
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        return gspread.authorize(creds)

    # ==================================================
    # 🔐 3️⃣ STREAMLIT CLOUD — st.secrets
    # ==================================================
    try:
        import streamlit as st

        info = dict(st.secrets["gcp_service_account"])
        info["private_key"] = info["private_key"].replace("\\n", "\n")
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        return gspread.authorize(creds)

    except Exception:
        pass

    # ==================================================
    # ❌ FALHA EXPLÍCITA
    # ==================================================
    raise RuntimeError(
        "Credenciais do Google não encontradas.\n"
        "Configure UMA das opções:\n"
        "- GCP_SERVICE_ACCOUNT_JSON (Render)\n"
        "- GCP_SERVICE_ACCOUNT_BASE64 (GitHub Actions)\n"
        "- st.secrets[gcp_service_account] (Streamlit)"
    )


# ==================================================
# 📄 ABERTURA DA PLANILHA
# ==================================================
def _open_spreadsheet():
    client = _get_client()

    spreadsheet_id = (
        os.getenv("SPREADSHEET_ID")
        or get_secret(["sheets", "spreadsheet_id"])
    )

    if not spreadsheet_id:
        raise RuntimeError("SPREADSHEET_ID não configurado.")

    spreadsheet_id = spreadsheet_id.strip()


    last_error = None

    for _ in range(3):
        try:
            return Spreadsheet(
                client.http_client,
                {"id": spreadsheet_id}
            )
        except APIError as e:
            last_error = e
            time.sleep(2)

    # se falhar todas as tentativas, explode de forma clara
    raise RuntimeError(
        f"Não foi possível abrir a planilha após tentativas. Erro: {last_error}"
    )

# ==================================================
# 📄 WORKSHEETS
# ==================================================
def _get_worksheet():
    """
    Aba principal: Pedidos Shopify
    """
    sh = _open_spreadsheet()
    try:
        return sh.worksheet("Pedidos | Ativo")
    except Exception:
        return sh.get_worksheet(0)


def _get_worksheet_by_name(nome_aba: str):
    """
    Retorna qualquer aba pelo nome (ex: Reenvio)
    """
    sh = _open_spreadsheet()
    return sh.worksheet(nome_aba)

# ==================================================
# 🔎 ÍNDICE EM MEMÓRIA — PEDIDOS (ESCALÁVEL)
# ==================================================
@cache(ttl=120)
def _index_pedidos():
    ws = _get_worksheet()
    values = ws.get_all_values()

    if not values:
        return {}

    header = [h.strip().upper() for h in values[0]]
    col_pedido = header.index("PEDIDO")

    index = {}
    for i, row in enumerate(values[1:], start=2):
        if len(row) > col_pedido:
            pedido = str(row[col_pedido]).strip()
            if pedido:
                index[pedido] = i

    return index

# ==================================================
# 🔎 ÍNDICE EM MEMÓRIA — REENVIOS
# ==================================================
@cache(ttl=120)
def _index_reenvios():
    ws = _get_worksheet_by_name(ABA_REENVIO)
    values = ws.get_all_values()

    if not values:
        return set()

    header = [h.strip().upper() for h in values[0]]
    col_pedido = header.index("PEDIDO")

    pedidos = set()
    for row in values[1:]:
        if len(row) > col_pedido:
            pedido = str(row[col_pedido]).strip()
            if pedido:
                pedidos.add(pedido)

    return pedidos

# ==================================================
# 📥 LEITURA — PEDIDOS ATIVO
# ==================================================
@cache(ttl=60)
def load_pedidos():
    ws = _get_worksheet()
    df = pd.DataFrame(ws.get_all_records())

    # 🔒 NORMALIZA COLUNAS COM SEGURANÇA TOTAL
    df.rename(columns=lambda c: str(c).strip(), inplace=True)

    return df

# ==================================================
# 📥 LEITURA — PEDIDOS FALHA
# ==================================================
@cache(ttl=60)
def load_falha():
    ws = _get_worksheet_by_name("Pedidos | Falha")
    df = pd.DataFrame(ws.get_all_records())
    df.rename(columns=lambda c: str(c).strip(), inplace=True)
    return df

# ==================================================
# 📥 LEITURA — PEDIDOS ENTREGUE
# ==================================================
@cache(ttl=60)
def load_entregue():
    ws = _get_worksheet_by_name("Pedidos | Entregue")
    df = pd.DataFrame(ws.get_all_records())
    df.rename(columns=lambda c: str(c).strip(), inplace=True)
    return df


# ==================================================
# 📥 LEITURA — REENVIOS
# ==================================================
@cache(ttl=60)
def load_reenvios():
    ws = _get_worksheet_by_name(ABA_REENVIO)
    values = ws.get_all_values()

    colunas_padrao = [
        "DATA",
        "CLIENTE",
        "PRODUTO",
        "VARIANTE",
        "QTD",
        "EMAIL",
        "SHOPIFY ORDER ID",
        "PEDIDO",
        "ID",
        "RASTREIO",
        "FRETE",
        "LINK",
        "OBSERVAÇÕES",
        "STATUS LOGÍSTICO",
        "DATA DO EVENTO",
        "HASH DO EVENTO",
        "DATA DA ÚLTIMA LEITURA",
        "RISCO LOGÍSTICO",
        "CIDADE",
        "ESTADO",
        "MOTIVO DO REENVIO",
        "REENVIO?",
        "PROCESSAR SHOPIFY?",
        "REEMBOLSO?"
    ]

    if not values or len(values) < 2:
        return pd.DataFrame(columns=colunas_padrao)

    headers = [str(c).strip().upper() for c in values[0]]
    rows = values[1:]

    df = pd.DataFrame(rows, columns=headers)

    for col in colunas_padrao:
        if col not in df.columns:
            df[col] = ""

    return df
    
# ==================================================
# 💾 PEDIDOS SHOPIFY — SALVAR ID (EM ESPERA → FEITO)
# ==================================================
def salvar_id(pedido: str, id_pedido: str):
    if not pedido or not id_pedido:
        raise ValueError("Pedido ou ID vazio.")

    ws = _get_worksheet()
    header = [h.strip().upper() for h in ws.row_values(1)]

    col_id = header.index("ID") + 1

    index = _index_pedidos()
    row = index.get(str(pedido))

    if not row:
        raise ValueError(f"Pedido {pedido} não encontrado na planilha.")

    ws.update_cell(row, col_id, id_pedido)
    cache.clear()
    
# ==================================================
# 💾 PEDIDOS SHOPIFY — SALVAR RASTREIO (FEITO → PROCESSADO)
# ==================================================
def salvar_rastreio(pedido: str, rastreio: str):
    if not pedido or not rastreio:
        raise ValueError("Pedido ou rastreio vazio.")

    ws = _get_worksheet()
    header = [h.strip().upper() for h in ws.row_values(1)]

    col_rastreio = header.index("RASTREIO") + 1

    index = _index_pedidos()
    row = index.get(str(pedido))

    if not row:
        raise ValueError(f"Pedido {pedido} não encontrado na planilha.")

    ws.update_cell(row, col_rastreio, rastreio)
    cache.clear()

# ==================================================
# 💾 PEDIDOS SHOPIFY — MARCAR REENVIO?
# ==================================================
def marcar_reenvio_pedido(pedido: str):
    if not pedido:
        raise ValueError("Pedido vazio.")

    ws = _get_worksheet()
    header = [h.strip().upper() for h in ws.row_values(1)]

    index = _index_pedidos()
    row = index.get(str(pedido))

    if not row:
        raise ValueError(f"Pedido {pedido} não encontrado.")

    col_reenvio = header.index("REENVIO?") + 1
    ws.update_cell(row, col_reenvio, "EXISTE REENVIO VINCULADO")
    cache.clear()

# ==================================================
# 📣 PEDIDOS SHOPIFY — MARCAR NOTIFICADO?
# ==================================================
def marcar_notificado(pedido: str):
    if not pedido:
        raise ValueError("Pedido vazio.")

    ws = _get_worksheet()
    header = [h.strip().upper() for h in ws.row_values(1)]

    if "NOTIFICADO?" not in header:
        raise ValueError("Coluna NOTIFICADO? não encontrada na planilha.")

    col_notificado = header.index("NOTIFICADO?") + 1

    index = _index_pedidos()
    row = index.get(str(pedido).strip())

    if not row:
        raise ValueError(f"Pedido {pedido} não encontrado na planilha.")

    ws.update_cell(row, col_notificado, "SIM")
    cache.clear()

# ==================================================
# 🔁 REENVIO — CRIAR NOVA LINHA (COM GOVERNANÇA)
# ==================================================
def criar_reenvio(dados_pedido: dict):
    ws = _get_worksheet_by_name(ABA_REENVIO)

    # --------------------------------------------------
    # 🔍 VERIFICA SE JÁ EXISTE REENVIO PARA ESSE PEDIDO
    # --------------------------------------------------

    pedido = str(dados_pedido.get("PEDIDO", "")).strip()

    if not pedido:
        raise ValueError("Pedido vazio ao criar reenvio.")

    reenvios_existentes = pedido in _index_reenvios()

    # --------------------------------------------------
    # 🧠 REGRA DE GOVERNANÇA
    # --------------------------------------------------
    # Primeiro reenvio → REENVIO? vazio
    # Reenvio de reenvio → REENVIO? = EM ESPERA
    if not reenvios_existentes:
        status_reenvio = ""
    else:
        status_reenvio = "EM ESPERA"

    # --------------------------------------------------
    # ➕ CRIA NOVA LINHA
    # --------------------------------------------------
    nova_linha = [
        pd.Timestamp.now().strftime("%Y-%m-%d"),  # DATA
        dados_pedido.get("CLIENTE", ""),
        dados_pedido.get("PRODUTO", ""),
        dados_pedido.get("VARIANTE", ""),
        str(dados_pedido.get("QTD") or ""),
        dados_pedido.get("EMAIL", ""),
        str(dados_pedido.get("SHOPIFY ORDER ID", "")).strip(),
        pedido,
        "",                     # ID
        "",                     # RASTREIO
        dados_pedido.get("FRETE", ""),
        "",                     # LINK
        dados_pedido.get("OBSERVAÇÕES", ""),
        "",                     # STATUS LOGÍSTICO
        "",                     # DATA DO EVENTO
        "",                     # HASH DO EVENTO
        "",                     # DATA DA ÚLTIMA LEITURA
        "",                     # RISCO LOGÍSTICO
        dados_pedido.get("CIDADE", ""),
        dados_pedido.get("ESTADO", ""),
        dados_pedido.get("MOTIVO DO REENVIO", ""),
        status_reenvio,         # REENVIO?
        "",                     # PROCESSAR SHOPIFY?
        ""                      # REEMBOLSO?
    ]
    
    ws.append_row(nova_linha, value_input_option="USER_ENTERED")
    cache.clear()

# ==================================================
# 🔁 REENVIO — SALVAR ID
# ==================================================
def salvar_id_reenvio_por_linha(sheet_row: int, id_reenvio: str):
    if not id_reenvio:
        raise ValueError("ID vazio.")

    ws = _get_worksheet_by_name(ABA_REENVIO)
    header = [h.strip().upper() for h in ws.row_values(1)]

    col_id = header.index("ID") + 1
    ws.update_cell(sheet_row, col_id, id_reenvio)
    cache.clear()

# ==================================================
# 🔁 REENVIO — SALVAR RASTREIO
# ==================================================
def salvar_rastreio_reenvio_por_linha(sheet_row: int, rastreio: str):
    if not rastreio:
        raise ValueError("Rastreio vazio.")

    ws = _get_worksheet_by_name(ABA_REENVIO)
    header = [h.strip().upper() for h in ws.row_values(1)]

    col_rastreio = header.index("RASTREIO") + 1
    ws.update_cell(sheet_row, col_rastreio, rastreio)
    cache.clear()

# ==================================================
# 🔁 REENVIO — MARCAR REENVIO?
# ==================================================
def marcar_reenvio_reenvio_por_linha(sheet_row: int, status: str):
    status = status.strip().upper()
    if status not in STATUS_REENVIO_VALIDOS:
        raise ValueError("Status inválido.")

    ws = _get_worksheet_by_name(ABA_REENVIO)
    header = [h.strip().upper() for h in ws.row_values(1)]

    col_reenvio = header.index("REENVIO?") + 1
    ws.update_cell(sheet_row, col_reenvio, status)
    cache.clear()

# ==================================================
# 🔁 REENVIO — SALVAR PROCESSAR SHOPIFY?
# ==================================================
def salvar_processar_shopify_por_linha(sheet_row: int, valor: str):
    if valor not in {"SIM", "NÃO"}:
        raise ValueError("Valor inválido para PROCESSAR SHOPIFY?")

    ws = _get_worksheet_by_name(ABA_REENVIO)
    header = [h.strip().upper() for h in ws.row_values(1)]

    if "PROCESSAR SHOPIFY?" not in header:
        raise ValueError("Coluna PROCESSAR SHOPIFY? não encontrada.")

    col_shopify = header.index("PROCESSAR SHOPIFY?") + 1
    ws.update_cell(sheet_row, col_shopify, valor)
    cache.clear()


@cache(ttl=300)
def pedido_existe(pedido: str) -> bool:
    """
    Verifica se o pedido já existe usando índice em memória.
    Escala bem para 20k+ linhas.
    """
    if not pedido:
        return False

    index = _index_pedidos()
    return str(pedido).strip() in index

def pedido_existe_webhook(pedido: str) -> bool:
    """
    Verificação direta, SEM CACHE.
    SEGURA para FastAPI / Render / Webhook.
    """
    if not pedido:
        return False

    ws = _get_worksheet()
    values = ws.get_all_values()

    if not values:
        return False

    header = [h.strip().upper() for h in values[0]]
    if "PEDIDO" not in header:
        return False

    col_pedido = header.index("PEDIDO")

    for row in values[1:]:
        if len(row) > col_pedido:
            if str(row[col_pedido]).strip() == str(pedido):
                return True

    return False

def pedido_existe_por_numero(pedido: str) -> bool:
    """
    Verificação direta para webhook (SEM CACHE).
    Usa número do pedido como chave.
    """
    if not pedido:
        return False

    ws = _get_worksheet()
    values = ws.get_all_values()

    if not values:
        return False

    header = [h.strip().upper() for h in values[0]]
    if "PEDIDO" not in header:
        return False

    col_pedido = header.index("PEDIDO")

    for row in values[1:]:
        if len(row) > col_pedido:
            if str(row[col_pedido]).strip() == str(pedido):
                return True

    return False

def inserir_linha_logistica(ws, linha: list):
    """
    Insere nova linha logo abaixo do cabeçalho.
    Método seguro para volume médio.
    """
    if not linha:
        raise ValueError("Linha vazia.")

    ws.insert_row(linha, index=2, value_input_option="RAW")

def atualizar_linha_por_pedido(ws, pedido: str, dados: dict):
    """
    Atualiza colunas específicas de um pedido existente.
    """
    if not pedido or not dados:
        return

    header = [h.strip().upper() for h in ws.row_values(1)]
    index = _index_pedidos()
    row = index.get(str(pedido))

    if not row:
        return

    for campo, valor in dados.items():
        campo = campo.strip().upper()
        if campo in header:
            col = header.index(campo) + 1
            ws.update_cell(row, col, valor)

    cache.clear()

def limpar_e_preparar_planilha():
    ws = _get_worksheet()
    ws.clear()

    cabecalho = [[
        "DATA","CLIENTE","PRODUTO","VARIANTE","QTD","EMAIL","SHOPIFY ORDER ID",
        "PEDIDO","ID","RASTREIO","FRETE","LINK","OBSERVAÇÕES","STATUS LOGÍSTICO",
        "DATA DO EVENTO","HASH DO EVENTO","DATA DA ÚLTIMA LEITURA",
        "RISCO LOGÍSTICO","NOTIFICADO?",
        "CIDADE","ESTADO","REENVIO?","REEMBOLSO?"
    ]]

    num_colunas = len(cabecalho[0])
    ultima_coluna = rowcol_to_a1(1, num_colunas).split("1")[0]

    ws.update(f"A1:{ultima_coluna}1", cabecalho)

def inserir_linhas_em_bloco(linhas: list):
    if not linhas:
        return

    ws = _get_worksheet()

    start_row = 2
    end_row = start_row + len(linhas) - 1
    num_colunas = len(linhas[0])

    ultima_coluna = rowcol_to_a1(1, num_colunas).split("1")[0]

    ws.update(
        f"A{start_row}:{ultima_coluna}{end_row}",
        linhas,
        value_input_option="RAW"
    )
