# tracking/rastreamento.py

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import gspread
from gspread.exceptions import APIError
from gspread.utils import rowcol_to_a1
from webdriver_manager.chrome import ChromeDriverManager

from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
import threading

from datetime import datetime
from zoneinfo import ZoneInfo
import hashlib
import re

# ==================================================
# CONFIG
# ==================================================
TZ = ZoneInfo("America/Sao_Paulo")
BATCH_SIZE = 30
WAIT_SECONDS = 15
MAX_RETRIES = 5
BASE_BACKOFF = 2
MAX_WORKERS = 2
STALL_DIAS = 9 
ABAS_RASTREAVEIS = [
    "Pedidos | Ativo",
    "Pedidos | Reenvio",
]

SLA_FRETE = {
    "SEDEX": 7,            # 2 a 5 dias úteis
    "PROMOCIONAL": 15,     # 9 a 12 dias úteis
}

# ==================================================
# LOG
# ==================================================
def log(msg):
    print(msg, flush=True)


# ==================================================
# CONTROLE GLOBAL DE DRIVERS
# ==================================================
drivers_criados = []
drivers_lock = threading.Lock()
thread_local = threading.local()

def rodar_rastreamento_para_aba(nome_aba: str):
    global sheet, header
    global COL_LINK, COL_OBS, COL_STATUS_LOG
    global COL_DATA_EVENTO, COL_HASH, COL_ULTIMA_LEITURA, COL_RISCO, COL_FRETE

    log(f"\n🔄 Iniciando rastreamento da aba: {nome_aba}")

    sheet = client.open_by_key(
        "1WTEiRnm1OFxzn6ag1MfI8VnlQCbL8xwxY3LeanCsdxk"
    ).worksheet(nome_aba)

    header = [h.strip() for h in sheet.row_values(1)]

    def col(nome):
        return header.index(nome) + 1

    COL_LINK = col("LINK")
    COL_OBS = col("OBSERVAÇÕES")
    COL_STATUS_LOG = col("STATUS LOGÍSTICO")
    COL_DATA_EVENTO = col("DATA DO EVENTO")
    COL_HASH = col("HASH DO EVENTO")
    COL_ULTIMA_LEITURA = col("DATA DA ÚLTIMA LEITURA")
    COL_RISCO = col("RISCO LOGÍSTICO")
    COL_FRETE = col("FRETE")

    dados = sheet.get_all_values()
    linhas = dados[1:]

    log(f"📦 Total de linhas: {len(linhas)}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(processar_linha, idx, row)
            for idx, row in enumerate(linhas, start=2)
        ]

        for i, _ in enumerate(as_completed(futures), start=1):
            if i % BATCH_SIZE == 0:
                flush_updates()

    flush_updates()

# ==================================================
# SELENIUM FACTORY
# ==================================================
def create_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    wait = WebDriverWait(driver, WAIT_SECONDS)
    return driver, wait


def get_driver():
    if not hasattr(thread_local, "driver"):
        driver, wait = create_driver()
        thread_local.driver = driver
        thread_local.wait = wait

        with drivers_lock:
            drivers_criados.append(driver)

        log("🧩 Driver criado para thread")

    return thread_local.driver, thread_local.wait

# ==================================================
# GOOGLE SHEETS
# ==================================================

def get_gspread_client():
    import os, json, base64
    from oauth2client.service_account import ServiceAccountCredentials

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    creds_dict = json.loads(
        base64.b64decode(os.environ["GCP_SERVICE_ACCOUNT_BASE64"]).decode()
    )

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)


client = get_gspread_client()

# ==================================================
# REGRA DE NEGÓCIO — IMPORTAÇÃO
# ==================================================

IMPORTACAO_FALHA_FINAL = [
    "importação não autorizada",
    "pedido não autorizado",
    "devolução determinada pela autoridade competente",
    "devolução determinada pela autoridade",
    "objeto devolvido ao remetente",
    "pacote destruído",
    "objeto destruído",
    "falha ao limpar na importação",
    "retido pela alfândega",
]

# ==================================================
# HELPERS
# ==================================================
def get_text(parent, cls):
    try:
        return parent.find_element(By.CLASS_NAME, cls).text.strip()
    except Exception:
        return ""


def detectar_falha_importacao(texto_eventos: str) -> str:
    for termo in IMPORTACAO_FALHA_FINAL:
        if termo in texto_eventos:
            return termo
    return ""

def normalizar_frete(frete_raw: str) -> str:
    texto = (frete_raw or "").upper()

    if "SEDEX" in texto or "2 A 5" in texto:
        return "SEDEX"

    if "PROMOCIONAL" in texto or "9 A 12" in texto or "GRÁTIS" in texto:
        return "PROMOCIONAL"

    # fallback seguro
    return "PROMOCIONAL"


def normalizar_texto(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def gerar_hash_evento(status_log: str, data_evento: str, label: str, desc: str, local: str) -> str:
    """
    Hash muda se QUALQUER parte do último evento mudar.
    """
    payload = "|".join([
        normalizar_texto(status_log),
        normalizar_texto(data_evento),
        normalizar_texto(label),
        normalizar_texto(desc),
        normalizar_texto(local),
    ])
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def parse_data_evento(data_str: str):
    """
    Tenta converter a data do evento para datetime.
    Aceita comuns tipo:
    - 08/01/2026
    - 08/01/2026 10:12
    - 08-01-2026
    """
    s = (data_str or "").strip()
    if not s:
        return None

    # pega só o começo da data/hora se tiver lixo
    s = re.sub(r"\s+", " ", s)

    formatos = [
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d-%m-%Y",
        "%d-%m-%Y %H:%M",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
    ]

    for fmt in formatos:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue

    return None

def calcular_risco(
    status_log: str,
    data_evento_str: str,
    data_pedido_str: str,
    frete: str,
    dias_sem_atualizacao: int = STALL_DIAS
) -> str:

    status = (status_log or "").strip().upper()

    # 🚨 Estados críticos
    if status in {"FALHA", "ERRO"}:
        return "CRÍTICO"

    # 🟢 Estados finais
    if status in {"ENTREGUE", "AGUARDANDO RETIRADA"}:
        return "NORMAL"

    agora = datetime.now(TZ)

    # =========================
    # ⏰ ATRASO → DATA DO PEDIDO
    # =========================
    dt_pedido = parse_data_evento(data_pedido_str)
    if dt_pedido and dt_pedido.tzinfo is None:
        dt_pedido = dt_pedido.replace(tzinfo=TZ)

    sla = SLA_FRETE.get(frete, 12)

    if dt_pedido:
        dias_pedido = (agora - dt_pedido).days
        if dias_pedido > sla:
            return "ATRASADO"

    # =================================
    # ⏳ SEM ATUALIZAÇÃO → DATA DO EVENTO
    # =================================
    dt_evento = parse_data_evento(data_evento_str)
    if dt_evento and dt_evento.tzinfo is None:
        dt_evento = dt_evento.replace(tzinfo=TZ)

    if dt_evento:
        dias_evento = (agora - dt_evento).days
        if dias_evento >= dias_sem_atualizacao:
            return "SEM ATUALIZAÇÃO"

    return "NORMAL"

# ==================================================
# BUFFER DE ESCRITA
# ==================================================
updates = []
lock_updates = threading.Lock()

def add_update(row, col, value):
    cell = rowcol_to_a1(row, col)
    with lock_updates:
        updates.append({
            "range": f"{sheet.title}!{cell}",
            "values": [[value]]
        })

def flush_updates():
    global updates

    with lock_updates:
        if not updates:
            return

        body = {
            "valueInputOption": "RAW",
            "data": updates
        }
        batch = updates
        updates = []

    for tentativa in range(1, MAX_RETRIES + 1):
        try:
            sheet.spreadsheet.values_batch_update(body)
            log(f"📤 Batch enviado ({len(batch)} ranges)")
            return
        except APIError:
            wait_time = (BASE_BACKOFF ** tentativa) + random.uniform(0, 1)
            log(f"⚠️ Erro Sheets (tentativa {tentativa}) – aguardando {wait_time:.1f}s")
            time.sleep(wait_time)

    log("❌ Falha definitiva ao escrever no Sheets")


def deve_rastrear(status_salvo, obs_atual, link):
    status = (status_salvo or "").strip().upper()

    # ⛔ Status terminal REAL
    if status in {"ENTREGUE", "FALHA"}:
        return False, "status terminal"

    if not link or not link.startswith("http"):
        return False, "link inválido"

    return True, "rastrear"

def resolver_status_logistico(eventos):
    """
    REGRA-MÃE:
    1) Evento final em QUALQUER ponto do histórico → FALHA
    2) Senão → último evento decide
    """

    # texto completo do histórico
    texto_historico = " ".join(ev.text.lower() for ev in eventos)

    motivo_falha = detectar_falha_importacao(texto_historico)
    if motivo_falha:
        return "FALHA", motivo_falha

    # último evento
    ultimo = eventos[0].find_element(By.CLASS_NAME, "rptn-order-tracking-text")
    texto_ultimo = (ultimo.text or "").lower()

    if "entregue" in texto_ultimo:
        return "ENTREGUE", ""

    if any(p in texto_ultimo for p in [
        "aguardando retirada",
        "objeto disponível para retirada",
        "disponível para retirada",
        "aguardando retirada pelo destinatário",
    ]):
        return "AGUARDANDO RETIRADA", ""

    return "EM TRÂNSITO", ""

def processar_linha(idx, row):
    COL_DATA_PEDIDO = header.index("DATA") + 1 if "DATA" in header else None
    data_pedido = row[COL_DATA_PEDIDO - 1] if COL_DATA_PEDIDO and len(row) >= COL_DATA_PEDIDO else ""

    link = row[COL_LINK - 1] if len(row) >= COL_LINK else ""
    obs_atual = row[COL_OBS - 1] if len(row) >= COL_OBS else ""
    hash_salvo = row[COL_HASH - 1] if len(row) >= COL_HASH else ""
    status_salvo = row[COL_STATUS_LOG - 1] if len(row) >= COL_STATUS_LOG else ""
    data_evento_salva = row[COL_DATA_EVENTO - 1] if len(row) >= COL_DATA_EVENTO else ""
    frete_raw = row[COL_FRETE - 1] if len(row) >= COL_FRETE else ""
    frete = normalizar_frete(frete_raw)

    link = (link or "").strip()
    obs_atual = (obs_atual or "").strip().lower()

    agora_str = datetime.now(ZoneInfo("America/Sao_Paulo")).replace(microsecond=0).isoformat()

    log(f"➡️ Linha {idx} | Status atual: {status_salvo or '—'}")

    # ==================================================
    # DECISÃO CENTRALIZADA DE RASTREIO
    # ==================================================
    rastrear, motivo = deve_rastrear(status_salvo, obs_atual, link)

    if not rastrear:
        log(f"⏭️ Linha {idx} ignorada ({motivo})")

        risco_atual = calcular_risco(
            status_salvo,
            data_evento_salva,
            data_pedido,
            frete
        )

        add_update(idx, COL_RISCO, risco_atual)

        if motivo == "link inválido":
            add_update(idx, COL_OBS, "⚠️ Link inválido ou vazio")

        return


    # ✅ Sempre marca que o sistema olhou
    ultima_salva = row[COL_ULTIMA_LEITURA - 1] if len(row) >= COL_ULTIMA_LEITURA else ""

    add_update(idx, COL_ULTIMA_LEITURA, agora_str)
    driver, wait = get_driver()

    try:
        driver.get(link)

        wait.until(
            EC.any_of(
                EC.presence_of_element_located((By.CLASS_NAME, "rptn-order-tracking-event")),
                EC.presence_of_element_located((By.CLASS_NAME, "rptn-order-tracking-not-found"))
            )
        )

        eventos = driver.find_elements(By.CLASS_NAME, "rptn-order-tracking-event")

        if not eventos:
            add_update(idx, COL_STATUS_LOG, "ERRO")
            add_update(idx, COL_OBS, "❌ ERRO DE RASTREAMENTO — Nenhum evento encontrado")
            add_update(idx, COL_RISCO, "CRÍTICO")
            return

        status_novo, motivo_falha = resolver_status_logistico(eventos)

        ultimo = eventos[0].find_element(By.CLASS_NAME, "rptn-order-tracking-text")

        data = get_text(ultimo, "rptn-order-tracking-date")
        label = get_text(ultimo, "rptn-order-tracking-label")
        local = get_text(ultimo, "rptn-order-tracking-location")
        desc = get_text(ultimo, "rptn-order-tracking-description")

        # ✅ Hash (muda se data/status/texto mudar)
        hash_novo = gerar_hash_evento(status_novo, data, label, desc, local)

        # ✅ Risco baseado em:
        # - ATRASO → data do pedido
        # - SEM ATUALIZAÇÃO → data do evento
        risco_novo = calcular_risco(
            status_novo,
            data,
            data_pedido,
            frete
        )


        # ✅ Texto humano (OBSERVAÇÕES)
        if motivo_falha:
            texto_obs = " | ".join(p for p in [
                "🚨 EVENTO FINAL NO HISTÓRICO — PEDIDO NÃO SERÁ ENTREGUE",
                f"Motivo: {motivo_falha}",
                f"Último status exibido: {label}",
                f"Data: {data}",
                f"Local: {local}",
            ] if p)
        else:
            texto_obs = " | ".join(
                p for p in [
                    f"Data: {data}",
                    f"Status: {label}",
                    f"Local: {local}",
                    f"Descrição: {desc}",
                ] if p
            )

        # ==================================================
        # ✅ Regra central: só reage se hash mudou
        # ==================================================
        if (hash_salvo or "").strip() == (hash_novo or "").strip():
            # Não mudou: só atualiza risco (e última leitura já foi atualizada acima)
            add_update(idx, COL_RISCO, risco_novo)
            return

        # Mudou: grava tudo
        add_update(idx, COL_OBS, texto_obs)
        add_update(idx, COL_STATUS_LOG, status_novo)
        add_update(idx, COL_DATA_EVENTO, data)
        add_update(idx, COL_HASH, hash_novo)
        add_update(idx, COL_RISCO, risco_novo)

    except Exception as e:
        log(f"❌ Erro linha {idx}: {e}")

        add_update(idx, COL_STATUS_LOG, "ERRO")
        add_update(idx, COL_OBS, "❌ ERRO TÉCNICO — Falha ao consultar rastreio. Reprocessar manualmente.")

        # erro técnico também é considerado pedido travado
        add_update(idx, COL_RISCO, "CRÍTICO")

if __name__ == "__main__":
    for aba in ABAS_RASTREAVEIS:
        try:
            rodar_rastreamento_para_aba(aba)
        except Exception as e:
            log(f"❌ Erro ao rastrear aba {aba}: {e}")

    for driver in drivers_criados:
        try:
            driver.quit()
        except Exception:
            pass

    log("🏁 Rastreamento finalizado para todas as abas")
