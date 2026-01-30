# tracking/mover_encerrados.py

import gspread
import os
import json
import time
import base64
from oauth2client.service_account import ServiceAccountCredentials
from gspread.spreadsheet import Spreadsheet
from gspread.exceptions import APIError

# =============================
# GOOGLE SHEETS
# =============================
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

cred_b64 = os.environ["GCP_SERVICE_ACCOUNT_BASE64"]
creds_dict = json.loads(base64.b64decode(cred_b64).decode())
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)

client = gspread.authorize(creds)

SPREADSHEET_ID = os.environ["SPREADSHEET_ID"].strip()

# tenta abrir a planilha com retry
last_error = None
for _ in range(3):
    try:
        sh = Spreadsheet(client.http_client, {"id": SPREADSHEET_ID})
        break
    except APIError as e:
        last_error = e
        time.sleep(2)
else:
    raise RuntimeError(f"Não foi possível abrir a planilha: {last_error}")

# =============================
# ABAS (PADRÃO FINAL)
# =============================
sheet_ativos = sh.worksheet("Pedidos | Ativo")
sheet_entregue = sh.worksheet("Pedidos | Entregue")
sheet_falha = sh.worksheet("Pedidos | Falha")

STATUS_ENTREGUE = "ENTREGUE"
STATUS_FALHA = "FALHA"

# =============================
# LEITURA DOS ATIVOS
# =============================
ativos = sheet_ativos.get_all_values()
header = ativos[0]
linhas = ativos[1:]

col_status = header.index("STATUS LOGÍSTICO")

ativos_restantes = []
mover_entregue = []
mover_falha = []

for row in linhas:
    status = (row[col_status] or "").strip().upper()

    if status == STATUS_ENTREGUE:
        mover_entregue.append(row)

    elif status == STATUS_FALHA:
        mover_falha.append(row)

    else:
        ativos_restantes.append(row)

# =============================
# FUNÇÃO AUXILIAR – PREPEND
# =============================
def prepend_rows(sheet, novas_linhas):
    """
    Insere novas linhas no topo da aba (abaixo do cabeçalho),
    mantendo o histórico existente.
    """
    if not novas_linhas:
        return

    atual = sheet.get_all_values()
    header_dest = atual[0]
    historico = atual[1:]

    nova_base = [header_dest] + novas_linhas + historico

    sheet.clear()
    sheet.update("A1", nova_base, value_input_option="RAW")

# =============================
# ATUALIZA DESTINOS
# =============================
prepend_rows(sheet_entregue, mover_entregue)
prepend_rows(sheet_falha, mover_falha)

# =============================
# ATUALIZA ATIVOS
# =============================
nova_base_ativos = [header] + ativos_restantes
sheet_ativos.clear()
sheet_ativos.update("A1", nova_base_ativos, value_input_option="RAW")

print(
    f"🏁 Movidos | Entregue: {len(mover_entregue)} | Falha: {len(mover_falha)}"
)
