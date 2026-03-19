import json
import os
import time
import logging
import requests
from datetime import datetime, timedelta
import pytz

# =================================================================
# AMBIENTE: local (.env) ou nuvem (GitHub Secrets)
# =================================================================
# Em ambiente local, cria um arquivo .env na mesma pasta com:
#   SPX_USERNAME=seu_ops_id
#   SPX_PASSWORD=sua_senha
#   GOOGLE_TOKEN_JSON=<conteúdo do token.json em uma linha>
#
# Na nuvem (GitHub Actions), as variáveis vêm dos Secrets automaticamente.
# O bloco abaixo tenta carregar o .env se existir — sem erro se não existir.
try:
    from dotenv import load_dotenv
    if load_dotenv(override=False):  # override=False: Secrets da nuvem têm prioridade
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
        logging.info("🔧 Ambiente LOCAL detectado — variáveis carregadas do .env")
except ImportError:
    pass  # python-dotenv não instalado = ambiente de nuvem, tudo bem

# --- Importações do Google Sheets ---
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# =================================================================
# CONFIGURAÇÃO GERAL
# =================================================================

PROD_OUTBOUND_SPREADSHEET_ID = "1-etBpNtYbvYvpQ5e8NLKxlJXBq5Wp4SmBfhylLY6QP8"
DOCK_QUEUE_SPREADSHEET_ID    = "1-etBpNtYbvYvpQ5e8NLKxlJXBq5Wp4SmBfhylLY6QP8"

CONFIG_SHEET_NAME            = "Configuracoes"
PRODUTIVIDADE_SHEET_NAME     = "raw_spx_workstation"
OUTBOUND_SHEET_NAME          = "raw_spx_packing_formated"
OUTBOUND_ORIGINAL_SHEET_NAME = "raw_spx_packing"
DOCK_QUEUE_SHEET_NAME        = "raw_spx_dock_queue"

PRODUCTIVITY_API_URL = "https://spx.shopee.com.br/api/wfm/admin/workstation/productivity/productivity_individual_list"
OUTBOUND_API_URL     = "https://spx.shopee.com.br/api/wfm/admin/dashboard/list"
DOCK_QUEUE_API_URL   = "https://spx.shopee.com.br/api/in-station/dock_management/queue/list"

# URLs do fluxo de login
# Passo 1: página FMS que seta cookies iniciais
FMS_LOGIN_PAGE_URL = (
    "https://fms.business.accounts.shopee.com.br/authenticate/login/"
    "?client_id=25"
    "&next=https%3A%2F%2Fspx.shopee.com.br%2Fapi%2Fadmin%2Fbasicserver%2Fops_tob_login"
    "%3Frefer%3Dhttps%3A%2F%2Fspx.shopee.com.br%2F%23%2F"
)
# Passo 2: endpoint real confirmado via DevTools
SPX_LOGIN_API_URL = "https://shopee.com.br/api/v4/account/business/login"

# Redirect final que seta a sessão SPX após login bem-sucedido
SPX_TOB_LOGIN_URL = (
    "https://spx.shopee.com.br/api/admin/basicserver/ops_tob_login"
    "?refer=https://spx.shopee.com.br/%23/"
)

# Fingerprint do device — vem do Secret SPX_DEVICE_FINGERPRINT
# Se precisar atualizar: DevTools → Network → login → Payload → security_device_fingerprint
SPX_DEVICE_FINGERPRINT = os.environ.get("SPX_DEVICE_FINGERPRINT", "")

EXECUTION_INTERVAL_SECONDS = 60
TIMEZONE = "America/Sao_Paulo"
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]

# =================================================================
# CREDENCIAIS SPX  ← Lidas de variáveis de ambiente (GitHub Secrets)
# =================================================================
SPX_USERNAME = os.environ.get("SPX_USERNAME", "")
SPX_PASSWORD = os.environ.get("SPX_PASSWORD", "")

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# =================================================================
# SESSÃO HTTP  (substitui o Selenium por completo)
# =================================================================

def criar_sessao() -> requests.Session:
    """Cria uma sessão requests com headers padrão do browser."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin":          "https://spx.shopee.com.br",
    })
    return session


def _md5(texto: str) -> str:
    """Retorna o hash MD5 de uma string (usado para senha no login SPX)."""
    import hashlib
    return hashlib.md5(texto.encode()).hexdigest()


def fazer_login(session: requests.Session) -> bool:
    """
    Fluxo de login confirmado via DevTools:

      1. GET na página FMS → seta cookies iniciais (SPC_F, SPC_SEC_SI, etc.)
      2. POST para shopee.com.br/api/v4/account/business/login
         com username, password em MD5, captcha_signature vazio
         e security_device_fingerprint fixo
      3. Resposta contém code=0 + token de autorização
      4. GET em ops_tob_login com o token → SPX seta cookies de sessão
    """
    if not SPX_USERNAME or not SPX_PASSWORD:
        logging.critical("SPX_USERNAME e/ou SPX_PASSWORD não definidos nos Secrets.")
        return False

    try:
        # ── Passo 1: carregar página FMS para obter cookies iniciais ───────────
        logging.info("Login — Passo 1: carregando página FMS...")
        session.headers.update({"Origin": "https://fms.business.accounts.shopee.com.br"})
        resp_page = session.get(FMS_LOGIN_PAGE_URL, timeout=30, allow_redirects=True)
        resp_page.raise_for_status()
        logging.info(f"Login — Passo 1 OK. Cookies obtidos: {len(session.cookies)}")

        # ── Passo 2: POST de login para shopee.com.br ──────────────────────────
        logging.info("Login — Passo 2: enviando credenciais...")
        payload = {
            "username":                  SPX_USERNAME,
            "password":                  _md5(SPX_PASSWORD),
            "captcha_signature":         "",
            "security_device_fingerprint": SPX_DEVICE_FINGERPRINT,
        }
        login_headers = {
            "Referer":      "https://fms.business.accounts.shopee.com.br/",
            "Origin":       "https://fms.business.accounts.shopee.com.br",
            "Content-Type": "application/json",
            "x-app-type":   "27",
        }
        resp_login = session.post(
            SPX_LOGIN_API_URL,
            json=payload,
            headers=login_headers,
            timeout=30,
            allow_redirects=False,  # vamos checar a resposta antes de redirecionar
        )
        resp_login.raise_for_status()

        data = resp_login.json()
        logging.info(f"Login — Passo 2 resposta: error={data.get('error')} | data keys={list(data.get('data', {}).keys())}")

        # Campo de sucesso é "error": 0  (não "code")
        if data.get("error") != 0:
            logging.error(f"Login falhou. Resposta completa: {data}")
            return False

        # ── Passo 3: usar nonce retornado para finalizar sessão no SPX ─────────
        # O token vem no campo "nonce" dentro de "data"
        token = (
            data.get("data", {}).get("nonce")
            or data.get("data", {}).get("token")
            or data.get("data", {}).get("access_token")
            or ""
        )
        logging.info(f"Login — Passo 3: nonce obtido: {'OK' if token else 'VAZIO'}")

        tob_url = SPX_TOB_LOGIN_URL
        if token:
            tob_url = (
                f"https://spx.shopee.com.br/api/admin/basicserver/ops_tob_login"
                f"?code={token}&refer=https://spx.shopee.com.br/%23/"
            )

        logging.info("Login — Passo 3: finalizando sessão SPX...")
        session.headers.update({"Origin": "https://spx.shopee.com.br"})
        resp_tob = session.get(tob_url, timeout=30, allow_redirects=True)
        resp_tob.raise_for_status()

        # ── Verificar cookies de sessão SPX ────────────────────────────────────
        csrf_spx = session.cookies.get("csrftoken", domain="spx.shopee.com.br") or ""
        spx_cid  = session.cookies.get("spx_cid",   domain="spx.shopee.com.br") or ""

        if csrf_spx or spx_cid:
            if csrf_spx:
                session.headers.update({"x-csrftoken": csrf_spx})
            logging.info("✅ Login SPX completo! Sessão estabelecida.")
            return True

        # Se não achou cookies específicos mas chegou até aqui sem erro, considera OK
        # (alguns ambientes podem usar nomes de cookies diferentes)
        logging.warning(
            "Login: cookies spx_cid/csrftoken não encontrados, "
            "mas nenhum erro ocorreu. Tentando continuar..."
        )
        return True

    except ConnectionAbortedError:
        raise
    except Exception as exc:
        logging.error(f"Exceção durante login SPX: {exc}", exc_info=True)
        return False


def executar_chamada_api(
    session: requests.Session,
    method: str,
    url: str,
    referer: str,
    payload: dict | None = None
) -> dict | None:
    """
    Executa uma chamada GET ou POST na API SPX usando a sessão requests.
    Retorna o campo 'data' da resposta JSON, ou None em caso de erro.
    """
    try:
        headers = {"Referer": referer}
        csrf = session.cookies.get("csrftoken", "")
        if csrf:
            headers["x-csrftoken"] = csrf
            session.headers.update({"x-csrftoken": csrf})

        if method.upper() == "POST":
            resp = session.post(url, json=payload, headers=headers, timeout=30)
        else:
            resp = session.get(url, headers=headers, timeout=30)

        # 403 = sem permissão para esse endpoint — ignora silenciosamente
        if resp.status_code == 403:
            logging.debug(f"API '{url}' retornou 403 (sem permissão) — ignorado.")
            return None

        resp.raise_for_status()
        json_response = resp.json()

        retcode = json_response.get("retcode")

        if retcode != 0:
            msg = json_response.get("message", "sem mensagem")
            # Só loga como erro se não for problema de permissão
            if retcode in (401, 403) or "cookie" in msg.lower() or "login" in msg.lower():
                raise ConnectionAbortedError("Sessão expirada detectada pela API.")
            logging.debug(f"API '{url}' retornou retcode={retcode}: {msg} — ignorado.")
            return None

        return json_response.get("data")

    except ConnectionAbortedError:
        raise
    except Exception as exc:
        # Ignora silenciosamente erros HTTP de permissão
        if "403" in str(exc) or "401" in str(exc):
            logging.debug(f"API '{url}' sem permissão — ignorado.")
            return None
        logging.error(f"Falha na chamada API '{url}': {exc}")
        return None

# =================================================================
# FUNÇÕES AUXILIARES
# =================================================================

def mapear_status_doca(status_id):
    return {1: "Pending", 2: "Assigned", 3: "Occupied", 4: "Ended", 5: "On Hold"}.get(
        status_id, f"Desconhecido ({status_id})"
    )

def mapear_tipo_chegada(tipo_id):
    return {1: "Line Haul", 7: "First Mile", 3: "Returns"}.get(
        tipo_id, f"ID Desconhecido ({tipo_id})"
    )

def formatar_tempo_de_espera(minutos):
    if not isinstance(minutos, (int, float)) or minutos <= 0:
        return "00:00"
    h, m = int(minutos // 60), int(minutos % 60)
    return f"{h:02d}:{m:02d}"

def calcular_periodos_coleta():
    tz  = pytz.timezone(TIMEZONE)
    agora = datetime.now(tz)
    dia_trabalho = agora if agora.hour >= 6 else agora - timedelta(days=1)
    inicio = dia_trabalho.replace(hour=6, minute=0, second=0, microsecond=0)
    fim    = (agora + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    periodos, hora_iter = [], inicio
    while hora_iter < fim:
        fim_iter = hora_iter + timedelta(hours=1)
        dia_iter = hora_iter if hora_iter.hour >= 6 else hora_iter - timedelta(days=1)
        periodos.append({
            "data_calendario": hora_iter.strftime("%Y-%m-%d"),
            "data_trabalho":   dia_iter.strftime("%Y-%m-%d"),
            "hora_inicio":     hora_iter.strftime("%H:%M"),
            "hora_fim":        fim_iter.strftime("%H:%M"),
            "periodo_str":     f"{hora_iter.hour}-{fim_iter.hour}",
        })
        hora_iter = fim_iter
    logging.info(f"Períodos a coletar: {len(periodos)}")
    return periodos

# =================================================================
# FUNÇÕES DE COLETA
# =================================================================

def coletar_dados_produtividade(session):
    logging.info("--- Coletando Produtividade (Workstation) ---")
    tz = pytz.timezone(TIMEZONE)
    dados_finais = []
    for periodo in calcular_periodos_coleta():
        start_dt = tz.localize(
            datetime.strptime(f"{periodo['data_calendario']} {periodo['hora_inicio']}", "%Y-%m-%d %H:%M")
        )
        end_dt = tz.localize(
            datetime.strptime(f"{periodo['data_calendario']} {periodo['hora_fim']}", "%Y-%m-%d %H:%M")
        )
        if start_dt.time() > end_dt.time():
            end_dt += timedelta(days=1)

        url = (
            f"{PRODUCTIVITY_API_URL}"
            f"?pageno=1&count=500"
            f"&start_time={int(start_dt.timestamp())}"
            f"&end_time={int(end_dt.timestamp())}"
            f"&activity_type=12"
        )
        data = executar_chamada_api(
            session, "GET", url,
            "https://spx.shopee.com.br/admin/workstation/productivity"
        )
        if data and data.get("list"):
            for item in data["list"]:
                ops_id, ops_name = "", ""
                if item.get("ops"):
                    parts = item["ops"].split("]")
                    if len(parts) > 1:
                        ops_id   = parts[0].replace("[", "").strip()
                        ops_name = parts[1].strip()
                dados_finais.append([
                    ops_id, ops_name,
                    item.get("workstation", ""),
                    item.get("activity_type", ""),
                    item.get("working_hours", 0),
                    item.get("total_throughput", 0),
                    item.get("check_in_time", ""),
                    item.get("check_out_time", ""),
                    "", "",
                    periodo["periodo_str"].split("-")[0],
                    periodo["data_trabalho"],
                ])
    return dados_finais


def coletar_dados_outbound(session):
    logging.info("--- Coletando Outbound (Packing) ---")
    payload = {
        "unit_type": 1, "process_type": 2, "period_type": 1,
        "pageno": 1, "count": 500, "productivity": 1,
        "order_by_total": 100, "event_id_list": [],
    }

    MAX_TENTATIVAS = 3
    ESPERA = 10  # segundos entre tentativas

    for tentativa in range(1, MAX_TENTATIVAS + 1):
        data = executar_chamada_api(
            session, "POST", OUTBOUND_API_URL,
            "https://spx.shopee.com.br/dashboard/overview",
            payload
        )

        if not data:
            logging.warning(f"Outbound tentativa {tentativa}/{MAX_TENTATIVAS}: API retornou None.")
        else:
            efficiency_list = data.get("efficiency_list", [])
            if efficiency_list:
                hora_atual = datetime.now(pytz.timezone(TIMEZONE)).hour
                originais, formatados = [], []
                for item in efficiency_list:
                    eff = item.get("efficiency", [])
                    padded = eff + [0] * (12 - len(eff))
                    originais.append([item.get("operator", ""), item.get("efficiency_total", 0)] + padded)
                    for i in range(12):
                        hora_eff = (hora_atual - i + 24) % 24
                        formatados.append([item.get("operator", ""), item.get("efficiency_total", 0), hora_eff, padded[i]])
                logging.info(f"Sucesso! {len(originais)} registros de Outbound na tentativa {tentativa}.")
                return originais, formatados
            else:
                logging.warning(f"Outbound tentativa {tentativa}/{MAX_TENTATIVAS}: efficiency_list vazio.")

        if tentativa < MAX_TENTATIVAS:
            logging.info(f"Aguardando {ESPERA}s antes de tentar novamente...")
            time.sleep(ESPERA)

    logging.warning("Outbound: todas as tentativas falharam — mantendo dados anteriores no Sheets.")
    return [], []


def coletar_dados_fila_doca(session):
    logging.info("--- Coletando Fila de Docas ---")
    payload = {
        "pageno": 1, "count": 500, "queue_type": 1,
        "add_to_queue_time": "", "queue_status": "1,2,3,5",
    }
    data = executar_chamada_api(
        session, "POST", DOCK_QUEUE_API_URL,
        "https://spx.shopee.com.br/station/inbound/dock",
        payload
    )
    if not (data and data.get("list")):
        logging.warning("Nenhum dado na Fila de Docas.")
        return []

    lista = []
    for i in data["list"]:
        lista.append([
            i.get("queue_number"),
            i.get("vehicle_number"),
            formatar_tempo_de_espera(i.get("waiting_time")),
            "Yes" if i.get("is_prioritized") == 1 else "No",
            ", ".join(i.get("prioritised_tags", [])),
            formatar_tempo_de_espera(i.get("on_hold_time")),
            i.get("route_info", {}).get("lh_trip_number"),
            i.get("route_info", {}).get("lh_trip_name"),
            i.get("handover_task_number"),
            i.get("order_quantity"),
            i.get("driver_name"),
            mapear_tipo_chegada(i.get("arrival_type")),
            i.get("agency"),
            "Yes" if i.get("is_printed") else "No",
            i.get("assigned_dock_code"),
            i.get("assigned_dock_group_name"),
            i.get("occupied_dock_code"),
            mapear_status_doca(i.get("queue_status")),
            i.get("occupancy_sequence"),
            "-",
        ])
    logging.info(f"Sucesso! {len(lista)} registros de Docas.")
    return lista

# =================================================================
# GOOGLE SHEETS
# =================================================================

def get_sheets_service():
    creds = None
    token_json = os.environ.get("GOOGLE_TOKEN_JSON", "")
    if token_json:
        import tempfile
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        tmp.write(token_json)
        tmp.close()
        creds = Credentials.from_authorized_user_file(tmp.name, SCOPES)
    elif os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as f:
            f.write(creds.to_json())

    return build("sheets", "v4", credentials=creds)


def write_to_sheet(service, spreadsheet_id, sheet_name, data):
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'"
        ).execute()
        if data and any(data):
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{sheet_name}'!A1",
                valueInputOption="USER_ENTERED",
                body={"values": data}
            ).execute()
            logging.info(f"✅ {len(data)} linhas escritas em '{sheet_name}'.")
        else:
            logging.warning(f"Nenhum dado para '{sheet_name}' — aba limpa.")
    except HttpError as err:
        logging.error(f"Erro ao escrever em '{sheet_name}': {err}")


def append_timestamp(service, spreadsheet_id, sheet_name, ts):
    try:
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A:B",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [["Última Atualização:", ts]]}
        ).execute()
    except HttpError as err:
        logging.error(f"Erro ao adicionar timestamp em '{sheet_name}': {err}")


def salvar_configs_sessao(session: requests.Session, service, spreadsheet_id, sheet_name):
    logging.info(f"--- Salvando configs de sessão em '{sheet_name}' ---")
    try:
        cookies_str = "; ".join(
            f"{c.name}={c.value}" for c in session.cookies
        )
        csrf = session.cookies.get("csrftoken", "N/A")
        dados = [
            ["Chave de Configuração", "Valor"],
            ["Data/Hora da Extração", datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d %H:%M:%S")],
            ["Cookie", cookies_str],
            ["x-csrftoken", csrf],
            ["User-Agent", session.headers.get("User-Agent", "N/A")],
        ]
        write_to_sheet(service, spreadsheet_id, sheet_name, dados)
    except Exception as exc:
        logging.error(f"Erro ao salvar configs de sessão: {exc}")

# =================================================================
# ORQUESTRADOR PRINCIPAL
# =================================================================

def main():
    # ── Autenticação Google Sheets ──────────────────────────────
    try:
        sheets_service = get_sheets_service()
    except Exception as exc:
        logging.critical(f"Falha ao autenticar Google Sheets: {exc}")
        return

    MAX_RETRIES_LOGIN = 5
    session = None

    # ── Loop infinito de coleta ─────────────────────────────────
    while True:
        # (Re)login se necessário
        if session is None:
            for tentativa in range(1, MAX_RETRIES_LOGIN + 1):
                logging.info(f"Tentativa de login {tentativa}/{MAX_RETRIES_LOGIN}…")
                session = criar_sessao()
                if fazer_login(session):
                    break
                session = None
                time.sleep(10 * tentativa)   # backoff progressivo
            else:
                logging.critical("Todas as tentativas de login falharam. Abortando.")
                return

        logging.info("### INICIANDO NOVO CICLO ###")
        try:
            ts = datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d %H:%M:%S")

            # 1 — Configs de sessão
            salvar_configs_sessao(session, sheets_service, PROD_OUTBOUND_SPREADSHEET_ID, CONFIG_SHEET_NAME)

            # 2 — Produtividade
            header_prod = [
                "ID do Operador", "Nome do Operador", "Estação de Trabalho",
                "Tipo de Atividade", "Horas Trabalhadas", "QUANTO O COLABORADOR FEZ",
                "Check-in", "Check-out", "Vazia 1", "Vazia 2", "Hora", "Data",
            ]
            dados_prod = coletar_dados_produtividade(session)
            write_to_sheet(sheets_service, PROD_OUTBOUND_SPREADSHEET_ID, PRODUTIVIDADE_SHEET_NAME, [header_prod] + dados_prod)
            if dados_prod:
                append_timestamp(sheets_service, PROD_OUTBOUND_SPREADSHEET_ID, PRODUTIVIDADE_SHEET_NAME, ts)

            # 3 — Outbound
            originais, formatados = coletar_dados_outbound(session)

            header_orig = ["Operador", "Total", "H-0","H-1","H-2","H-3","H-4","H-5","H-6","H-7","H-8","H-9","H-10","H-11"]
            if originais:
                write_to_sheet(sheets_service, PROD_OUTBOUND_SPREADSHEET_ID, OUTBOUND_ORIGINAL_SHEET_NAME, [header_orig] + originais)
                append_timestamp(sheets_service, PROD_OUTBOUND_SPREADSHEET_ID, OUTBOUND_ORIGINAL_SHEET_NAME, ts)
            else:
                logging.warning("Outbound original vazio — mantendo dados anteriores no Sheets.")

            header_fmt = ["Operador", "Total", "Hora", "Eficiência"]
            if formatados:
                write_to_sheet(sheets_service, PROD_OUTBOUND_SPREADSHEET_ID, OUTBOUND_SHEET_NAME, [header_fmt] + formatados)
                append_timestamp(sheets_service, PROD_OUTBOUND_SPREADSHEET_ID, OUTBOUND_SHEET_NAME, ts)
            else:
                logging.warning("Outbound formatado vazio — mantendo dados anteriores no Sheets.")

            # 4 — Fila de Docas
            header_doca = [
                "Queue Number","Vehicle Number","Waiting Time","Prioritised","Prioritised Factors",
                "On-hold Time","LH Trip Number","LH Trip Name","Handover Task Number",
                "Pending Inbound Parcel Qty","Driver Name","Arrival Type","Agency","Print Tag",
                "Assigned Dock","Assigned Dock Group","Occupied Dock","Status",
                "Dock Occupancy Sequence","Action",
            ]
            dados_doca = coletar_dados_fila_doca(session)
            write_to_sheet(sheets_service, DOCK_QUEUE_SPREADSHEET_ID, DOCK_QUEUE_SHEET_NAME, [header_doca] + dados_doca)
            if dados_doca:
                append_timestamp(sheets_service, DOCK_QUEUE_SPREADSHEET_ID, DOCK_QUEUE_SHEET_NAME, ts)

        except ConnectionAbortedError:
            logging.warning("Sessão expirada — forçando novo login no próximo ciclo.")
            session = None
            continue   # reinicia o loop sem esperar

        except Exception as exc:
            logging.error(f"Erro inesperado no ciclo: {exc}", exc_info=True)
            # Não mata o script — apenas loga e continua

        logging.info(f"### CICLO CONCLUÍDO. Aguardando {EXECUTION_INTERVAL_SECONDS}s… ###")
        time.sleep(EXECUTION_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()