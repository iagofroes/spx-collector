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
YMS_SPREADSHEET_ID           = "1Ro56eetkC_IS4JUtLium5oA8Ty6XZ5-noQxO44VlYfc"

CONFIG_SHEET_NAME            = "Configuracoes"
PRODUTIVIDADE_SHEET_NAME     = "raw_spx_workstation"
OUTBOUND_SHEET_NAME          = "raw_spx_packing_formated"
OUTBOUND_ORIGINAL_SHEET_NAME = "raw_spx_packing"
YMS_SHEET_NAME               = "yms_ontime"

PRODUCTIVITY_API_URL  = "https://spx.shopee.com.br/api/wfm/admin/workstation/productivity/productivity_individual_list"
OUTBOUND_API_URL      = "https://spx.shopee.com.br/api/wfm/admin/dashboard/list"
LINEHAUL_API_BASE     = "https://spx.shopee.com.br/api/admin/transportation/trip/list_v2"
LINEHAUL_REFERER      = "https://spx.shopee.com.br/#/hubLinehaulTrips/trip"

LINEHAUL_STATION_TYPES = "2,3,7,12,14,16,18"
LINEHAUL_TAB_TYPES     = [1, 2, 3]
LINEHAUL_TAB_LABEL     = {1: "Pending", 2: "Handover", 3: "Ended"}
LINEHAUL_PAGE_SIZE     = 200
LINEHAUL_DISPLAY_DAYS  = 7

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


def calcular_display_range():
    tz    = pytz.timezone(TIMEZONE)
    agora = datetime.now(tz)
    fim   = agora.replace(hour=23, minute=59, second=59, microsecond=0)
    ini   = (agora - timedelta(days=LINEHAUL_DISPLAY_DAYS)).replace(
                hour=0, minute=0, second=0, microsecond=0)
    return f"{int(ini.timestamp())},{int(fim.timestamp())}"


def ts_to_str(ts):
    if not ts or ts == 0:
        return "-"
    try:
        ts = int(ts)
        if ts > 1e12:
            ts = ts // 1000
        tz = pytz.timezone(TIMEZONE)
        return datetime.fromtimestamp(ts, tz=tz).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return str(ts)


def safe(val, default="-"):
    if val is None or val == "":
        return default
    return val


STATUS_MAP = {
    1: "Pending", 2: "Assigned", 3: "Departed",
    4: "Arrived", 5: "Completed", 6: "Cancelled",
    7: "Loading", 8: "Loaded",
}
ON_TIME_MAP = {
    0: "-",           1: "On Time",
    2: "Late Arrival", 3: "Early Arrival",
    4: "Late Departure", 5: "Early Departure",
}
CIOT_STATUS_MAP = {1: "Created", 2: "Pending Create"}
TOLL_STATUS_MAP = {1: "Fail", 2: "NA", 3: "Paid", 4: "Pending Payment"}
MDFE_STATUS_MAP = {
    1: "Cancel",          2: "Closed",
    3: "Created",         4: "Failed Creation",
    5: "No MDFe issued",  6: "Waiting tax document",
    7: "Webhook Failure",
}


def processar_trip(t, tab_label):
    try:
        trip_stations = t.get("trip_station") or []
        origem  = next((s for s in trip_stations if s.get("sequence_number") == 1), None)
        destino = next((s for s in trip_stations if s.get("station_operation_type") == 1), None)
        if not destino and len(trip_stations) > 1:
            destino = trip_stations[-1]

        def ts_orig(campo): return ts_to_str(origem.get(campo) if origem else None)
        def ts_dest(campo): return ts_to_str(destino.get(campo) if destino else None)

        sta = ts_dest("sta")
        std = ts_orig("std")
        ata = ts_dest("ata")
        atd = ts_orig("atd")
        eta = ts_dest("eta")
        etd = ts_orig("etd")

        loading_time = ts_orig("loading_time")
        seal_time    = ts_orig("seal_time")
        load_qty     = sum(s.get("load_quantity",   0) for s in trip_stations)
        unload_qty   = sum(s.get("unload_quantity", 0) for s in trip_stations)

        stations = t.get("station_list") or t.get("stations") or []
        if stations:
            station_str = " → ".join(s.get("name") or s.get("station_name") or "?" for s in stations)
        elif trip_stations:
            station_str = " → ".join(
                s.get("station_name") or "?"
                for s in sorted(trip_stations, key=lambda x: x.get("sequence_number", 0))
            )
        else:
            station_str = safe(t.get("station") or t.get("station_name"))

        on_time_raw = next(
            (s.get("on_time_indicator_value") for s in trip_stations
             if s.get("on_time_indicator_value")), 0
        )
        on_time_str = ON_TIME_MAP.get(on_time_raw, str(on_time_raw) if on_time_raw else "-")

        veh_plate = t.get("vehicle_number") or t.get("plate_number") or "-"
        if isinstance(veh_plate, list):
            veh_plate = ", ".join(veh_plate)

        status_str = STATUS_MAP.get(t.get("trip_status") or t.get("status"), str(safe(t.get("trip_status"))))
        ciot_str   = CIOT_STATUS_MAP.get(t.get("ciot_status"), str(safe(t.get("ciot_status"))))
        toll_str   = TOLL_STATUS_MAP.get(t.get("toll_status"), str(safe(t.get("toll_status"))))
        mdfe_str   = MDFE_STATUS_MAP.get(t.get("mdfe_status"), str(safe(t.get("mdfe_status"))))

        return [
            tab_label,
            safe(t.get("trip_number")   or t.get("lh_trip_number")),
            safe(t.get("trip_name")     or t.get("lh_trip_name")),
            status_str, station_str,
            ts_to_str(t.get("last_location_update_time")),
            on_time_str, safe(t.get("vehicle_type")),
            f"{sta} / {std}", f"{ata} / {atd}", f"{eta} / {etd}",
            loading_time, seal_time, load_qty, unload_qty,
            veh_plate,
            safe(t.get("driver_name")        or t.get("driver")),
            safe(t.get("second_driver_name") or t.get("second_driver")),
            ciot_str, safe(t.get("ciot_err") or t.get("ciot_error")),
            toll_str, safe(t.get("toll_err") or t.get("toll_error")),
            mdfe_str,
            safe(t.get("trip_source")), safe(t.get("trip_type")),
            safe(t.get("cost_type")),
            safe(t.get("agency_name") or t.get("agency")),
            ts_to_str(t.get("mtime") or t.get("update_time")),
            safe(t.get("operator")),
            ts_to_str(t.get("assigned_time") or t.get("assign_time")),
            safe(t.get("to_inbound_quantity"),    0),
            safe(t.get("order_inbound_quantity"), 0),
            safe(t.get("pack_type")),
            safe(t.get("order_packed_quantity"),  0),
            safe(t.get("to_packed_quantity"),     0),
            safe(t.get("to_loaded_quantity"),     0),
            safe(t.get("order_loaded_quantity"),  0),
        ]
    except Exception as e:
        logging.warning(f"Erro ao processar trip {t.get('trip_number','?')}: {e}")
        return None


def coletar_linehaul_trips(session):
    logging.info("--- Coletando LineHaul Trips ---")
    display_range = calcular_display_range()
    todas = []

    for tab in LINEHAUL_TAB_TYPES:
        label  = LINEHAUL_TAB_LABEL[tab]
        pageno = 1
        coletados = 0
        logging.info(f"  [{label}]")

        while True:
            url = (
                f"{LINEHAUL_API_BASE}"
                f"?station_type={LINEHAUL_STATION_TYPES}"
                f"&pageno={pageno}&count={LINEHAUL_PAGE_SIZE}"
                f"&query_type=1&tab_type={tab}"
                f"&display_range={display_range}"
            )
            data = executar_chamada_api(session, "GET", url, LINEHAUL_REFERER)

            if not data:
                break

            lista     = data.get("list") or data.get("trip_list") or data.get("trips") or []
            total_api = int(data.get("total", data.get("count", 0)))

            if not lista:
                break

            logging.info(f"    p.{pageno}: {len(lista)} registros (total={total_api})")

            for item in lista:
                row = processar_trip(item, label)
                if row:
                    todas.append(row)

            coletados += len(lista)
            if coletados >= total_api or len(lista) < LINEHAUL_PAGE_SIZE:
                logging.info(f"    [{label}] concluído: {coletados}/{total_api}")
                break

            pageno += 1
            time.sleep(0.3)

    logging.info(f"LineHaul TOTAL: {len(todas)} registros.")
    return todas

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

            # 4 — LineHaul Trips (yms_ontime)
            header_yms = [
                "Tab", "LH Trip Number", "LH Trip Name", "Status",
                "Station (Origem → Destino)", "Last Location Update Time",
                "On Time Indicator", "Vehicle Type",
                "STA / STD", "ATA / ATD", "ETA / ETD",
                "Loading Time", "Seal Time",
                "Inbound Qty", "Outbound Qty",
                "Vehicle Plate Number", "Driver", "Second Driver",
                "CIOT Status", "CIOT Error",
                "Toll Status", "Toll Error",
                "MDFe Status", "Trip Source", "Trip Type", "Cost Type",
                "Agency", "Time Update", "Operator", "Assign Time",
                "Pending Inbound TO", "Pending Inbound Order",
                "Pending Inbound TO Pack Type",
                "Order Packed", "TO Packed", "TO Loaded", "Order Loaded",
            ]
            dados_yms = coletar_linehaul_trips(session)
            if dados_yms:
                write_to_sheet(sheets_service, YMS_SPREADSHEET_ID, YMS_SHEET_NAME, [header_yms] + dados_yms)
                append_timestamp(sheets_service, YMS_SPREADSHEET_ID, YMS_SHEET_NAME, ts)
            else:
                logging.warning("LineHaul vazio — mantendo dados anteriores no Sheets.")

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