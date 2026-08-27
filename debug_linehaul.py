"""
PATCH — spx_collector.py
Aplica 3 correções para o bug de cookies duplicados + csrftoken ausente.

COMO APLICAR:
  Substitua as 3 funções abaixo no spx_collector.py pelo conteúdo deste arquivo.
  As demais funções não precisam ser alteradas.
"""

# =================================================================
# CORREÇÃO 1 — Substitui a função get_cookie_safe (NOVA, adicionar)
# Adicione esta função logo após criar_sessao(), antes de _md5()
# =================================================================

import logging


def get_cookie_safe(session, name):
    """
    Pega cookie pelo nome sem explodir com CookieConflictError.
    Prefere domínio exato 'spx.shopee.com.br' sobre '.spx.shopee.com.br'.
    """
    # Prioridade 1: domínio exato SPX
    for cookie in session.cookies:
        if cookie.name == name and cookie.domain == "spx.shopee.com.br":
            return cookie.value
    # Prioridade 2: subdomínio .spx
    for cookie in session.cookies:
        if cookie.name == name and cookie.domain == ".spx.shopee.com.br":
            return cookie.value
    # Prioridade 3: qualquer domínio
    for cookie in session.cookies:
        if cookie.name == name:
            return cookie.value
    return ""


# =================================================================
# CORREÇÃO 2 — Substitui fazer_login() completo
# O csrftoken não é mais setado pelo SPX no login.
# Usa get_cookie_safe() para evitar CookieConflictError.
# =================================================================

def fazer_login(session):
    if not SPX_USERNAME or not SPX_PASSWORD:
        logging.critical("SPX_USERNAME e/ou SPX_PASSWORD não definidos nos Secrets.")
        return False

    try:
        logging.info("Login — Passo 1: carregando página FMS...")
        session.headers.update({"Origin": "https://fms.business.accounts.shopee.com.br"})
        resp_page = session.get(FMS_LOGIN_PAGE_URL, timeout=30, allow_redirects=True)
        resp_page.raise_for_status()
        logging.info(f"Login — Passo 1 OK. Cookies obtidos: {len(session.cookies)}")

        logging.info("Login — Passo 2: enviando credenciais...")
        payload = {
            "username":                    SPX_USERNAME,
            "password":                    _md5(SPX_PASSWORD),
            "captcha_signature":           "",
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
            allow_redirects=False,
        )
        resp_login.raise_for_status()

        data = resp_login.json()
        logging.info(f"Login — Passo 2 resposta: error={data.get('error')} | data keys={list(data.get('data', {}).keys())}")

        if data.get("error") != 0:
            logging.error(f"Login falhou. Resposta completa: {data}")
            return False

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

        # ── FIX: usa get_cookie_safe para evitar CookieConflictError ──────────
        csrf    = get_cookie_safe(session, "csrftoken")
        spx_cid = get_cookie_safe(session, "spx_cid")
        spx_uk  = get_cookie_safe(session, "spx_uk")      # auth token principal agora

        if csrf:
            session.headers.update({"x-csrftoken": csrf})

        # FIX: aceita sessão se tiver spx_uk ou spx_cid, mesmo sem csrftoken
        if csrf or spx_cid or spx_uk:
            logging.info("✅ Login SPX completo! Sessão estabelecida.")
            return True

        logging.warning(
            "Login: cookies spx_cid/csrftoken/spx_uk não encontrados, "
            "mas nenhum erro ocorreu. Tentando continuar..."
        )
        return True

    except ConnectionAbortedError:
        raise
    except Exception as exc:
        logging.error(f"Exceção durante login SPX: {exc}", exc_info=True)
        return False


# =================================================================
# CORREÇÃO 3 — Substitui executar_chamada_linehaul() completo
# Usa get_cookie_safe() em vez de session.cookies.get()
# =================================================================

def executar_chamada_linehaul(session, url):
    """Chamada GET específica para LineHaul — usa get_cookie_safe para evitar conflito."""
    try:
        # FIX: get_cookie_safe no lugar de session.cookies.get() que explode com duplicatas
        csrf = get_cookie_safe(session, "csrftoken")
        headers = {
            "Referer":     "https://spx.shopee.com.br/",
            "x-csrftoken": csrf,
        }
        resp = session.get(url, headers=headers, timeout=30)

        if resp.status_code == 403:
            logging.debug("LineHaul 403 — ignorado.")
            return None

        resp.raise_for_status()
        json_response = resp.json()
        retcode = json_response.get("retcode", json_response.get("code", -1))

        if retcode != 0:
            msg = json_response.get("message", "")
            if "cookie" in msg.lower() or "login" in msg.lower():
                raise ConnectionAbortedError("Sessão expirada.")
            logging.debug(f"LineHaul retcode={retcode}: {msg}")
            return None

        return json_response.get("data", json_response)

    except ConnectionAbortedError:
        raise
    except Exception as exc:
        logging.error(f"Falha LineHaul [{url[:80]}]: {exc}")
        return None