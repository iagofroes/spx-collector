import json
import gspread
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

ID_FONTE = "1YQz5RqJ3NiZ_WzXgL2r9iBSJJhEAWWVU0xMPjPoDy5w"
ID_DESTINO = "1kPajm35CAytXTGkh5skSGEGCooeXuiPOSUW99KJCP-g"

MESES = ["jan", "fev", "mar", "abr", "mai", "jun",
         "jul", "ago", "set", "out", "nov", "dez"]

def formatar_data(dt: datetime) -> str:
    return f"{dt.day}-{MESES[dt.month - 1]}."

def sincronizar_abs():
    creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    client = gspread.authorize(creds)

    print("📥 Lendo origem...")
    sh_fonte = client.open_by_key(ID_FONTE).worksheet("Registro_Presenca")
    dados_fonte = sh_fonte.get_all_values()

    print("📥 Lendo destino...")
    sh_destino = client.open_by_key(ID_DESTINO).worksheet("ABS")
    matriz_destino = sh_destino.get_all_values()

    # Linha 1 (índice 0) = cabeçalho com datas
    cabecalho = [str(c).strip().lower() for c in matriz_destino[0]]

    # Nomes na coluna B (índice 1), a partir da linha 2 (índice 1)
    nomes_destino = [str(row[1]).strip().upper() for row in matriz_destino[1:]]

    data_limite = datetime.now() - timedelta(days=3)

    atualizacoes = {}  # a1notation -> valor
    celulas_ignoradas = 0

    print("🔄 Processando registros...")
    for linha in dados_fonte[1:]:  # pula cabeçalho da origem
        if len(linha) < 7 or not linha[1].strip():
            continue

        # Data na coluna B (índice 1) formato DD/MM/YYYY
        data_str = linha[1].strip().split(" ")[0]
        try:
            dt = datetime.strptime(data_str, "%d/%m/%Y")
        except ValueError:
            continue

        if dt < data_limite:
            continue

        data_f = formatar_data(dt)
        nome_f = linha[3].strip().upper()
        status_f = linha[6].strip()

        if nome_f not in nomes_destino:
            continue

        row_idx = nomes_destino.index(nome_f)  # base 0 relativo aos nomes
        
        if data_f.lower() not in cabecalho:
            continue
        
        col_idx = cabecalho.index(data_f.lower())  # base 0

        # Linha real na planilha = row_idx + 2 (base 1 + pula cabeçalho)
        num_linha = row_idx + 2
        # Coluna real = col_idx + 1 (base 1)
        num_coluna = col_idx + 1

        a1 = gspread.utils.rowcol_to_a1(num_linha, num_coluna)

        if a1 in atualizacoes:
            continue

        # Verifica valor atual na matriz (row_idx+1 porque matriz inclui cabeçalho)
        valor_atual = ""
        row_matriz = row_idx + 1
        if row_matriz < len(matriz_destino) and col_idx < len(matriz_destino[row_matriz]):
            valor_atual = str(matriz_destino[row_matriz][col_idx]).strip()

        if valor_atual == "":
            atualizacoes[a1] = status_f
        else:
            celulas_ignoradas += 1

    if atualizacoes:
        # Batch update
        body = {
            "valueInputOption": "USER_ENTERED",
            "data": [
                {"range": f"ABS!{a1}", "values": [[v]]}
                for a1, v in atualizacoes.items()
            ]
        }
        service = client.sheet1  # apenas para pegar o spreadsheet service
        sh_destino.spreadsheet.values_batch_update(body)
        print(f"✓ {len(atualizacoes)} células atualizadas!")
    else:
        print("• Nenhuma atualização necessária.")

    if celulas_ignoradas > 0:
        print(f"ℹ {celulas_ignoradas} células já preenchidas preservadas.")

if __name__ == "__main__":
    sincronizar_abs()