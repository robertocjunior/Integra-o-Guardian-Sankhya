import requests
import os
from dotenv import load_dotenv
import json
import sys
import pyodbc
from datetime import datetime
import io # Importa para capturar a saída do console
from flask import Flask, Response, stream_with_context # Importa o Flask

# Inicializa a aplicação Flask
app = Flask(__name__)

# --- Funções de Lógica de Negócio (praticamente as mesmas, mas os 'print's serão capturados) ---

def load_credentials():
    load_dotenv(override=True)

    api_required_vars = ["TOKEN", "APPKEY", "USERNAME_API", "PASSWORD_API"]
    db_required_vars = ["DB_SERVER", "DB_DATABASE", "DB_USERNAME", "DB_PASSWORD"]

    credentials = {}

    for var in api_required_vars:
        value = os.getenv(var)
        if not value:
            print(f"Erro Crítico: A variável de ambiente '{var}' (API) não foi encontrada ou está vazia no arquivo .env.")
            sys.exit(1)
        credentials[var.lower()] = value.strip()

    for var in db_required_vars:
        value = os.getenv(var)
        if not value:
            print(f"Erro Crítico: A variável de ambiente '{var}' (DB) não foi encontrada ou está vazia no arquivo .env.")
            sys.exit(1)
        credentials[var.lower()] = value.strip()

    return credentials

def perform_login(credentials):
    url = "https://api.sankhya.com.br/login"

    headers = {
        "token": credentials["token"],
        "appkey": credentials["appkey"],
        "username": credentials["username_api"],
        "password": credentials["password_api"]
    }

    print("Tentando realizar o login na API Sankhya...")
    try:
        response = requests.post(url, headers=headers)
        response.raise_for_status()

        response_data = response.json()
        bearer_token = response_data.get("bearerToken")
        print("\n✅ Login na API Sankhya realizado com sucesso!")
        return bearer_token

    except requests.exceptions.HTTPError as http_err:
        print(f"\n❌ Ocorreu um erro HTTP ao logar na API: {http_err}")
        print(f"Status Code: {http_err.response.status_code}")
        try:
            error_details = http_err.response.json()
            print("Detalhes do erro:")
            print(json.dumps(error_details, indent=4, ensure_ascii=False))
        except json.JSONDecodeError:
            print(f"Resposta do servidor (não-JSON): {http_err.response.text}")
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Ocorreu um erro de conexão ao logar na API: {e}")

    return None

def connect_to_sql_server(credentials):
    server = credentials["db_server"]
    database = credentials["db_database"]
    username = credentials["db_username"]
    password = credentials["db_password"]

    cnxn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
    )
    print("\n--- Tentando conectar ao SQL Server ---")
    try:
        conn = pyodbc.connect(cnxn_str)
        print("✅ Conexão com o SQL Server estabelecida com sucesso!")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"❌ Erro ao conectar ao SQL Server: {ex}")
        print(f"Detalhes do erro: {ex.args[1]}")
        sys.exit(1)

def update_sankhya_partner_status(codparc, bearer_token):
    url = "https://api.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=DatasetSP.save&outputType=json"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer_token}"
    }
    
    payload = {
        "serviceName": "DatasetSP.save",
        "requestBody": {
            "entityName": "Parceiro",
            "standAlone": False,
            "fields": [
                "CODPARC",
                "AD_IMPORTADOGUARDIAN"
            ],
            "records": [
                {
                    "pk": {
                        "CODPARC": str(codparc)
                    },
                    "values": {
                        "1": "S" # 'S' para indicar que foi importado
                    }
                }
            ]
        }
    }
    
    print(f"--- Tentando atualizar AD_IMPORTADOGUARDIAN para 'S' no Sankhya para CODPARC: {codparc} ---")
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        response_data = response.json()
        
        if response_data.get("status") == "1":
            print(f"✅ AD_IMPORTADOGUARDIAN atualizado para 'S' no Sankhya para CODPARC: {codparc}!")
        else:
            print(f"⚠️ Falha ao atualizar AD_IMPORTADOGUARDIAN para CODPARC: {codparc}.")
            print(f"Resposta detalhada: {json.dumps(response_data, indent=4, ensure_ascii=False)}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro de conexão ou HTTP ao tentar atualizar AD_IMPORTADOGUARDIAN para CODPARC {codparc}: {e}")
        if e.response is not None:
            print(f"Status Code: {e.response.status_code}")
            print(f"Resposta do servidor: {e.response.text}")

def insert_partners_into_sql(records, sql_conn, bearer_token):
    if not records:
        print("Nenhum registro para inserir no SQL Server.")
        return

    cursor = sql_conn.cursor()
    insert_sql = """
    INSERT INTO tbTransportadora (
        TRP_CODIGO, TRP_DESCRICAO, TRP_RAZAO_SOCIAL, TRP_CNPJ,
        TRP_INSCRICAO_ESTADUAL, TRP_ENDERECO, TRP_COMPLEMENTO,
        TRP_MUNICIPIO, EST_CODIGO, TRP_CEP, TRP_TELEFONE, TRP_DATA, TRP_ESTADO
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    print("\n--- Inserindo dados na tabela tbTransportadora do SQL Server ---")
    
    codparcs_inserted_successfully = [] 
    
    try:
        for record in records:
            current_datetime = datetime.now()
            trp_estado_value = 0 

            trp_codigo = record.get("SKN_CODIGO", {}).get("$", None)
            trp_descricao = record.get("SKN_DECRICAO", {}).get("$", None)
            trp_razao_social = record.get("SKN_RAZAOSOCIAL", {}).get("$", None)
            trp_cnpj = record.get("SKN_CNPJ", {}).get("$", None)
            trp_inscricao_estadual = record.get("SKN_INSCRICAO_ESTADUAL", {}).get("$", None)
            trp_endereco = record.get("SKN_ENDERECO", {}).get("$", None)
            trp_complemento = record.get("SKN_COMPLEMENTO", {}).get("$", None)
            trp_municipio = record.get("SKN_MUNICIPIO", {}).get("$", None)
            trp_est_codigo = record.get("SNK_EST_CODIGO", {}).get("$", None)
            trp_cep = record.get("SNK_CEP", {}).get("$", None)
            trp_telefone = record.get("SNK_TELEFONE", {}).get("$", None)
            codparc_api = record.get("CODPARC", {}).get("$", None)

            cursor.execute(
                insert_sql,
                trp_codigo, trp_descricao, trp_razao_social, trp_cnpj,
                trp_inscricao_estadual, trp_endereco, trp_complemento,
                trp_municipio, trp_est_codigo, trp_cep, trp_telefone, current_datetime, trp_estado_value
            )
            if codparc_api:
                codparcs_inserted_successfully.append(codparc_api)

        sql_conn.commit()
        print(f"✅ {len(records)} registros inseridos com sucesso na tabela tbTransportadora!")

        if codparcs_inserted_successfully:
            print("\n--- Atualizando status AD_IMPORTADOGUARDIAN no Sankhya ---")
            for codparc in codparcs_inserted_successfully:
                update_sankhya_partner_status(codparc, bearer_token)
        else:
            print("Nenhum CODPARC válido para atualizar no Sankhya após a inserção.")

    except pyodbc.Error as ex:
        sql_conn.rollback()
        print(f"❌ Erro ao inserir dados no SQL Server: {ex}")
        print(f"Detalhes do erro: {ex.args[1]}")
    finally:
        cursor.close()

def get_guardian_partners(token, sql_conn):
    url = "https://api.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadView&outputType=json"

    print(f"\n--- Buscando parceiros da view 'VIEW_PARCEIROS_GUARDIAN' na API Sankhya ---")

    headers = {
        "Authorization": f"Bearer {token}"
    }

    payload = {
        "serviceName": "CRUDServiceProvider.loadView",
        "requestBody": {
            "query": {
                "viewName": "VIEW_PARCEIROS_GUARDIAN",
                "fields": {
                    "field": {
                        "$": "CODPARC,SKN_CODIGO,SKN_DECRICAO,SKN_RAZAOSOCIAL,SKN_CNPJ,SKN_INSCRICAO_ESTADUAL,SKN_ENDERECO,SKN_COMPLEMENTO,SKN_MUNICIPIO,SNK_EST_CODIGO,SNK_CEP,SNK_TELEFONE"
                    }
                },
                "offsetPage": "0",
                "maxRows": "500"
            }
        }
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()

        response_data = response.json()
        print("✅ Dados da API recebidos com sucesso!")

        records = response_data.get("responseBody", {}).get("records", {}).get("record", [])
        if records:
            insert_partners_into_sql(records, sql_conn, token)
        else:
            print("Nenhum registro de parceiro encontrado na resposta da API.")

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro na requisição de dados da API: {e}")
        if e.response is not None:
            print(f"Status Code: {e.response.status_code}")
            print(f"Resposta do servidor: {e.response.text}")

def perform_logout(token, appkey):
    url = "https://api.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=MobileLoginSP.logout&outputType=json"

    print("\n--- Encerrando a sessão (logout) da API Sankhya ---")

    headers = {
        "Authorization": f"Bearer {token}",
        "appkey": appkey,
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        response_data = response.json()
        if response_data.get("status") == "1":
            print("✅ Sessão da API Sankhya encerrada com sucesso!")
        else:
            print("⚠️ Logout da API Sankhya realizado, mas o status da resposta não foi o esperado.")
            print(json.dumps(response_data, indent=4, ensure_ascii=False))

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao tentar encerrar a sessão da API Sankhya: {e}")
        if e.response is not None:
            print(f"Status Code: {e.response.status_code}")
            print(f"Resposta do servidor: {e.response.text}")

# --- Nova função para envolver a lógica principal e capturar a saída ---
def run_integration_process():
    # Redireciona sys.stdout para um buffer em memória
    old_stdout = sys.stdout
    redirected_output = io.StringIO()
    sys.stdout = redirected_output

    try:
        credentials = load_credentials()
        bearer_token = perform_login(credentials)

        sql_connection = None

        if bearer_token:
            try:
                print(f"\n🔑 Bearer Token da API armazenado com sucesso.")
                sql_connection = connect_to_sql_server(credentials)
                if sql_connection:
                    get_guardian_partners(bearer_token, sql_connection)
            finally:
                if bearer_token:
                    perform_logout(bearer_token, credentials['appkey'])
                if sql_connection:
                    sql_connection.close()
                    print("Conexão com o SQL Server fechada.")
        else:
            print("\nFalha ao obter o Bearer Token da API. As próximas requisições não serão executadas.")
    except SystemExit: # Captura sys.exit() para evitar que o servidor Flask seja derrubado por um erro crítico
        print("\nProcesso encerrado devido a um erro crítico.")
    finally:
        # Restaura sys.stdout para o original
        sys.stdout = old_stdout
        # Retorna todo o conteúdo capturado do buffer
        return redirected_output.getvalue()

# --- Rotas Flask para a interface web ---

@app.route('/')
def index():
    # Página inicial com um botão para iniciar a integração
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Integração Guardian</title>
        <style>
            body { font-family: sans-serif; background-color: #f4f4f4; color: #333; margin: 0; padding: 20px; text-align: center;}
            .container { max-width: 800px; margin: 50px auto; background-color: #fff; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
            h1 { color: #007bff; margin-bottom: 20px; }
            p { margin-bottom: 30px; }
            button {
                background-color: #007bff;
                color: white;
                padding: 12px 25px;
                border: none;
                border-radius: 5px;
                cursor: pointer;
                font-size: 18px;
                transition: background-color 0.3s ease;
            }
            button:hover { background-color: #0056b3; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Executar Integração Guardian - Sankhya/SQL Server</h1>
            <p>Clique no botão abaixo para iniciar o processo de integração. O resultado detalhado será exibido nesta página.</p>
            <form action="/run_integration" method="get">
                <button type="submit">Iniciar Integração</button>
            </form>
        </div>
    </body>
    </html>
    """

@app.route('/run_integration')
def run_integration():
    # Executa o processo de integração e captura a saída
    output = run_integration_process()
    
    # Retorna a saída formatada como HTML pré-formatado para manter quebras de linha e espaços.
    # Adicionei um pouco de estilo para simular um terminal.
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Relatório de Integração Guardian</title>
        <style>
            body {{ font-family: monospace; background-color: #1e1e1e; color: #d4d4d4; padding: 20px; }}
            pre {{ white-space: pre-wrap; word-wrap: break-word; background-color: #000; padding: 15px; border-radius: 5px; overflow-x: auto; }}
            h1 {{ color: #007bff; }}
            button {{
                background-color: #007bff;
                color: white;
                padding: 10px 20px;
                border: none;
                border-radius: 5px;
                cursor: pointer;
                font-size: 16px;
                margin-bottom: 20px;
            }}
            button:hover {{ background-color: #0056b3; }}
        </style>
    </head>
    <body>
        <h1>Relatório de Integração Guardian</h1>
        <button onclick="window.location.href='/'">Voltar</button>
        <pre>{output}</pre>
    </body>
    </html>
    """

# Inicia o servidor Flask
if __name__ == '__main__':
    # debug=True é ótimo para desenvolvimento, pois reinicia o servidor
    # automaticamente em mudanças no código e mostra erros detalhados.
    # Para produção, você deve usar um servidor WSGI como Gunicorn ou Waitress.
    app.run(debug=True)