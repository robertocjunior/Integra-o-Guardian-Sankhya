import requests
import os
from dotenv import load_dotenv
import json
import sys
import pyodbc
from datetime import datetime

def load_credentials():
    """
    Carrega e valida as credenciais do arquivo .env.
    Retorna um dicionário com as credenciais ou encerra o script se alguma estiver faltando.
    """
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
    """
    Executa a requisição de login para a API Sankhya e trata a resposta.
    """
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
    """
    Estabelece uma conexão com o banco de dados SQL Server usando credenciais do .env.
    """
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
    """
    Atualiza o campo AD_IMPORTADOGUARDIAN para 'S' no Sankhya para um CODPARC específico.
    """
    url = "https://api.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=DatasetSP.save&outputType=json"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer_token}"
    }
    
    # Payload para atualizar o campo AD_IMPORTADOGUARDIAN
    # O "1" em "values":{"1":"S"} refere-se à segunda coluna (índice 1) na lista "fields"
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
                        "CODPARC": str(codparc) # CODPARC como string
                    },
                    "values": {
                        "1": "S" # Definindo 'S' para AD_IMPORTADOGUARDIAN
                    }
                }
            ]
        }
    }
    
    print(f"--- Tentando atualizar AD_IMPORTADOGUARDIAN para 'S' no Sankhya para CODPARC: {codparc} ---")
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status() # Lança exceção para erros HTTP
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

def insert_partners_into_sql(records, sql_conn, bearer_token): # <<<< Modificado: Recebe bearer_token
    """
    Insere os dados dos parceiros na tabela tbTransportadora do SQL Server,
    mapeando os campos da API para as colunas da tabela e adicionando TRP_DATA e TRP_ESTADO.
    Após a inserção bem-sucedida, atualiza o status no Sankhya.
    """
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
    
    # Lista para armazenar CODPARC dos registros inseridos com sucesso
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
            codparc_api = record.get("CODPARC", {}).get("$", None) # <<<< Extraindo CODPARC da API

            cursor.execute(
                insert_sql,
                trp_codigo, trp_descricao, trp_razao_social, trp_cnpj,
                trp_inscricao_estadual, trp_endereco, trp_complemento,
                trp_municipio, trp_est_codigo, trp_cep, trp_telefone, current_datetime, trp_estado_value
            )
            # Se a inserção individual no cursor não levantar erro, adicione à lista
            if codparc_api:
                codparcs_inserted_successfully.append(codparc_api)

        sql_conn.commit() # Confirma todas as alterações no banco de dados de uma vez
        print(f"✅ {len(records)} registros inseridos com sucesso na tabela tbTransportadora!")

        # <<<< Novo: Chamar a API Sankhya para cada registro inserido com sucesso
        if codparcs_inserted_successfully:
            print("\n--- Atualizando status AD_IMPORTADOGUARDIAN no Sankhya ---")
            for codparc in codparcs_inserted_successfully:
                update_sankhya_partner_status(codparc, bearer_token)
        else:
            print("Nenhum CODPARC válido para atualizar no Sankhya após a inserção.")

    except pyodbc.Error as ex:
        sql_conn.rollback() # Reverte a transação em caso de erro
        print(f"❌ Erro ao inserir dados no SQL Server: {ex}")
        print(f"Detalhes do erro: {ex.args[1]}")
    finally:
        cursor.close()

def get_guardian_partners(token, sql_conn):
    """
    Busca os dados da view 'VIEW_PARCEIROS_GUARDIAN' usando o serviço CRUDServiceProvider
    e tenta inseri-los no SQL Server.
    """
    url = "https://api.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadView&outputType=json"

    print(f"\n--- Buscando parceiros da view 'VIEW_PARCEIROS_GUARDIAN' na API Sankhya ---")

    headers = {
        "Authorization": f"Bearer {token}"
    }

    # <<<< Modificado: Payload para incluir CODPARC na consulta
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
            # <<<< Modificado: Passa o token também para insert_partners_into_sql
            insert_partners_into_sql(records, sql_conn, token)
        else:
            print("Nenhum registro de parceiro encontrado na resposta da API.")

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro na requisição de dados da API: {e}")
        if e.response is not None:
            print(f"Status Code: {e.response.status_code}")
            print(f"Resposta do servidor: {e.response.text}")

def perform_logout(token, appkey):
    """
    Encerra a sessão na API Sankhya (logout).
    """
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

if __name__ == "__main__":
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