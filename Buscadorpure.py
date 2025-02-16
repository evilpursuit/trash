from datetime import datetime

# Data limite para o script
VALID_UNTIL = datetime(2025, 12, 10)  # Exemplo de validade

# Verifica se o script ainda é válido
if datetime.now() > VALID_UNTIL:
    print("Sua licença expirou e seu programa não pode mais ser executado. Entre em contato com (41) 9 9599-9175 para reativar")
    exit()


import os
from typing import List, Dict, Tuple, Any
import sqlite3
import re
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import threading
import logging
import json
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# Configuração de logging
logging.basicConfig(filename='app_log.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')

db_lock = threading.Lock()

def load_cbo_data():
    cbo_list = []
    try:
        logging.debug("Tentando abrir arquivo CBO2002_LISTA.csv")
        with open('CBO2002_LISTA.csv', 'r', encoding='utf-8') as file:
            for line in file:
                try:
                    cbo, profissao = line.strip().split(',', 1)
                    # Remove hífens do CBO
                    cbo_clean = cbo.replace('-', '')
                    # Adiciona tanto a versão completa quanto a versão curta do CBO
                    cbo_short = cbo_clean[:4]
                    cbo_list.append((profissao, cbo_clean, cbo_short))
                except Exception as e:
                    logging.error(f"Erro ao processar linha do arquivo CBO: {e}")
                    continue
            
        result = sorted(set(cbo_list))  # Remove duplicatas e ordena
        logging.debug(f"Carregadas {len(result)} profissões únicas do arquivo CBO")
        return result
    except FileNotFoundError:
        logging.error("Arquivo CBO2002_LISTA.csv não encontrado")
        return []
    except Exception as e:
        logging.error(f"Erro ao carregar arquivo CBO: {e}")
        return []
        
def filter_profissoes(event=None):
    search_term = cbo_combobox.get().lower()
    if search_term:
        # Divide o termo de busca em palavras
        search_words = search_term.split()
        
        # Filtra profissões que contêm todas as palavras da busca
        filtered_profissoes = []
        for prof in cbo_profissoes:
            prof_lower = prof.lower()
            if all(word in prof_lower for word in search_words):
                filtered_profissoes.append(prof)
        
        cbo_combobox['values'] = filtered_profissoes
        
        if filtered_profissoes:
            if not cbo_combobox.winfo_ismapped():
                cbo_combobox.event_generate('<Down>')  # Mostra o dropdown apenas se ainda não estiver visível
    else:
        cbo_combobox['values'] = cbo_profissoes

    # Mantém o texto digitado pelo usuário
    current_text = cbo_combobox.get()
    cbo_combobox.select_clear()
    cbo_combobox.set(current_text)

def delayed_filter(event=None):
    # Cancela o timer anterior se existir
    if hasattr(cbo_combobox, '_timer'):
        window.after_cancel(cbo_combobox._timer)
    # Cria um novo timer
    cbo_combobox._timer = window.after(300, filter_profissoes)

def resource_path(relative_path: str, external: bool = False) -> str:
    if external:
        return os.path.join(".", relative_path)
    else:
        base_path = os.path.abspath(".")
        return os.path.join(base_path, relative_path)

json_file_path = resource_path('state_city_map.json')

try:
    with open(json_file_path, 'r', encoding='utf-8') as f:
        state_city_map = json.load(f)
        logging.debug("JSON carregado com sucesso.")
except json.JSONDecodeError as e:
    logging.error(f"Erro ao carregar JSON: {e}")
    state_city_map = {}

def connect_to_database(db_name: str, external: bool = False) -> sqlite3.Connection:
    external_dbs = ["Email.db", "Score.db", 
                    "SRS_TB_TSE.db", "SRS_TB_UNIVERSITARIOS.db"]
    if db_name in external_dbs:
        external = True
    db_path = resource_path(db_name, external)
    
    try:
        if not os.path.exists(db_path):
            logging.error(f"Banco de dados não encontrado: {db_path}")
            raise FileNotFoundError(f"Banco de dados não encontrado: {db_path}")
            
        connection = sqlite3.connect(db_path, timeout=30)
        logging.debug(f"Conectado ao banco de dados: {db_name} em {db_path}")
        return connection
    except sqlite3.DatabaseError as e:
        logging.error(f"Erro ao conectar ao banco de dados '{db_name}' em '{db_path}': {e}")
        raise
    except Exception as e:
        logging.error(f"Erro inesperado ao conectar ao banco de dados '{db_name}' em '{db_path}': {e}")
        raise

def table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        exists = cursor.fetchone() is not None
        logging.debug(f"Tabela {table_name} existe: {exists}")
        return exists
    except sqlite3.Error as e:
        logging.error(f"Erro ao verificar a existência da tabela {table_name}: {e}")
        return False

# Função de busca por universitários por UF
def fetch_universitarios_by_uf(uf: str) -> List[Dict[str, Any]]:
    """
    Função que busca universitários pelo estado (UF).
    :param uf: Código da UF (estado).
    :return: Lista de dicionários com os dados dos universitários encontrados.
    """
    if not uf:
        logging.error("UF não fornecida. A busca não pode ser realizada.")
        return []  # Retorna uma lista vazia se a UF estiver faltando

    with db_lock:
        connection = connect_to_database("SRS_TB_UNIVERSITARIOS.db")
        try:
            if not table_exists(connection, "SRS_TB_UNIVERSITARIOS"):
                raise Exception("A tabela 'SRS_TB_UNIVERSITARIOS' não existe no banco de dados.")
            
            cursor = connection.cursor()
            logging.debug(f"Iniciando busca por universitários no estado: {uf}")
            cursor.execute("""
                SELECT CONTATOS_ID, FACULDADE, CURSO, CAMPUS, ANO_VESTIBULAR, COTA, ANO_CONCLUSAO
                FROM SRS_TB_UNIVERSITARIOS WHERE UF = ?""", (uf,))
            
            result = cursor.fetchall()

            if not result:
                logging.debug(f"Nenhum universitário encontrado para o estado {uf}.")
                return []

            universitarios_data = []
            for row in result:
                try:
                    universitarios_data.append({
                        "CONTATOS_ID": row[0],
                        "FACULDADE": row[1],
                        "CURSO": row[2],
                        "CAMPUS": row[3].encode('latin-1').decode('utf-8', errors='ignore'),  # Ignora erros de codificação
                        "ANO_VESTIBULAR": row[4],
                        "COTA": row[5],
                        "ANO_CONCLUSAO": row[6]
                    })
                except UnicodeDecodeError as e:
                    logging.error(f"Erro de codificação ao processar o campo CAMPUS: {e}")
                    continue  # Continua para o próximo registro, ignorando o erro de codificação

            logging.debug(f"{len(universitarios_data)} universitários encontrados para o estado {uf}.")
            return universitarios_data

        except Exception as e:
            logging.error(f"Erro ao buscar universitários: {e}")
            return []
        finally:
            connection.close()

# Função de busca por faixa etária
def fetch_contatos_by_age_and_location(uf: str, city: str, sexo: str, year: int) -> List[int]:
    with db_lock:
        # Primeira busca: recuperar os contatos associados à cidade no banco Enderecos.db
        connection_enderecos = connect_to_database("Enderecos.db")
        try:
            if not table_exists(connection_enderecos, "srs_enderecos"):
                raise Exception("A tabela 'srs_enderecos' não existe no banco de dados 'Enderecos.db'.")
            
            cursor_enderecos = connection_enderecos.cursor()
            cursor_enderecos.execute("SELECT CONTATOS_ID FROM srs_enderecos WHERE UF = ? AND CIDADE = ?", (uf, city))
            contatos_ids = [row[0] for row in cursor_enderecos.fetchall()]

            if not contatos_ids:
                logging.debug(f"Nenhum contato encontrado para a cidade '{city}' no estado '{uf}'")
                return []
        finally:
            connection_enderecos.close()

        # Segunda busca: filtrar esses contatos pelo critério de faixa etária e sexo no banco Contatos.db
        connection_contatos = connect_to_database("Contatos.db")
        try:
            if not table_exists(connection_contatos, "SRS_CONTATOS"):
                raise Exception("A tabela 'SRS_CONTATOS' não existe no banco de dados 'Contatos.db'.")
            
            cursor_contatos = connection_contatos.cursor()

            # Para evitar problemas com grande número de variáveis em uma consulta, fazer em lotes
            valid_ids = []
            batch_size = 999  # Limite do SQLite
            for i in range(0, len(contatos_ids), batch_size):
                batch_ids = contatos_ids[i:i + batch_size]
                placeholders = ', '.join(['?'] * len(batch_ids))
                query = f"""
                    SELECT CONTATOS_ID, NASC, SEXO, DT_OB FROM SRS_CONTATOS
                    WHERE CONTATOS_ID IN ({placeholders})"""
                cursor_contatos.execute(query, (*batch_ids,))
                leads = cursor_contatos.fetchall()

                for lead in leads:
                    nasc_str = lead[1]  # Data de nascimento no formato 'YYYY-MM-DD HH:MM:SS'
                    sexo_contato = lead[2]  # O valor do campo SEXO (M/F)
                    obito_str = lead[3]  # Data de óbito, se houver
                    
                    # Verifica se a data de nascimento é válida
                    if nasc_str and len(nasc_str) >= 4:
                        try:
                            nasc_year = int(nasc_str[:4])
                            
                            # Verificar se o lead está falecido
                            if obito_str:
                                match_obito = re.search(r'\b(19[0-9]{2}|20[0-2][0-4])\b', obito_str)
                                if match_obito:
                                    logging.debug(f"CONTATOS_ID {lead[0]} está falecido: '{obito_str}'")
                                    continue  # Ignorar leads falecidos com data válida

                            # Verificar se a data de nascimento é antes do ano fornecido e o lead está vivo
                            if nasc_year < year:
                                # Aplicar o filtro de sexo
                                if sexo in ["M", "F"] and sexo_contato == sexo:
                                    valid_ids.append(lead[0])
                                elif sexo == "" or sexo == "Ambos":  # Quando ambos os sexos são aceitos
                                    valid_ids.append(lead[0])
                            else:
                                logging.debug(f"CONTATOS_ID {lead[0]} fora do critério de ano: '{nasc_str}'")
                        except ValueError as e:
                            logging.error(f"Erro ao converter a data de nascimento para CONTATOS_ID {lead[0]}: {e}")
                    else:
                        logging.debug(f"CONTATOS_ID {lead[0]} tem uma data de nascimento vazia ou inválida.")

            logging.debug(f"{len(valid_ids)} leads encontrados para cidade '{city}', UF '{uf}', nascidos antes de {year} com sexo '{sexo}'.")
            return valid_ids
        finally:
            connection_contatos.close()
            
# Função de busca por nomes via arquivo txt
def fetch_contatos_by_names(name_file_path: str, uf: str) -> List[int]:
    """
    Função que busca contatos por nome e UF de emissão (coluna UF_EMISSAO), 
    com base em um arquivo de texto que contém a lista de nomes.
    :param name_file_path: Caminho do arquivo de texto com nomes.
    :param uf: UF/Estado de emissão para filtrar os contatos.
    :return: Lista de IDs de contatos encontrados.
    """
    valid_ids = []
    with db_lock:
        connection_contatos = connect_to_database("Contatos.db")
        try:
            # Verifica se a tabela existe no banco de dados
            if not table_exists(connection_contatos, "SRS_CONTATOS"):
                raise Exception("A tabela 'SRS_CONTATOS' não existe.")

            cursor_contatos = connection_contatos.cursor()

            # Ler nomes do arquivo
            try:
                with open(name_file_path, 'r', encoding='utf-8') as file:
                    nomes = file.read().splitlines()
            except Exception as e:
                logging.error(f"Erro ao abrir o arquivo de nomes '{name_file_path}': {e}")
                return []

            # Buscar IDs dos contatos pelos nomes fornecidos, já filtrando pela UF_EMISSAO
            batch_size = 999  # Limite do SQLite
            for i in range(0, len(nomes), batch_size):
                batch_names = nomes[i:i + batch_size]
                placeholders = ', '.join(['?'] * len(batch_names))
                query = f"""
                    SELECT CONTATOS_ID FROM SRS_CONTATOS
                    WHERE NOME IN ({placeholders}) AND UF_EMISSAO = ?
                """
                cursor_contatos.execute(query, (*batch_names, uf))
                valid_ids.extend(row[0] for row in cursor_contatos.fetchall())

            logging.debug(f"{len(valid_ids)} leads encontrados para os nomes fornecidos e UF_EMISSAO '{uf}'.")
            return valid_ids

        except Exception as e:
            logging.error(f"Erro ao buscar contatos: {e}")
            return []
        finally:
            connection_contatos.close()

# Função de busca por bairro
def fetch_contatos_by_neighborhood(uf: str, city: str, neighborhood: str) -> List[int]:
    """
    Busca IDs de contatos com base no estado, cidade e bairro fornecidos.

    :param uf: Estado (UF)
    :param city: Nome da cidade
    :param neighborhood: Nome do bairro
    :return: Lista de IDs de contatos que correspondem aos critérios
    """
    with db_lock:
        connection = connect_to_database("Enderecos.db")
        try:
            if not table_exists(connection, "srs_enderecos"):
                raise Exception("A tabela 'srs_enderecos' não existe no banco de dados.")

            cursor = connection.cursor()
            cursor.execute("SELECT CONTATOS_ID FROM srs_enderecos WHERE UF = ? AND CIDADE = ? AND BAIRRO = ?", 
                           (uf, city, neighborhood))
            valid_ids = [row[0] for row in cursor.fetchall()]
            logging.debug(f"{len(valid_ids)} leads encontrados para a cidade '{city}', bairro '{neighborhood}', UF '{uf}'.")
            return valid_ids
        except Exception as e:
            logging.error(f"Erro ao buscar contatos: {e}")
            return []  # Retorna lista vazia em caso de erro
        finally:
            connection.close()

# Relatório para salvar dados com universitários
def save_universitarios_to_txt(data: Dict[int, Dict[str, Any]], universitarios_data: List[Dict[str, Any]], output_file: str) -> None:
    headers = [
        "CONTATOS_ID", "CPF", "NOME", "SEXO", "NASC", "CBO", "RENDA", "FAIXA_RENDA_ID",
        "LOGR_NOME1", "LOGR_NUMERO1", "LOGR_COMPLEMENTO1", "BAIRRO1", "CIDADE1", "CEP1",
        "LOGR_NOME2", "LOGR_NUMERO2", "LOGR_COMPLEMENTO2", "BAIRRO2", "CIDADE2", "CEP2",
        "CSB8", "CSB8_FAIXA", "CSBA", "CSBA_FAIXA",
        "FACULDADE", "CURSO", "CAMPUS", "ANO_VESTIBULAR", "COTA", "ANO_CONCULSAO"
    ]
    
    with open(output_file, 'w', encoding="utf-8") as f:
        f.write(';'.join(headers) + '\n')

        for contatos_id, values in data.items():
            row_data = [format_field(contatos_id)] + [format_field(values.get(key, "None")) for key in headers[1:]]
            
            # Adicionando dados dos universitários
            for univ_data in universitarios_data:
                if univ_data["CONTATOS_ID"] == contatos_id:
                    row_data.extend([univ_data.get("FACULDADE", "None"), univ_data.get("CURSO", "None"),
                                     univ_data.get("CAMPUS", "None"), univ_data.get("ANO_VESTIBULAR", "None"),
                                     univ_data.get("COTA", "None"), univ_data.get("ANO_CONCULSAO", "None")])
            f.write(';'.join(row_data) + '\n')

    logging.debug(f"Relatório salvo em '{output_file}' com dados de universitários.")

def fetch_contatos_id_by_cep(cep: str) -> List[int]:
    with db_lock:
        connection = connect_to_database("Enderecos.db")
        try:
            if not table_exists(connection, "srs_enderecos"):
                raise Exception("A tabela 'srs_enderecos' não existe no banco de dados 'Enderecos.db'.")

            cursor = connection.cursor()
            cursor.execute("SELECT CONTATOS_ID FROM srs_enderecos WHERE CEP = ?", (cep,))
            valid_ids = [row[0] for row in cursor.fetchall()]

            logging.debug(f"{len(valid_ids)} leads encontrados para o CEP '{cep}'")
            return valid_ids
        finally:
            connection.close()

def fetch_contatos_id_by_city_neighborhood_income(city: str, neighborhood: str, faixa_renda: str) -> List[int]:
    with db_lock:
        enderecos_connection = connect_to_database("Enderecos.db")
        contatos_connection = connect_to_database("Contatos.db")
        try:
            if not table_exists(enderecos_connection, "srs_enderecos"):
                raise Exception("A tabela 'srs_enderecos' não existe no banco de dados 'Enderecos.db'.")
            if not table_exists(contatos_connection, "SRS_CONTATOS"):
                raise Exception("A tabela 'SRS_CONTATOS' não existe no banco de dados 'Contatos.db'.")

            # Caso especial para o bairro "CENTRO"
            if neighborhood.upper() == "CENTRO":
                # Filtrar primeiro pela cidade
                enderecos_cursor = enderecos_connection.cursor()
                enderecos_cursor.execute("SELECT CONTATOS_ID FROM srs_enderecos WHERE CIDADE = ?", (city,))
                enderecos_ids = [row[0] for row in enderecos_cursor.fetchall()]
            else:
                # Filtrar primeiro pelo bairro
                enderecos_cursor = enderecos_connection.cursor()
                enderecos_cursor.execute("SELECT CONTATOS_ID FROM srs_enderecos WHERE BAIRRO = ?", (neighborhood,))
                enderecos_ids = [row[0] for row in enderecos_cursor.fetchall()]

            if not enderecos_ids:
                logging.debug(f"Nenhum lead encontrado para a cidade '{city}' e bairro '{neighborhood}'")
                return []

            # Filtrar os CONTATOS_IDs pela cidade
            filtered_enderecos_ids = []
            for contatos_id in enderecos_ids:
                enderecos_cursor.execute("SELECT CONTATOS_ID FROM srs_enderecos WHERE CONTATOS_ID = ? AND CIDADE = ?", 
                                         (contatos_id, city))
                result = enderecos_cursor.fetchone()
                if result:
                    filtered_enderecos_ids.append(result[0])

            if not filtered_enderecos_ids:
                logging.debug(f"Nenhum lead encontrado para a cidade '{city}' e bairro '{neighborhood}'")
                return []

            # Agora buscar na tabela SRS_CONTATOS baseando-se nos IDs filtrados e na faixa de renda
            valid_ids = []
            batch_size = 999  # SQLite geralmente suporta até 999 variáveis por consulta
            for i in range(0, len(filtered_enderecos_ids), batch_size):
                batch_ids = filtered_enderecos_ids[i:i + batch_size]
                placeholders = ', '.join(['?'] * len(batch_ids))
                query = f"SELECT CONTATOS_ID FROM SRS_CONTATOS WHERE CONTATOS_ID IN ({placeholders}) AND FAIXA_RENDA_ID = ?"
                contatos_cursor = contatos_connection.cursor()
                contatos_cursor.execute(query, (*batch_ids, faixa_renda))
                valid_ids.extend([row[0] for row in contatos_cursor.fetchall()])

            logging.debug(f"{len(valid_ids)} leads encontrados para a cidade '{city}', bairro '{neighborhood}', faixa de renda '{faixa_renda}'")
            return valid_ids
        finally:
            enderecos_connection.close()
            contatos_connection.close()

def fetch_contatos_id_by_risk_score_city_neighborhood(city: str, neighborhood: str, faixa_risco: str) -> List[int]:
    with db_lock:
        enderecos_connection = connect_to_database("Enderecos.db")
        scores_connection = connect_to_database("Score.db")
        try:
            if not table_exists(enderecos_connection, "srs_enderecos"):
                raise Exception("A tabela 'srs_enderecos' não existe no banco de dados 'Enderecos.db'.")
            if not table_exists(scores_connection, "SRS_TB_MODELOS_ANALYTICS_SCORE"):
                raise Exception("A tabela 'SRS_TB_MODELOS_ANALYTICS_SCORE' não existe no banco de dados 'Score.db'.")

            # Caso especial para o bairro "CENTRO"
            if neighborhood.upper() == "CENTRO":
                # Filtrar primeiro pela cidade
                enderecos_cursor = enderecos_connection.cursor()
                enderecos_cursor.execute("SELECT CONTATOS_ID FROM srs_enderecos WHERE CIDADE = ?", (city,))
                enderecos_ids = [row[0] for row in enderecos_cursor.fetchall()]
            else:
                # Filtrar primeiro pelo bairro
                enderecos_cursor = enderecos_connection.cursor()
                enderecos_cursor.execute("SELECT CONTATOS_ID FROM srs_enderecos WHERE BAIRRO = ?", (neighborhood,))
                enderecos_ids = [row[0] for row in enderecos_cursor.fetchall()]

            if not enderecos_ids:
                logging.debug(f"Nenhum lead encontrado para a cidade '{city}' e bairro '{neighborhood}'")
                return []

            # Filtrar os CONTATOS_IDs pela cidade
            filtered_enderecos_ids = []
            for contatos_id in enderecos_ids:
                enderecos_cursor.execute("SELECT CONTATOS_ID FROM srs_enderecos WHERE CONTATOS_ID = ? AND CIDADE = ?", 
                                         (contatos_id, city))
                result = enderecos_cursor.fetchone()
                if result:
                    filtered_enderecos_ids.append(result[0])

            if not filtered_enderecos_ids:
                logging.debug(f"Nenhum lead encontrado para a cidade '{city}' e bairro '{neighborhood}'")
                return []

            # Agora buscar na tabela Score.db baseando-se nos IDs filtrados e na faixa de risco
            valid_ids = []
            batch_size = 999  # SQLite geralmente suporta até 999 variáveis por consulta
            for i in range(0, len(filtered_enderecos_ids), batch_size):
                batch_ids = filtered_enderecos_ids[i:i + batch_size]
                placeholders = ', '.join(['?'] * len(batch_ids))
                query = f"SELECT CONTATOS_ID FROM SRS_TB_MODELOS_ANALYTICS_SCORE WHERE CONTATOS_ID IN ({placeholders}) AND CSBA_FAIXA = ?"
                scores_cursor = scores_connection.cursor()
                scores_cursor.execute(query, (*batch_ids, faixa_risco))
                valid_ids.extend([row[0] for row in scores_cursor.fetchall()])

            logging.debug(f"{len(valid_ids)} leads encontrados para a cidade '{city}', bairro '{neighborhood}', faixa de risco '{faixa_risco}'")
            return valid_ids
        finally:
            enderecos_connection.close()
            scores_connection.close()

def fetch_contatos_id_by_titulo_eleitor(titulo_eleitor: str) -> List[int]:
    with db_lock:
        connection = connect_to_database("SRS_TB_TSE.db")
        try:
            if not table_exists(connection, "SRS_TB_TSE"):
                raise Exception("A tabela 'SRS_TB_TSE' não existe no banco de dados 'SRS_TB_TSE.db'.")

            cursor = connection.cursor()
            cursor.execute("SELECT CONTATOS_ID FROM SRS_TB_TSE WHERE TITULO_ELEITOR = ?", (titulo_eleitor,))
            valid_ids = [row[0] for row in cursor.fetchall()]

            logging.debug(f"{len(valid_ids)} leads encontrados para o TÍTULO ELEITOR '{titulo_eleitor}'")
            return valid_ids
        finally:
            connection.close()

def fetch_contato_data_by_contatos_id(cursor: sqlite3.Cursor, contatos_id: int) -> Dict[int, Dict[str, Any]]:
    fields_to_search = ["CPF", "NOME", "SEXO", "NASC", "CBO", "RENDA", "FAIXA_RENDA_ID"]
    data = {}
    try:
        query = f"SELECT CONTATOS_ID, {', '.join(fields_to_search)} FROM SRS_CONTATOS WHERE CONTATOS_ID = ?"
        cursor.execute(query, (contatos_id,))
        result = cursor.fetchone()
        if result:
            data[contatos_id] = {field: result[i + 1] for i, field in enumerate(fields_to_search)}
        else:
            data[contatos_id] = {field: "None" for field in fields_to_search}
        return data
    except sqlite3.DatabaseError as e:
        logging.error(f"Erro ao buscar dados para CONTATOS_ID {contatos_id} no banco de dados Contatos.db: {e}")
        return {contatos_id: {field: "Erro" for field in fields_to_search}}
    except UnicodeDecodeError as e:
        logging.error(f"Erro de codificação ao buscar dados para CONTATOS_ID {contatos_id} no banco de dados Contatos.db: {e}")
        return {contatos_id: {field: "None" for field in fields_to_search}}
        
def fetch_contatos_id_by_city_cbo(uf: str, city: str, profissao: str) -> List[int]:
    """
    Busca IDs de contatos com base no estado, cidade e profissão selecionada.
    """
    # Encontra os CBOs correspondentes à profissão selecionada
    cbo_data = load_cbo_data()
    selected_cbos = []
    for item in cbo_data:
        if item[0] == profissao:
            selected_cbos.extend([item[1], item[2]])  # Adiciona tanto o CBO completo quanto o curto
    
    if not selected_cbos:
        logging.error(f"Nenhum CBO encontrado para a profissão: {profissao}")
        return []

    with db_lock:
        enderecos_connection = connect_to_database("Enderecos.db")
        contatos_connection = connect_to_database("Contatos.db")
        try:
            # Primeiro filtrar pela cidade e UF
            enderecos_cursor = enderecos_connection.cursor()
            enderecos_cursor.execute(
                "SELECT DISTINCT CONTATOS_ID FROM srs_enderecos WHERE CIDADE = ? AND UF = ?",
                (city, uf)
            )
            enderecos_ids = [row[0] for row in enderecos_cursor.fetchall()]

            if not enderecos_ids:
                logging.debug(f"Nenhum lead encontrado para a cidade '{city}' e UF '{uf}'")
                return []

            # Buscar na tabela SRS_CONTATOS pelos IDs filtrados e CBOs
            valid_ids = []
            batch_size = 999
            for i in range(0, len(enderecos_ids), batch_size):
                batch_ids = enderecos_ids[i:i + batch_size]
                placeholders_ids = ', '.join(['?'] * len(batch_ids))
                placeholders_cbos = ', '.join(['?'] * len(selected_cbos))
                query = f"""
                    SELECT CONTATOS_ID 
                    FROM SRS_CONTATOS 
                    WHERE CONTATOS_ID IN ({placeholders_ids}) 
                    AND CBO IN ({placeholders_cbos})
                """
                params = tuple(batch_ids) + tuple(selected_cbos)
                contatos_cursor = contatos_connection.cursor()
                contatos_cursor.execute(query, params)
                valid_ids.extend([row[0] for row in contatos_cursor.fetchall()])

            logging.debug(f"{len(valid_ids)} leads encontrados para a UF '{uf}', cidade '{city}', CBOs {selected_cbos}")
            return valid_ids
        except Exception as e:
            logging.error(f"Erro ao buscar contatos por CBO: {e}")
            return []
        finally:
            enderecos_connection.close()
            contatos_connection.close()
            
def fetch_telefones_by_cpf(cpf: str) -> Tuple[str, str]:
    """
    Busca os telefones a partir do CPF no banco de dados CSN_TELEFONES.db.
    :param cpf: CPF do lead.
    :return: Tupla com (telefone, telefoneSecundario).
    """
    with db_lock:
        connection = connect_to_database("Cadsus.db")
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT telefone, telefoneSecundario FROM datasus WHERE cpf = ?", (cpf,))
            result = cursor.fetchone()
            if result:
                return result  # Retorna os valores encontrados
            return ("None", "None")  # Retorna valores padrão se não encontrado
        except Exception as e:
            logging.error(f"Erro ao buscar telefones para CPF {cpf}: {e}")
            return ("Erro", "Erro")  # Retorna erro em caso de falha
        finally:
            connection.close()

def fetch_missing_data(cursor: sqlite3.Cursor, lead_data: Dict[int, Dict[str, Any]], contatos_id: int, field: str, table: str) -> None:
    try:
        if not table_exists(cursor.connection, table):
            return

        query = f"SELECT {field} FROM {table} WHERE CONTATOS_ID = ?"
        cursor.execute(query, (contatos_id,))
        results = cursor.fetchall()
        if results:
            lead_data[contatos_id][field] = [result[0] for result in results]
        else:
            lead_data[contatos_id][field] = ["None"]
    except sqlite3.DatabaseError as e:
        logging.error(f"Erro ao buscar dados do campo {field} para CONTATOS_ID {contatos_id} no banco de dados {table}: {e}")
        lead_data[contatos_id][field] = ["None"]
    except UnicodeDecodeError as e:
        logging.error(f"Erro de codificação ao buscar dados do campo {field} para CONTATOS_ID {contatos_id} no banco de dados {table}: {e}")
        lead_data[contatos_id][field] = ["None"]

def fetch_missing_address_data(cursor: sqlite3.Cursor, lead_data: Dict[int, Dict[str, Any]], contatos_id: int, index: int = 1) -> None:
    try:
        query = "SELECT LOGR_NOME, LOGR_NUMERO, LOGR_COMPLEMENTO, BAIRRO, CIDADE, CEP FROM srs_enderecos WHERE CONTATOS_ID = ?"
        cursor.execute(query, (contatos_id,))
        result = cursor.fetchone()
        address_fields = ["LOGR_NOME", "LOGR_NUMERO", "LOGR_COMPLEMENTO", "BAIRRO", "CIDADE", "CEP"]
        if result:
            for i, field in enumerate(address_fields):
                lead_data[contatos_id][f"{field}{index}"] = result[i] if result else "None"
        else:
            for field in address_fields:
                lead_data[contatos_id][f"{field}{index}"] = "None"
    except sqlite3.DatabaseError as e:
        logging.error(f"Erro ao buscar dados de endereço para CONTATOS_ID {contatos_id} no banco de dados Enderecos.db: {e}")
        for field in address_fields:
            lead_data[contatos_id][f"{field}{index}"] = "Erro"
    except UnicodeDecodeError as e:
        logging.error(f"Erro de codificação ao buscar dados de endereço para CONTATOS_ID {contatos_id} no banco de dados Enderecos.db: {e}")
        for field in address_fields:
            lead_data[contatos_id][f"{field}{index}"] = "None"

def fetch_scores_data(cursor: sqlite3.Cursor, lead_data: Dict[int, Dict[str, Any]], contatos_id: int) -> None:
    try:
        query = "SELECT CSB8, CSB8_FAIXA, CSBA, CSBA_FAIXA FROM SRS_TB_MODELOS_ANALYTICS_SCORE WHERE CONTATOS_ID = ?"
        cursor.execute(query, (contatos_id,))
        result = cursor.fetchone()
        if result:
            lead_data[contatos_id].update({
                "CSB8": result[0],
                "CSB8_FAIXA": result[1],
                "CSBA": result[2],
                "CSBA_FAIXA": result[3]
            })
        else:
            lead_data[contatos_id].update({
                "CSB8": "None",
                "CSB8_FAIXA": "None",
                "CSBA": "None",
                "CSBA_FAIXA": "None"
            })
    except sqlite3.DatabaseError as e:
        logging.error(f"Erro ao buscar dados de score para CONTATOS_ID {contatos_id} no banco de dados Score.db: {e}")
        lead_data[contatos_id].update({
            "CSB8": "Erro",
            "CSB8_FAIXA": "Erro",
            "CSBA": "Erro",
            "CSBA_FAIXA": "Erro"
        })
    except UnicodeDecodeError as e:
        logging.error(f"Erro de codificação ao buscar dados de score para CONTATOS_ID {contatos_id} no banco de dados Score.db: {e}")
        lead_data[contatos_id].update({
            "CSB8": "None",
            "CSB8_FAIXA": "None",
            "CSBA": "None",
            "CSBA_FAIXA": "None"
        })

def fetch_tse_data(cursor: sqlite3.Cursor, lead_data: Dict[int, Dict[str, Any]], contatos_id: int) -> None:
    try:
        query = "SELECT TITULO_ELEITOR, ZONA, SECAO FROM SRS_TB_TSE WHERE CONTATOS_ID = ?"
        cursor.execute(query, (contatos_id,))
        result = cursor.fetchone()
        if result:
            lead_data[contatos_id].update({
                "TITULO_ELEITOR": result[0],
                "ZONA": result[1],
                "SECAO": result[2]
            })
        else:
            lead_data[contatos_id].update({
                "TITULO_ELEITOR": "None",
                "ZONA": "None",
                "SECAO": "None"
            })
    except sqlite3.DatabaseError as e:
        logging.error(f"Erro ao buscar dados de título de eleitor para CONTATOS_ID {contatos_id} no banco de dados SRS_TB_TSE.db: {e}")
        lead_data[contatos_id].update({
            "TITULO_ELEITOR": "Erro",
            "ZONA": "Erro",
            "SECAO": "Erro"
        })
    except UnicodeDecodeError as e:
        logging.error(f"Erro de codificação ao buscar dados de título de eleitor para CONTATOS_ID {contatos_id} no banco de dados SRS_TB_TSE.db: {e}")
        lead_data[contatos_id].update({
            "TITULO_ELEITOR": "None",
            "ZONA": "None",
            "SECAO": "None"
        })

def format_field(value: Any) -> str:
    if isinstance(value, list) and value:
        return value[0]
    elif value is None:
        return "None"
    return str(value)

def save_to_txt(data: Dict[int, Dict[str, Any]], output_file: str) -> None:
    headers = [
        "CONTATOS_ID", "CPF", "NOME", "SEXO", "NASC", "CBO", "RENDA", "FAIXA_RENDA_ID",
        "LOGR_NOME1", "LOGR_NUMERO1", "LOGR_COMPLEMENTO1", "BAIRRO1", "CIDADE1", "CEP1",
        "LOGR_NOME2", "LOGR_NUMERO2", "LOGR_COMPLEMENTO2", "BAIRRO2", "CIDADE2", "CEP2",
        "CSB8", "CSB8_FAIXA", "CSBA", "CSBA_FAIXA",
        "TITULO_ELEITOR", "ZONA", "SECAO",
        "TELEFONE", "TELEFONE_SECUNDARIO"  # Novas colunas adicionadas
    ]
    ...
    for contatos_id, values in data.items():
        row_data = [
            format_field(contatos_id),
            format_field(values.get("CPF", "None")),
            format_field(values.get("NOME", "None")),
            format_field(values.get("TELEFONE", "None")),  # Adicionando telefone
            format_field(values.get("TELEFONE_SECUNDARIO", "None"))  # Adicionando telefone secundário
        ]
    
    for i in range(1, 7):
        headers.append(f"EMAIL{i}")

    for i in range(1, 7):
        headers.extend([f"DDD{i}", f"TELEFONE{i}"])

    with open(output_file, 'w', encoding="utf-8") as f:
        f.write(';'.join(headers) + '\n')

        for contatos_id, values in data.items():
            row_data = [
                format_field(contatos_id),
                format_field(values.get("CPF", "None")),
                format_field(values.get("NOME", "None")),
                format_field(values.get("SEXO", "None")),
                format_field(values.get("NASC", "None")),
                format_field(values.get("CBO", "None")),
                format_field(values.get("RENDA", "None")),
                format_field(values.get("FAIXA_RENDA_ID", "None")),
                format_field(values.get("LOGR_NOME1", "None")),
                format_field(values.get("LOGR_NUMERO1", "None")),
                format_field(values.get("LOGR_COMPLEMENTO1", "None")),
                format_field(values.get("BAIRRO1", "None")),
                format_field(values.get("CIDADE1", "None")),
                format_field(values.get("CEP1", "None")),
                format_field(values.get("LOGR_NOME2", "None")),
                format_field(values.get("LOGR_NUMERO2", "None")),
                format_field(values.get("LOGR_COMPLEMENTO2", "None")),
                format_field(values.get("BAIRRO2", "None")),
                format_field(values.get("CIDADE2", "None")),
                format_field(values.get("CEP2", "None")),
                format_field(values.get("CSB8", "None")),
                format_field(values.get("CSB8_FAIXA", "None")),
                format_field(values.get("CSBA", "None")),
                format_field(values.get("CSBA_FAIXA", "None")),
                format_field(values.get("TITULO_ELEITOR", "None")),
                format_field(values.get("ZONA", "None")),
                format_field(values.get("SECAO", "None"))
            ]
            
            emails = values.get("EMAIL", ["None"] * 6)
            for email in emails:
                row_data.append(format_field(email))
            for _ in range(6 - len(emails)):
                row_data.append("None")

            ddds = values.get("DDD", ["None"] * 6)
            telefones = values.get("TELEFONE", ["None"] * 6)
            for i in range(6):
                if i < len(ddds):
                    row_data.append(format_field(ddds[i]))
                else:
                    row_data.append("None")
                if i < len(telefones):
                    row_data.append(format_field(telefones[i]))
                else:
                    row_data.append("None")

            f.write(';'.join(row_data) + '\n')
    
    logging.debug(f"Relatório salvo em '{output_file}' com {len(data)} registros.")

def update_city_combobox(event: tk.Event) -> None:
    state = state_combobox.get().strip()  # Certifique-se de capturar o valor corretamente
    logging.debug(f"UF selecionada: {state}")  # Adicione log para depuração
    if state:
        cities = state_city_map.get(state, [])
        city_combobox['values'] = cities
    else:
        city_combobox['values'] = []

def process_contato_id(contatos_id: int) -> Dict[int, Dict[str, Any]]:
    lead_data = {}
    try:
        logging.debug(f"Iniciando processamento do CONTATOS_ID: {contatos_id}")
        
        # SRS_CONTATOS
        try:
            with connect_to_database("Contatos.db") as conn_contatos:
                cursor_contatos = conn_contatos.cursor()
                lead_data.update(fetch_contato_data_by_contatos_id(cursor_contatos, contatos_id))
        except Exception as e:
            logging.error(f"Erro ao processar SRS_CONTATOS para ID {contatos_id}: {e}")
            return {}

        # SRS_HISTORICO_TELEFONES
        try:
            with connect_to_database("Telefones.db") as conn_telefones:
                cursor_telefones = conn_telefones.cursor()
                fetch_missing_data(cursor_telefones, lead_data, contatos_id, "DDD", "SRS_HISTORICO_TELEFONES")
                fetch_missing_data(cursor_telefones, lead_data, contatos_id, "TELEFONE", "SRS_HISTORICO_TELEFONES")
        except Exception as e:
            logging.error(f"Erro ao processar SRS_HISTORICO_TELEFONES para ID {contatos_id}: {e}")
            lead_data[contatos_id]["DDD"] = ["None"]
            lead_data[contatos_id]["TELEFONE"] = ["None"]

        # SRS_EMAIL
        try:
            with connect_to_database("Email.db") as conn_email:
                cursor_email = conn_email.cursor()
                fetch_missing_data(cursor_email, lead_data, contatos_id, "EMAIL", "SRS_EMAIL")
        except Exception as e:
            logging.error(f"Erro ao processar SRS_EMAIL para ID {contatos_id}: {e}")
            lead_data[contatos_id]["EMAIL"] = ["None"]

        # Enderecos
        try:
            with connect_to_database("Enderecos.db") as conn_enderecos:
                cursor_enderecos = conn_enderecos.cursor()
                fetch_missing_address_data(cursor_enderecos, lead_data, contatos_id, 1)
                fetch_missing_address_data(cursor_enderecos, lead_data, contatos_id, 2)
        except Exception as e:
            logging.error(f"Erro ao processar Enderecos para ID {contatos_id}: {e}")
            # Definir valores padrão para campos de endereço

        # Score.db
        try:
            with connect_to_database("Score.db") as conn_scores:
                cursor_scores = conn_scores.cursor()
                fetch_scores_data(cursor_scores, lead_data, contatos_id)
        except Exception as e:
            logging.error(f"Erro ao processar Score.db para ID {contatos_id}: {e}")
            # Definir valores padrão para scores

        # SRS_TB_TSE
        try:
            with connect_to_database("SRS_TB_TSE.db") as conn_tse:
                cursor_tse = conn_tse.cursor()
                fetch_tse_data(cursor_tse, lead_data, contatos_id)
        except Exception as e:
            logging.error(f"Erro ao processar SRS_TB_TSE para ID {contatos_id}: {e}")
            # Definir valores padrão para dados do TSE

        logging.debug(f"Processamento concluído para CONTATOS_ID: {contatos_id}")
        return lead_data

    except Exception as e:
        logging.error(f"Erro geral ao processar CONTATOS_ID {contatos_id}: {e}")
        return {}

def process_contato_ids_chunk(contatos_ids_chunk: List[int]) -> Dict[int, Dict[str, Any]]:
    chunk_data = {}
    for contatos_id in contatos_ids_chunk:
        lead_data = process_contato_id(contatos_id)
        chunk_data.update(lead_data)
    return chunk_data

def search_leads() -> None:
    # Capturar e verificar o valor da UF (estado)
    state = state_combobox.get().strip()  # Certifique-se de capturar a UF corretamente
    logging.debug(f"Valor da UF selecionada (state): {state}")
    
    if not state and search_option.get() == "Universitarios":  # Verifica se a UF é obrigatória
        messagebox.showerror("Erro", "UF (estado) é obrigatório para busca por universitários.")
        logging.error("UF não fornecida. A busca não pode ser realizada.")
        return
    
    if not state and search_option.get() in ["CityNeighborhoodIncome", "RiskScoreCityNeighborhood", "Bairro"]:
        messagebox.showerror("Erro", "UF (estado) é obrigatório para a busca por cidade, bairro ou faixa de renda/risco.")
        logging.error("UF não fornecida. A busca não pode ser realizada.")
        return

    # Capturar os demais campos de entrada
    city = city_combobox.get().strip()
    neighborhood = neighborhood_entry.get().strip()
    faixa_renda = faixa_renda_combobox.get().strip()
    risco = risco_combobox.get().strip()
    cep = cep_entry.get().strip()
    titulo_eleitor = titulo_eleitor_entry.get().strip()
    output_prefix = output_prefix_entry.get().strip()
    cep_file_path = cep_file_path_entry.get().strip()
    titulo_eleitor_file_path = titulo_eleitor_file_path_entry.get().strip()
    nome_file_path = nomes_file_path_entry.get().strip()
    profissao = cbo_combobox.get().strip()
    
    lead_data = {}
    contatos_ids = []  # Inicializando a variável 'contatos_ids'
    
    # Logging da opção de busca selecionada
    logging.debug(f"Opção de busca selecionada: {search_option.get()}")

    # Validação de parâmetros obrigatórios
    if search_option.get() == "CEP" and not cep and not cep_file_path:
        messagebox.showerror("Erro", "CEP ou arquivo de CEPs é obrigatório para a busca por CEP.")
        return
    elif search_option.get() == "CityNeighborhoodIncome" and (not state or not city or not neighborhood or not faixa_renda):
        messagebox.showerror("Erro", "Estado, Cidade, Bairro e Faixa de Renda são campos obrigatórios.")
        return
    elif search_option.get() == "RiskScoreCityNeighborhood" and (not state or not city or not neighborhood or not risco):
        messagebox.showerror("Erro", "Estado, Cidade, Bairro e Faixa de Risco são campos obrigatórios.")
        return
    elif search_option.get() == "TituloEleitor" and not titulo_eleitor and not titulo_eleitor_file_path:
        messagebox.showerror("Erro", "Título de Eleitor ou arquivo de Títulos de Eleitor é obrigatório para a busca por Título de Eleitor.")
        return
    elif search_option.get() == "Bairro" and (not state or not city or not neighborhood):
        messagebox.showerror("Erro", "Estado, Cidade e Bairro são campos obrigatórios para a busca por Bairro.")
        return
    elif search_option.get() == "CityCBO" and (not state or not city or not profissao):
        messagebox.showerror("Erro", "Estado, Cidade e Profissão são campos obrigatórios para a busca por CBO.")
        return

    try:
        show_loading()  # Exibe o indicador de carregamento

        total_processed = 0
        file_count = 1

        # Processamento para busca por CEP
        if search_option.get() == "CEP":
            if cep_file_path:
                with open(cep_file_path, 'r') as file:
                    ceps = file.read().splitlines()
                for cep in ceps:
                    contatos_ids.extend(fetch_contatos_id_by_cep(cep))
            else:
                contatos_ids = fetch_contatos_id_by_cep(cep)
            messagebox.showinfo("Resultado", f"{len(contatos_ids)} LEADS encontrados para os CEPs fornecidos.")
            logging.debug(f"Iniciando processamento de {len(contatos_ids)} leads para os CEPs fornecidos")

        # Processamento para busca por Cidade, Bairro e Faixa de Renda
        elif search_option.get() == "CityNeighborhoodIncome":
            contatos_ids = fetch_contatos_id_by_city_neighborhood_income(city, neighborhood, faixa_renda)
            messagebox.showinfo("Resultado", f"{len(contatos_ids)} LEADS encontrados para a cidade '{city}', bairro '{neighborhood}' e faixa de renda '{faixa_renda}'")
            logging.debug(f"Iniciando processamento de {len(contatos_ids)} leads para a cidade '{city}', bairro '{neighborhood}' e faixa de renda '{faixa_renda}'")

        # Processamento para busca por Faixa de Risco, Cidade e Bairro
        elif search_option.get() == "RiskScoreCityNeighborhood":
            contatos_ids = fetch_contatos_id_by_risk_score_city_neighborhood(city, neighborhood, risco)
            messagebox.showinfo("Resultado", f"{len(contatos_ids)} LEADS encontrados para a cidade '{city}', bairro '{neighborhood}' e faixa de risco '{risco}'")
            logging.debug(f"Iniciando processamento de {len(contatos_ids)} leads para a cidade '{city}', bairro '{neighborhood}' e faixa de risco '{risco}'")

        # Processamento para busca por Faixa Etária
        elif search_option.get() == "FaixaEtaria":
            year = int(faixa_etaria_entry.get())
            sexo = sexo_combobox.get()
            contatos_ids = fetch_contatos_by_age_and_location(state, city, sexo, year)
            messagebox.showinfo("Resultado", f"{len(contatos_ids)} LEADS encontrados para a cidade '{city}', nascidos antes de {year}.")
            logging.debug(f"Iniciando processamento de {len(contatos_ids)} leads para a cidade '{city}', nascidos antes de {year}")

        # Processamento para busca por Título de Eleitor
        elif search_option.get() == "TituloEleitor":
            if titulo_eleitor_file_path:
                with open(titulo_eleitor_file_path, 'r') as file:
                    titulos = file.read().splitlines()
                for titulo in titulos:
                    contatos_ids.extend(fetch_contatos_id_by_titulo_eleitor(titulo))
            else:
                contatos_ids = fetch_contatos_id_by_titulo_eleitor(titulo_eleitor)
            messagebox.showinfo("Resultado", f"{len(contatos_ids)} LEADS encontrados para os Títulos de Eleitor fornecidos.")
            logging.debug(f"Iniciando processamento de {len(contatos_ids)} leads para os Títulos de Eleitor fornecidos")

        # Processamento para busca por Bairro
        elif search_option.get() == "Bairro":
            contatos_ids = fetch_contatos_by_neighborhood(state, city, neighborhood)
            messagebox.showinfo("Resultado", f"{len(contatos_ids)} LEADS encontrados para o bairro '{neighborhood}', cidade '{city}', UF '{state}'")
            logging.debug(f"Iniciando processamento de {len(contatos_ids)} leads para o bairro '{neighborhood}', cidade '{city}', UF '{state}'")

        # Processamento para busca por Nomes
        elif search_option.get() == "Nomes":
            if not nome_file_path:
                messagebox.showerror("Erro", "O arquivo de nomes é obrigatório para a busca por nomes.")
                return
            contatos_ids = fetch_contatos_by_names(nome_file_path, state)
            if not contatos_ids:
                messagebox.showinfo("Resultado", "Nenhum lead encontrado para os nomes fornecidos.")
                return
            messagebox.showinfo("Resultado", f"{len(contatos_ids)} LEADS encontrados para os nomes fornecidos.")
            logging.debug(f"Iniciando processamento de {len(contatos_ids)} leads para os nomes fornecidos")

        # Processamento para busca por Universitários
        elif search_option.get() == "Universitarios":
            universitarios_data = fetch_universitarios_by_uf(state)
            if not universitarios_data:
                messagebox.showinfo("Resultado", f"Nenhum universitário encontrado para a UF '{state}'")
                logging.debug(f"Nenhum universitário encontrado para a UF '{state}'.")
                return
            logging.debug(f"Processando {len(universitarios_data)} universitários encontrados para a UF '{state}'.")

            # Salvando o relatório dos universitários
            output_file = f"{output_prefix}_{state}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt"
            save_universitarios_to_txt({}, universitarios_data, output_file)
            messagebox.showinfo("Resultado", f"Relatório salvo com dados de universitários para '{state}'.")
            logging.debug(f"Relatório salvo com dados de universitários para '{state}' em '{output_file}'.")
        
        # Adicione este bloco para processar a busca por CBO
        elif search_option.get() == "CityCBO":
            if not state or not city or not profissao:
               messagebox.showerror("Erro", "Estado, Cidade e Profissão são campos obrigatórios para a busca por CBO.")
               return
    
            logging.debug(f"Iniciando busca por CBO com UF: {state}, Cidade: {city}, Profissão: {profissao}")
            contatos_ids = fetch_contatos_id_by_city_cbo(state, city, profissao)
            if not contatos_ids:
                messagebox.showinfo("Resultado", f"Nenhum lead encontrado para a UF '{state}', cidade '{city}', Profissão '{profissao}'")
                return
            messagebox.showinfo("Resultado", f"{len(contatos_ids)} LEADS encontrados para a UF '{state}', cidade '{city}', Profissão '{profissao}'")
            logging.debug(f"Iniciando processamento de {len(contatos_ids)} leads para a UF '{state}', cidade '{city}', Profissão '{profissao}'")

        # Processamento dos IDs de contatos encontrados
        if contatos_ids:
            chunk_size = 100
            total_processed = 0
            file_count = 1
            lead_data = {}

            try:
                with ProcessPoolExecutor() as executor:
                    futures = []
                    for i in range(0, len(contatos_ids), chunk_size):
                        chunk = contatos_ids[i:i + chunk_size]
                        futures.append(executor.submit(process_contato_ids_chunk, chunk))
                    
                    for future in as_completed(futures):
                        try:
                            chunk_data = future.result()
                            if chunk_data:  # Verifica se os dados não estão vazios
                                lead_data.update(chunk_data)
                                total_processed += len(chunk_data)

                                if total_processed % 10000 == 0:
                                    output_file = f"{output_prefix}_{file_count}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt"
                                    save_to_txt(lead_data, output_file)
                                    messagebox.showinfo("Resultado", f"Relatório intermediário salvo com {total_processed} registros.")
                                    logging.debug(f"Relatório intermediário salvo em '{output_file}' com {total_processed} registros.")
                                    lead_data.clear()
                                    file_count += 1
                        except Exception as e:
                            logging.error(f"Erro ao processar chunk de dados: {e}")
                            continue

                # Salvar relatório final
                if lead_data:
                    output_file = f"{output_prefix}_{file_count}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt"
                    save_to_txt(lead_data, output_file)
                    messagebox.showinfo("Resultado", f"Relatório final salvo com {total_processed} registros.")
                    logging.debug(f"Relatório final salvo em '{output_file}' com {total_processed} registros.")

            except Exception as e:
                logging.error(f"Erro durante o processamento em lote: {e}")
                messagebox.showerror("Erro", f"Erro durante o processamento em lote: {e}")

    except Exception as e:
        logging.error(f"Erro ao processar a busca de leads: {e}")
        messagebox.showerror("Erro", f"Erro ao processar a busca de leads: {e}")
    finally:
        hide_loading()  # Esconde o indicador de carregamento
        
def search_leads_threaded() -> None:
    threading.Thread(target=search_leads).start()

def is_valid_cep(cep: str) -> bool:
    pattern = r"^\d{8}$"
    return bool(re.match(pattern, cep))

def show_loading() -> None:
    loading_label.pack(pady=20)
    animate_loading()

def hide_loading() -> None:
    loading_label.pack_forget()

def animate_loading() -> None:
    frames = ["⏳", "⌛", "⌛"]
    def update_frame(idx: int = 0) -> None:
        frame = frames[idx % len(frames)]
        loading_label.config(text=frame)
        if loading_label.winfo_ismapped():
            window.after(500, update_frame, idx+1)
    update_frame()

def on_search_option_changed() -> None:
    # Esconder todos os frames de opções de busca
    cep_frame.pack_forget()
    state_frame.pack_forget()
    city_frame.pack_forget()
    neighborhood_frame.pack_forget()
    faixa_renda_frame.pack_forget()
    risco_frame.pack_forget()
    titulo_eleitor_frame.pack_forget()
    faixa_etaria_frame.pack_forget()
    universitarios_frame.pack_forget()
    nomes_frame.pack_forget()
    cbo_frame.pack_forget()

    # Mostrar o frame correspondente com base na opção de busca selecionada
    if search_option.get() == "CEP":
        cep_frame.pack(pady=10)
    elif search_option.get() == "CityNeighborhoodIncome":
        state_frame.pack(pady=10)
        city_frame.pack(pady=10)
        neighborhood_frame.pack(pady=10)
        faixa_renda_frame.pack(pady=10)
    elif search_option.get() == "RiskScoreCityNeighborhood":
        state_frame.pack(pady=10)
        city_frame.pack(pady=10)
        neighborhood_frame.pack(pady=10)
        risco_frame.pack(pady=10)
    elif search_option.get() == "TituloEleitor":
        state_frame.pack(pady=10)  # Adicionado para exibir o estado
        titulo_eleitor_frame.pack(pady=10)
    elif search_option.get() == "Nomes":
        state_frame.pack(pady=10)  # Adicionado para filtrar por UF/Estado
        nomes_frame.pack(pady=10)
    elif search_option.get() == "Bairro":
        state_frame.pack(pady=10)
        city_frame.pack(pady=10)
        neighborhood_frame.pack(pady=10)
    elif search_option.get() == "Universitarios":
        state_frame.pack(pady=10)
        universitarios_frame.pack(pady=10)
    elif search_option.get() == "FaixaEtaria":
        state_frame.pack(pady=10)
        city_frame.pack(pady=10)
        faixa_etaria_frame.pack(pady=10)
    elif search_option.get() == "CityCBO":
        state_frame.pack(pady=10)
        city_frame.pack(pady=10)
        cbo_frame.pack(pady=10)

def select_file(entry_field: ttk.Entry) -> None:
    file_path = filedialog.askopenfilename(filetypes=[("Text files", "*.txt")])
    if file_path:
        entry_field.delete(0, tk.END)
        entry_field.insert(0, file_path)

if __name__ == "__main__":
    multiprocessing.freeze_support()  # Garante suporte a congelamento para multiprocessamento

    window = tk.Tk()
    window.title("<AssemblyName>")
    window.geometry("800x800")

    # Criação do menu
    menu_bar = tk.Menu(window)
    window.config(menu=menu_bar)

    # Menu "Opções"
    options_menu = tk.Menu(menu_bar, tearoff=0)
    menu_bar.add_cascade(label="Opções", menu=options_menu)
    options_menu.add_command(label="Nova Consulta", command=lambda: messagebox.showinfo("Nova Consulta", "Iniciar uma nova consulta."))
    options_menu.add_command(label="Contrato", command=lambda: messagebox.showinfo("Contrato", "Uso exclusivo de <Nome> - Doc nº  <NumDoc>. Verifique os temos em Acesse: <LinkContrato>"))
    options_menu.add_command(label="Verificar Registro", command=lambda: messagebox.showinfo("Verificar Registro", "Uso exclusivo de <Nome> - Doc nº  <NumDoc>. Acesse <LinkContrato> e verifique as atualizações disponíveis e condições especiais para aquisição"))

    search_option = tk.StringVar(value="CEP")
    cep_frame = ttk.Frame(window)
    state_frame = ttk.Frame(window)
    city_frame = ttk.Frame(window)
    neighborhood_frame = ttk.Frame(window)
    faixa_renda_frame = ttk.Frame(window)
    risco_frame = ttk.Frame(window)
    titulo_eleitor_frame = ttk.Frame(window)
    faixa_etaria_frame = ttk.Frame(window)
    universitarios_frame = ttk.Frame(window)
    nomes_frame = ttk.Frame(window)
    cbo_frame = ttk.Frame(window)

    ttk.Label(window, text="Opção de Busca:").pack(pady=10)
    ttk.Radiobutton(window, text="CEP", variable=search_option, value="CEP", command=on_search_option_changed).pack()
    ttk.Radiobutton(window, text="Cidade, Bairro e Faixa de Renda", variable=search_option, value="CityNeighborhoodIncome", command=on_search_option_changed).pack()
    ttk.Radiobutton(window, text="Faixa de Risco, Cidade e Bairro", variable=search_option, value="RiskScoreCityNeighborhood", command=on_search_option_changed).pack()
    ttk.Radiobutton(window, text="Faixa Etária", variable=search_option, value="FaixaEtaria", command=on_search_option_changed).pack()
    ttk.Radiobutton(window, text="Universitários", variable=search_option, value="Universitarios", command=on_search_option_changed).pack()
    ttk.Radiobutton(window, text="Título de Eleitor", variable=search_option, value="TituloEleitor", command=on_search_option_changed).pack()
    ttk.Radiobutton(window, text="Nomes", variable=search_option, value="Nomes", command=on_search_option_changed).pack()
    ttk.Radiobutton(window, text="Bairro", variable=search_option, value="Bairro", command=on_search_option_changed).pack()
    ttk.Radiobutton(window, text="Cidade e Profissão", variable=search_option, value="CityCBO", command=on_search_option_changed).pack()

    ttk.Label(cep_frame, text="CEP (somente números):").pack(pady=10)
    cep_entry = ttk.Entry(cep_frame)
    cep_entry.pack()
    ttk.Label(cep_frame, text="ou selecione um arquivo de CEPs:").pack(pady=10)
    cep_file_path_entry = ttk.Entry(cep_frame, width=50)
    cep_file_path_entry.pack(side=tk.LEFT)
    cep_file_button = ttk.Button(cep_frame, text="Selecionar Arquivo", command=lambda: select_file(cep_file_path_entry))
    cep_file_button.pack(side=tk.LEFT, padx=10)

    ttk.Label(state_frame, text="Estado:").pack(pady=10)
    state_combobox = ttk.Combobox(state_frame, values=[state for state in state_city_map.keys() if state != "null" and state != "--"])
    state_combobox.pack()
    state_combobox.bind("<<ComboboxSelected>>", update_city_combobox)

    ttk.Label(city_frame, text="Cidade:").pack(pady=10)
    city_combobox = ttk.Combobox(city_frame)
    city_combobox.pack()

    ttk.Label(neighborhood_frame, text="Bairro:").pack(pady=10)
    neighborhood_entry = ttk.Entry(neighborhood_frame)
    neighborhood_entry.pack()

    ttk.Label(faixa_renda_frame, text="Faixa de Renda:").pack(pady=10)
    faixa_renda_combobox = ttk.Combobox(faixa_renda_frame, values=[str(i) for i in range(1, 13)])
    faixa_renda_combobox.pack()

    ttk.Label(risco_frame, text="Faixa de Risco:").pack(pady=10)
    risco_combobox = ttk.Combobox(risco_frame, values=["BAIXISSIMO RISCO", "BAIXO", "MEDIO", "ALTO", "ALTISSIMO"])
    risco_combobox.pack()

    ttk.Label(titulo_eleitor_frame, text="Título de Eleitor:").pack(pady=10)
    titulo_eleitor_entry = ttk.Entry(titulo_eleitor_frame)
    titulo_eleitor_entry.pack()
    ttk.Label(titulo_eleitor_frame, text="ou selecione um arquivo de Títulos de Eleitor:").pack(pady=10)
    titulo_eleitor_file_path_entry = ttk.Entry(titulo_eleitor_frame, width=50)
    titulo_eleitor_file_path_entry.pack(side=tk.LEFT)
    titulo_eleitor_file_button = ttk.Button(titulo_eleitor_frame, text="Selecionar Arquivo", command=lambda: select_file(titulo_eleitor_file_path_entry))
    titulo_eleitor_file_button.pack(side=tk.LEFT, padx=10)
    
    # Campos para faixa etária
    ttk.Label(faixa_etaria_frame, text="Nascido Antes de (Ano):").pack(pady=10)
    faixa_etaria_entry = ttk.Entry(faixa_etaria_frame)
    faixa_etaria_entry.pack()
    ttk.Label(faixa_etaria_frame, text="Selecione o Sexo:").pack(pady=10)
    sexo_combobox = ttk.Combobox(faixa_etaria_frame, values=["M", "F", "Ambos"])
    sexo_combobox.pack()

    # Campos para universitários
    ttk.Label(universitarios_frame, text="Selecione a UF:").pack(pady=10)
    universitarios_uf_combobox = ttk.Combobox(universitarios_frame, values=list(state_city_map.keys()))
    universitarios_uf_combobox.pack()

    # Campos para nomes
    ttk.Label(nomes_frame, text="Selecione o arquivo de Nomes:").pack(pady=10)
    nomes_file_path_entry = ttk.Entry(nomes_frame, width=50)
    nomes_file_path_entry.pack(side=tk.LEFT)
    nomes_file_button = ttk.Button(nomes_frame, text="Selecionar Arquivo", command=lambda: select_file(nomes_file_path_entry))
    nomes_file_button.pack(side=tk.LEFT, padx=10)
    
    # Campos para CBO
    ttk.Label(cbo_frame, text="Selecione a Profissão:").pack(pady=10)
    cbo_data = load_cbo_data()
    if not cbo_data:
        messagebox.showerror("Erro", "Não foi possível carregar a lista de profissões. Verifique se o arquivo CBO2002_LISTA.csv existe.")
        logging.error("Lista de profissões vazia")
    else:
        logging.debug(f"Carregadas {len(cbo_data)} profissões para o combobox")
        
    cbo_profissoes = [item[0] for item in cbo_data]  # Pega só as profissões
    cbo_combobox = ttk.Combobox(cbo_frame, values=cbo_profissoes, width=50)
    cbo_combobox.pack()

    # Adiciona o binding para a função de filtro
    cbo_combobox.bind('<KeyRelease>', delayed_filter)

    # Permite edição
    cbo_combobox['state'] = 'normal'

    # Adiciona uma dica para o usuário
    ttk.Label(cbo_frame, text="Digite para filtrar as profissões", font=("Helvetica", 8)).pack()

    # Adicione um log para verificar os valores
    logging.debug(f"Valores carregados no combobox: {cbo_profissoes[:5]}...")  # Mostra os 5 primeiros valores

    ttk.Label(window, text="Prefixo do arquivo de saída:").pack(pady=10)
    output_prefix_entry = ttk.Entry(window)
    output_prefix_entry.pack()

    ttk.Button(window, text="Buscar Leads", command=search_leads_threaded).pack(pady=20)

    loading_label = ttk.Label(window, text="", font=("Helvetica", 24))

    on_search_option_changed()

    window.mainloop()

