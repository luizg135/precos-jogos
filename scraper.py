import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time # Para adicionar um pequeno atraso entre as requisições
from fuzzywuzzy import fuzz # Importa fuzzywuzzy
import re # Para expressões regulares na limpeza de títulos
import gspread # Importa gspread para interagir com Google Sheets
import os # Para acessar variáveis de ambiente (secrets do GitHub Actions)
import json # Para ler as credenciais JSON do service account
from oauth2client.service_account import ServiceAccountCredentials # Novo import para a autenticação antiga
import traceback # Importa o módulo traceback para depuração de erros

# Você pode instalar python-levenshtein para melhor desempenho: pip install fuzzywuzzy python-levenshtein

# --- Configuração Global ---
# Limiar de semelhança: Apenas resultados com uma pontuação acima deste valor
# serão considerados como uma correspondência válida.
SIMILARITY_THRESHOLD = 70 # Alterado para 70% conforme sua solicitação

# --- Funções Utilitárias para Tratamento de Preços ---

def clean_price_to_float(price_str: str) -> float:
    """
    Converte uma string de preço (ex: "R$ 199,90", "Gratuito", "Preço indisponível") para um float.
    Retorna float('inf') para preços indisponíveis ou inválidos, e 0.0 para "Gratuito".
    """
    if not isinstance(price_str, str):
        return float('inf') # Trata tipos não-string (ex: NaN do Excel) como preço alto

    price_str = price_str.lower().strip()
    if "gratuito" in price_str:
        return 0.0
    if "preço indisponível" in price_str or "não encontrado" in price_str:
        return float('inf') # Representa um preço desconhecido/indisponível para comparação

    # Remove "R$", substitui vírgula por ponto, e remove outros caracteres não numéricos/ponto
    cleaned_price = price_str.replace("r$", "").replace(".", "").replace(",", ".").strip()
    try:
        # Tenta extrair apenas a parte numérica
        match = re.search(r'\d[\d\.]*', cleaned_price)
        if match:
            return float(match.group(0))
        return float('inf')
    except ValueError:
        return float('inf') # Retorna infinito se a conversão falhar

def format_float_to_price_str(price_float: float) -> str:
    """
    Converte um float de preço de volta para uma string formatada (ex: "R$ 199,90").
    """
    if price_float == 0.0:
        return "Gratuito"
    if price_float == float('inf'):
        return "Preço indisponível"
    # Formata para Real Brasileiro
    return f"R$ {price_float:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def _clean_game_title(title: str) -> str:
    """
    Remove plataforma, edição e outros sufixos comuns de um título de jogo
    para melhorar a correspondência fuzzy.
    """
    clean_title = title.lower()
    # Expressões regulares para remover palavras-chave comuns
    keywords_to_remove = [
        r'\bps4\b', r'\bps5\b', r'\bplaystation\b', r'\bdeluxe edition\b',
        r'\bspecial edition\b', r'\bstandard edition\b', r'\bultimate edition\b',
        r'\bremastered\b', r'\bgoty\b', r'\bgame of the year\b', r'\bedition\b',
        r'™', r'®' # Remove símbolos de marca registrada
    ]
    for keyword in keywords_to_remove:
        clean_title = re.sub(keyword, '', clean_title)
    
    # Remove conteúdo entre parênteses e colchetes
    clean_title = re.sub(r'\(.*?\)', '', clean_title)
    clean_title = re.sub(r'\[.*?\]', '', clean_title)
    
    # Remove espaços extras e espaços no início/fim
    clean_title = re.sub(r'\s+', ' ', clean_title).strip()
    return clean_title


# --- Classes dos Scrapers ---

class SteamScraper:
    """
    Scraper para buscar informações de jogos e preços na Steam.
    """
    BASE_URL = "https://store.steampowered.com/search/"

    def search_game_price(self, game_name: str) -> dict:
        """
        Busca o preço de um jogo específico na Steam, usando correspondência fuzzy
        e considerando os primeiros 5 resultados. Inclui um fallback para páginas individuais.
        """
        print(f"STEAM: Buscando por '{game_name}'...")
        params = {'term': game_name, 'l': 'brazilian', 'cc': 'br'}
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        
        # Cookies mais abrangentes para contornar a verificação de idade
        cookies = {
            'birthtime': '86400',  # 1 de Janeiro de 1970 em Unix timestamp
            'wants_mature_content': '1',
            'mature_content': '1'
        }

        best_match_element_from_search = None
        highest_score_from_search = 0

        try:
            response = requests.get(self.BASE_URL, params=params, headers=headers, cookies=cookies, timeout=15)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"ERRO STEAM: Falha de comunicação na busca para '{game_name}': {e}")
            return self._format_error("Falha de comunicação.")

        soup_search_results = BeautifulSoup(response.text, 'html.parser')
        search_results = soup_search_results.select("#search_resultsRows a")[:5]

        cleaned_game_name = _clean_game_title(game_name)

        if search_results:
            for result_element in search_results:
                title_element = result_element.select_one("span.title")
                if title_element:
                    result_title = title_element.text.strip()
                    cleaned_result_title = _clean_game_title(result_title)
                    score = fuzz.ratio(cleaned_game_name, cleaned_result_title)
                    
                    if score > highest_score_from_search:
                        highest_score_from_search = score
                        best_match_element_from_search = result_element
        
        # Se encontrou um bom match acima do limiar na página de busca, retorna.
        if best_match_element_from_search and highest_score_from_search >= SIMILARITY_THRESHOLD:
            title = best_match_element_from_search.select_one("span.title").text.strip()
            game_url = best_match_element_from_search['href']
            final_price_str = "Preço indisponível"
            discount_price_element = best_match_element_from_search.select_one(".search_price.discounted, .discount_final_price")
            if discount_price_element:
                price_text = discount_price_element.text.strip().split("R$")[-1].strip()
                final_price_str = f"R$ {price_text}" if price_text else "Preço indisponível"
            else:
                regular_price_element = best_match_element_from_search.select_one(".search_price")
                if regular_price_element:
                    price_text = regular_price_element.text.strip().split("R$")[-1].strip()
                    final_price_str = f"R$ {price_text}" if price_text else "Preço indisponível"
            
            return {
                "found": True,
                "title": title,
                "price_str": final_price_str,
                "price_float": clean_price_to_float(final_price_str),
                "url": game_url,
                "similarity_score": highest_score_from_search
            }
        
        # --- FALLBACK: Se a busca inicial falhou ou foi censurada, tentar acessar o primeiro link diretamente ---
        print(f"  STEAM: Busca inicial falhou ou semelhança baixa ({highest_score_from_search}%). Tentando fallback para o primeiro link...")
        
        first_possible_link = soup_search_results.select_one("#search_resultsRows a")
        if first_possible_link and 'href' in first_possible_link.attrs:
            game_page_url = first_possible_link['href']
            
            # --- DEBUG: Imprime a URL do fallback e o status da requisição ---
            print(f"  STEAM DEBUG: URL do fallback: {game_page_url}")
            try:
                response_game_page = requests.get(game_page_url, headers=headers, cookies=cookies, timeout=15)
                print(f"  STEAM DEBUG: Status da requisição da página do jogo: {response_game_page.status_code}")
                response_game_page.raise_for_status()
            except requests.RequestException as e:
                print(f"  ERRO STEAM: Falha de comunicação na página do jogo '{game_name}' ({game_page_url}): {e}")
                return self._format_error("Falha de comunicação no fallback.")

            soup_game_page = BeautifulSoup(response_game_page.text, 'html.parser')
            # --- DEBUG: Imprime um trecho do HTML da página do jogo ---
            print(f"  STEAM DEBUG: Snippet do HTML da página do jogo (primeiros 500 caracteres):")
            print(response_game_page.text[:500])
            # --- FIM DEBUG ---


            # Tenta extrair título e preço da página do jogo
            # Seletores comuns para título e preço em uma página de jogo Steam
            game_page_title_element = soup_game_page.select_one("div.apphub_AppName, div.game_title_area h1, #appHubAppName")
            game_page_price_element = soup_game_page.select_one(".game_purchase_price, .discount_block .discount_final_price, .price_discount .discount_final_price")

            fallback_title = "Título indisponível"
            fallback_price_str = "Preço indisponível"
            fallback_score = 0

            if game_page_title_element:
                fallback_title = game_page_title_element.text.strip()
                cleaned_fallback_title = _clean_game_title(fallback_title)
                fallback_score = fuzz.ratio(cleaned_game_name, cleaned_fallback_title)

            if game_page_price_element:
                price_text = game_page_price_element.text.strip().split("R$")[-1].strip()
                fallback_price_str = f"R$ {price_text}" if price_text else "Preço indisponível"
            
            # Se o fallback encontrou um título e a similaridade é aceitável, retorna
            if fallback_title != "Título indisponível" and fallback_score >= SIMILARITY_THRESHOLD:
                print(f"  STEAM: Fallback bem-sucedido para '{fallback_title}' (Semelhança: {fallback_score}%).")
                return {
                    "found": True,
                    "title": fallback_title,
                    "price_str": fallback_price_str,
                    "price_float": clean_price_to_float(fallback_price_str),
                    "url": game_page_url,
                    "similarity_score": fallback_score
                }
            else:
                 print(f"  STEAM: Fallback falhou para '{game_name}'. Título do jogo: '{fallback_title}', Semelhança: {fallback_score}%.")
                 return self._format_error(f"Jogo não encontrado ou semelhança muito baixa ({highest_score_from_search}% na busca e {fallback_score}% no fallback).")

        return self._format_error(f"Jogo não encontrado ou semelhança muito baixa ({highest_score_from_search}%).")


    def _format_error(self, message: str) -> dict:
        """
        Formata um dicionário de erro para resultados da Steam.
        """
        return {
            "found": False,
            "title": None,
            "price_str": message,
            "price_float": float('inf'), # Sinaliza um preço muito alto para não ser o menor histórico
            "url": None,
            "similarity_score": 0 # Semelhança 0 em caso de erro/não encontrado
        }


class PsnScraper:
    """
    Scraper para buscar informações de jogos e preços na PlayStation Store.
    """
    BASE_URL = "https://store.playstation.com/pt-br/search/"

    def search_game_price(self, game_name: str) -> dict:
        """
        Busca o preço de um jogo específico na PSN, usando correspondência fuzzy
        e considerando os primeiros 5 resultados.
        """
        print(f"PSN: Buscando por '{game_name}'...")
        # Formata o nome do jogo para a URL da PSN (espaços por %20)
        formatted_game_name = game_name.replace(' ', '%20')
        search_url = f"{self.BASE_URL}{formatted_game_name}"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        try:
            response = requests.get(search_url, headers=headers, timeout=15)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"ERRO PSN: Falha de comunicação para '{game_name}': {e}")
            return self._format_error("Falha de comunicação.")

        soup = BeautifulSoup(response.content, 'html.parser')

        # Pega todos os potenciais tiles de produto na página de resultados, limitado a 5
        all_product_tiles = soup.find_all('div', class_='psw-product-tile')[:5]
        
        best_match_tile = None
        highest_score = 0
        game_url = search_url # Default URL, pode ser atualizada se encontrar um link específico

        cleaned_game_name = _clean_game_title(game_name)

        # Primeira tentativa: verificar se é uma página de jogo direto (redirecionamento)
        page_title_tag = soup.find('h1', class_='psw-m-t-2xs psw-t-title-l psw-l-line-break-m') or \
                         soup.find('h1', class_='psw-p-t-xs')
        if page_title_tag:
             page_title = page_title_tag.text.strip()
             cleaned_page_title = _clean_game_title(page_title)
             score = fuzz.ratio(cleaned_game_name, cleaned_page_title)
             if score >= SIMILARITY_THRESHOLD:
                best_match_tile = soup 
                highest_score = score
                # A URL já é a search_url para o caso de redirecionamento direto
        
        # Se encontrou múltiplos tiles, aplica correspondência fuzzy
        if all_product_tiles:
            for tile in all_product_tiles:
                title_tag = tile.find('span', class_='psw-t-body') or tile.find('span', class_='psw-h5')
                if title_tag:
                    result_title = title_tag.text.strip()
                    cleaned_result_title = _clean_game_title(result_title)
                    score = fuzz.ratio(cleaned_game_name, cleaned_result_title)
                    
                    if score > highest_score:
                        highest_score = score
                        best_match_tile = tile
                        # Tenta extrair a URL específica do tile
                        link_tag = tile.find('a', class_='psw-top-left psw-bottom-right psw-stretched-link')
                        if link_tag and 'href' in link_tag.attrs:
                            game_url = "https://store.playstation.com" + link_tag['href']
                        elif tile.name == 'a' and 'href' in tile.attrs:
                            game_url = "https://store.playstation.com" + tile['href']

        # Se não encontrou uma boa correspondência acima do limiar
        if not best_match_tile or highest_score < SIMILARITY_THRESHOLD:
            return self._format_error(f"Jogo não encontrado ou semelhança muito baixa ({highest_score}%).")


        title = 'Nome não encontrado'
        price_str = 'Preço indisponível'
        
        # Extrai o título do melhor tile
        if best_match_tile == soup:
            temp_title_tag = soup.find('h1', class_='psw-m-t-2xs psw-t-title-l psw-l-line-break-m') or \
                             soup.find('h1', class_='psw-p-t-xs')
            if temp_title_tag:
                title = temp_title_tag.text.strip()
            # E o preço da página principal
            temp_price_element = soup.find('span', class_='psw-m-r-3') or \
                                 soup.find('span', class_='psw-l-line-through') or \
                                 soup.find('span', class_='psw-h5')
            if temp_price_element:
                price_str = temp_price_element.text.strip()
        else: # Se best_match_tile é um tile específico
            title_tag = best_match_tile.find('span', class_='psw-t-body') or best_match_tile.find('span', class_='psw-h5')
            if title_tag:
                title = title_tag.text.strip()

            # Extrai o preço do melhor tile
            price_element = best_match_tile.find('span', class_='psw-m-r-3') # Preço atual/promoção
            if not price_element:
                price_element = best_match_tile.find('span', class_='psw-l-line-through') # Preço original riscado (se houver desconto)
            if not price_element:
                price_element = best_match_tile.find('span', class_='psw-h5') # Outro seletor possível
            if price_element:
                price_str = price_element.text.strip()
        
        return {
            "found": True,
            "title": title,
            "price_str": price_str,
            "price_float": clean_price_to_float(price_str),
            "url": game_url,
            "similarity_score": highest_score
        }

    def _format_error(self, message: str) -> dict:
        """
        Formata um dicionário de erro para resultados da PSN.
        """
        return {
            "found": False,
            "title": None,
            "price_str": message,
            "price_float": float('inf'),
            "url": None,
            "similarity_score": 0 # Semelhança 0 em caso de erro/não encontrado
        }


# --- Lógica Principal do Script ---

# Cache global para planilhas e dados (MOVIDO PARA CIMA PARA GARANTIR DEFINIÇÃO GLOBAL)
_sheet_cache = {}
_data_cache = {}
_cache_ttl_seconds = 300 # Tempo de vida do cache em segundos (5 minutos)
_last_cache_update = {}

# Configuração da URL da planilha (usando a mesma variável de ambiente do seu API)
# Esta classe de Config simula a leitura das variáveis de ambiente para o script
# Assim, o script pode usar os mesmos nomes de variáveis que sua API.
class PriceTrackerConfig:
    GOOGLE_SHEETS_CREDENTIALS_JSON = os.environ.get('GSPREAD_SERVICE_ACCOUNT_CREDENTIALS') # Use o nome do secret do Price Tracker
    if not GOOGLE_SHEETS_CREDENTIALS_JSON:
        print("CRITICAL ERROR: 'GSPREAD_SERVICE_ACCOUNT_CREDENTIALS' environment variable is not set!")

    # Usaremos GOOGLE_SHEET_URL para o método de acesso antigo
    GOOGLE_SHEET_URL = os.environ.get('GOOGLE_SHEET_URL') # Novo secret para o Price Tracker
    if not GOOGLE_SHEET_URL:
        print("CRITICAL ERROR: 'GOOGLE_SHEET_URL' environment variable is not set!")


# --- NOVO: Função auxiliar para converter número de coluna para letra ---
def _col_to_char(col_num: int) -> str:
    """
    Converte um número de coluna (1-based) para sua representação em letra (A, B, ..., Z, AA, AB, ...).
    Esta função substitui gspread.utils.col_to_char, que foi removida em versões recentes do gspread.
    """
    string = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        string = chr(65 + remainder) + string
    return string
# --- FIM NOVO ---


def _get_sheet_for_price_tracker(sheet_name):
    """
    Retorna o objeto da planilha (worksheet) para o Price Tracker, usando cache.
    Autentica com as credenciais da conta de serviço lidas de uma variável de ambiente,
    e abre a planilha pela URL, conforme o sistema da sua API.
    """
    global _sheet_cache # Declarar como global
    if sheet_name in _sheet_cache:
        return _sheet_cache[sheet_name]
    
    try:
        credentials_json = PriceTrackerConfig.GOOGLE_SHEETS_CREDENTIALS_JSON
        if not credentials_json:
            print("CRITICAL ERROR (PriceTracker): GOOGLE_SHEETS_CREDENTIALS environment variable is not set in Config.")
            return None
        
        google_sheet_url = PriceTrackerConfig.GOOGLE_SHEET_URL
        if not google_sheet_url:
            print("CRITICAL ERROR (PriceTracker): GOOGLE_SHEET_URL environment variable is not set in Config.")
            return None

        # --- DEBUG: Imprime a URL que está sendo usada ---
        print(f"DEBUG (PriceTracker): Google Sheet URL being used: {google_sheet_url}")
        # --- FIM DEBUG ---

        # Carregar as credenciais do JSON
        creds_dict = json.loads(credentials_json)
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        
        # Autorizar o cliente gspread com as credenciais
        gc = gspread.authorize(creds)
        
        print("DEBUG (PriceTracker): Type of 'gc' object after authorize: ", type(gc))
        print(f"DEBUG (PriceTracker): gspread version: {gspread.__version__}")

        # Usar open_by_url para acessar a planilha
        spreadsheet = gc.open_by_url(google_sheet_url)
        worksheet = spreadsheet.worksheet(sheet_name)
        _sheet_cache[sheet_name] = worksheet
        
        print(f"DEBUG (PriceTracker): Successfully opened spreadsheet by URL and worksheet '{sheet_name}'.")
        return worksheet
    except Exception as e:
        print(f"Erro ao autenticar ou abrir planilha '{sheet_name}' no Price Tracker: {e}"); traceback.print_exc()
        return None

def _get_data_from_sheet_for_price_tracker(sheet_name):
    """Retorna os dados da planilha para o Price Tracker, usando cache com TTL."""
    global _data_cache, _last_cache_update # Declarar como global
    current_time = datetime.now()
    if sheet_name in _data_cache and \
       (current_time - _last_cache_update.get(sheet_name, datetime.min)).total_seconds() < _cache_ttl_seconds:
        print(f"Dados da planilha '{sheet_name}' servidos do cache no Price Tracker.")
        return _data_cache[sheet_name]

    sheet = _get_sheet_for_price_tracker(sheet_name)
    if not sheet:
        return []

    try:
        data = sheet.get_all_records()
        _data_cache[sheet_name] = data
        _last_cache_update[sheet_name] = current_time
        print(f"Dados da planilha '{sheet_name}' atualizados do Google Sheets e armazenados em cache no Price Tracker.")
        return data
    except gspread.exceptions.APIError as e:
        if "unable to parse range" in str(e): 
            print(f"AVISO (PriceTracker): Planilha '{sheet_name}' vazia ou com erro de range, retornando lista vazia. Detalhes: {e}")
            return []
        print(f"Erro ao ler dados da planilha '{sheet_name}' no Price Tracker: {e}"); traceback.print_exc()
        return []
    except Exception as e:
        print(f"Erro genérico ao ler dados da planilha '{sheet_name}' no Price Tracker: {e}"); traceback.print_exc()
        return []

def _invalidate_cache(sheet_name):
    """Invalida o cache para uma planilha específica."""
    global _data_cache # Declarar como global
    if sheet_name in _data_cache:
        del _data_cache[sheet_name]
        print(f"Cache para a planilha '{sheet_name}' invalidado.")


def run_scraper(google_sheet_url: str, worksheet_name: str = 'Desejos'):
    """
    Função principal que orquestra a leitura da planilha do Google Sheets, o scraping e a atualização.
    """
    steam_scraper = SteamScraper()
    psn_scraper = PsnScraper()
    current_date = datetime.now().strftime('%Y-%m-%d') # Data atual para registro

    try:
        # Define a variável de ambiente para a URL da planilha para a classe PriceTrackerConfig
        os.environ['GOOGLE_SHEET_URL'] = google_sheet_url

        # Lê os dados da planilha
        data = _get_data_from_sheet_for_price_tracker(worksheet_name)
        if not data:
            print(f"ERRO: Não foi possível carregar dados da planilha '{worksheet_name}'. Verifique o ID/URL da planilha e permissões.")
            return

        df = pd.DataFrame(data)

        if 'Nome' not in df.columns:
            print(f"Erro: A planilha '{worksheet_name}' não possui a coluna 'Nome'.")
            print("Certifique-se de que a primeira coluna com os nomes dos jogos esteja nomeada exatamente 'Nome'.")
            return

        # Define as colunas que serão preenchidas no Google Sheets
        target_gsheet_columns = [
            'Steam Preco Atual',
            'Steam Menor Preco Historico',
            'PSN Preco Atual',
            'PSN Menor Preco Historico',
            'Ultima Atualizacao'
        ]
        
        # Garante que as colunas existam no DataFrame para manipulação
        for col in target_gsheet_columns:
            if col not in df.columns:
                df[col] = 'Preço indisponível' # Valor padrão para novas colunas

        # Pega os cabeçalhos da planilha para encontrar os índices das colunas target
        # Usamos _get_sheet_for_price_tracker aqui para garantir que estamos usando o método de acesso correto
        gsheet_worksheet = _get_sheet_for_price_tracker(worksheet_name)
        if not gsheet_worksheet:
            print(f"ERRO: Não foi possível obter o objeto da planilha para {worksheet_name}.")
            return

        gsheet_headers = gsheet_worksheet.row_values(1)
        col_indices = {}
        # Garante que todas as colunas de destino existam na planilha, adicionando se necessário.
        for col_name in target_gsheet_columns:
            if col_name not in gsheet_headers:
                print(f"Adicionando coluna '{col_name}' à planilha do Google Sheets.")
                gsheet_headers.append(col_name)
                # Atualiza apenas a célula do cabeçalho
                gsheet_worksheet.update_cell(1, len(gsheet_headers), col_name)
            col_indices[col_name] = gsheet_headers.index(col_name) + 1 # gspread é 1-based

        # Itera sobre cada jogo na planilha
        for index, row in df.iterrows():
            game_name = row['Nome']
            if pd.isna(game_name) or str(game_name).strip() == '':
                print(f"\nPulando linha {index + 2}: Nome do jogo vazio.")
                continue

            print(f"\nProcessando jogo: {game_name}")

            # --- Busca na Steam ---
            steam_result = steam_scraper.search_game_price(game_name)
            df.at[index, 'Steam Preco Atual'] = steam_result['price_str']
            
            current_steam_price_float = steam_result['price_float']
            historical_steam_price_str = df.at[index, 'Steam Menor Preco Historico']
            historical_steam_price_float = clean_price_to_float(historical_steam_price_str)

            if current_steam_price_float < historical_steam_price_float:
                df.at[index, 'Steam Menor Preco Historico'] = steam_result['price_str']
                print(f"  STEAM: Novo menor preço histórico para '{game_name}': {steam_result['price_str']} (Semelhança: {steam_result['similarity_score']}%)")
            elif historical_steam_price_float == float('inf') and steam_result['found']:
                 df.at[index, 'Steam Menor Preco Historico'] = steam_result['price_str']
                 print(f"  STEAM: Primeiro preço registrado para '{game_name}': {steam_result['price_str']} (Semelhança: {steam_result['similarity_score']}%)")
            else:
                 print(f"  STEAM: Preço atual para '{game_name}': {steam_result['price_str']} (Semelhança: {steam_result['similarity_score']}%)")


            # --- Busca na PSN ---
            psn_result = psn_scraper.search_game_price(game_name)
            df.at[index, 'PSN Preco Atual'] = psn_result['price_str']

            current_psn_price_float = psn_result['price_float']
            historical_psn_price_str = df.at[index, 'PSN Menor Preco Historico']
            historical_psn_price_float = clean_price_to_float(historical_psn_price_str)

            if current_psn_price_float < historical_psn_price_float:
                df.at[index, 'PSN Menor Preco Historico'] = psn_result['price_str']
                print(f"  PSN: Novo menor preço histórico para '{game_name}': {psn_result['price_str']} (Semelhança: {psn_result['similarity_score']}%)")
            elif historical_psn_price_float == float('inf') and psn_result['found']:
                 df.at[index, 'PSN Menor Preco Historico'] = psn_result['price_str']
                 print(f"  PSN: Primeiro preço registrado para '{game_name}': {psn_result['price_str']} (Semelhança: {psn_result['similarity_score']}%)")
            else:
                 print(f"  PSN: Preço atual para '{game_name}': {psn_result['price_str']} (Semelhança: {psn_result['similarity_score']}%)")
            
            df.at[index, 'Ultima Atualizacao'] = current_date

            time.sleep(1) # Pequeno atraso para evitar sobrecarregar os servidores

        # --- Atualiza o Google Sheet ---
        # Prepara os dados para atualização (apenas as colunas modificadas)
        start_row = 2 # Começa na linha 2 (abaixo do cabeçalho)
        
        updates = []
        for r_idx, row_df in df.iterrows():
            row_data = []
            for col_name in target_gsheet_columns:
                row_data.append(row_df[col_name])
            updates.append(row_data)

        # Determina o range completo para a atualização
        # Agora usando a nova função _col_to_char
        start_col_letter = _col_to_char(col_indices[target_gsheet_columns[0]])
        end_col_letter = _col_to_char(col_indices[target_gsheet_columns[-1]])
        end_row = start_row + len(df) - 1

        range_to_update = f"{start_col_letter}{start_row}:{end_col_letter}{end_row}"
        
        print(f"\nAtualizando Google Sheet no range: {range_to_update}")
        gsheet_worksheet.update(range_to_update, updates)


        print(f"\nPlanilha do Google Sheets '{worksheet_name}' atualizada com sucesso!")
        print("\nVisão geral dos dados processados:")
        print(df[['Nome'] + target_gsheet_columns])

    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a execução do script: {e}")

# --- Executa o Scraper ---
if __name__ == "__main__":
    # A URL da planilha deve ser passada como uma variável de ambiente no GitHub Actions.
    # Usaremos GOOGLE_SHEET_URL para o Price Tracker, que será configurado no GitHub Actions.
    GOOGLE_SHEET_URL = os.getenv('GOOGLE_SHEET_URL')

    if not GOOGLE_SHEET_URL:
        print("Erro: A variável de ambiente 'GOOGLE_SHEET_URL' não está definida.")
        print("Por favor, defina GOOGLE_SHEET_URL nas secrets do GitHub Actions.")
        exit(1)

    run_scraper(google_sheet_url=GOOGLE_SHEET_URL, worksheet_name='Desejos')
