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
import math # Importa math para ceil

# Removidos imports do Selenium para deixar a aplicação mais leve

# Você pode instalar python-levenshtein para melhor desempenho: pip install fuzzywuzzy python-levenshtein

# --- Configuração Global ---
# Limiar de semelhança: Apenas resultados com uma pontuação acima deste valor
# serão considerados como uma correspondência válida.
SIMILARITY_THRESHOLD = 70 # Alterado para 70% conforme sua solicitação

# --- Funções Utilitárias para Tratamento de Preços ---

def clean_price_to_float(price_str: str) -> float:
    """
    Converte uma string de preço (ex: "R$ 199,90", "Gratuito", "Não encontrado") para um float.
    Retorna float('inf') para preços indisponíveis ou inválido, e 0.0 para "Gratuito".
    Os preços numéricos são arredondados para o inteiro mais próximo (para cima).
    """
    if not isinstance(price_str, str):
        return float('inf') # Trata tipos não-string (ex: NaN do Excel) como preço alto

    price_str_lower = price_str.lower().strip()
    if "gratuito" in price_str_lower or "free" in price_str_lower or "grátis" in price_str_lower:
        return 0.0
    if "não encontrado" in price_str_lower or "preço indisponível" in price_str_lower:
        return float('inf') # Representa um preço desconhecido/indisponível para comparação

    # Remove "R$", substitui vírgula por ponto, e remove outros caracteres não numéricos/ponto
    cleaned_price = price_str.replace("r$", "").replace(".", "").replace(",", ".").strip()
    try:
        # Tenta extrair apenas a parte numérica e arredonda para o inteiro mais próximo (para cima)
        match = re.search(r'\d[\d\.]*', cleaned_price)
        if match:
            return math.ceil(float(match.group(0))) # math.ceil para arredondar para cima
        return float('inf')
    except ValueError:
        return float('inf') # Retorna infinito se a conversão falhar

def format_float_to_price_str(price_float: float) -> str:
    """
    Converte um float de preço (já arredondado para cima) de volta para uma string formatada (ex: "400").
    Retorna "Não encontrado" se o preço for float('inf').
    Retorna "0" para jogos gratuitos.
    """
    if price_float == 0.0:
        return "0" # Mostra "0" para jogos gratuitos
    if price_float == float('inf'):
        return "Não encontrado" # Consistente com a mensagem de erro
    # Formata como um número inteiro, sem o "R$" e sem aspas.
    return str(int(price_float)) # Apenas o número inteiro como string, sem aspa inicial

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
    Scraper para buscar informações de jogos e preços na Steam usando requests e BeautifulSoup.
    """
    BASE_URL = "https://store.steampowered.com/search/"

    def search_game_price(self, game_name: str) -> dict:
        """
        Busca o preço de um jogo específico na Steam, usando correspondência fuzzy
        e considerando os primeiros 5 resultados.
        """
        print(f"STEAM: Buscando por '{game_name}'...")
        # Removido o parâmetro 'l' (idioma) para uma busca mais abrangente
        params = {'term': game_name, 'cc': 'br'} # Mantém 'cc': 'br' para preço em BRL
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        # Cookies para contornar a verificação de idade (ainda mantidos, mas podem não ser 100% eficazes sem JS)
        cookies = {
            'birthtime': '86400',  # 1 de Janeiro de 1970 em Unix timestamp
            'wants_mature_content': '1',
            'mature_content': '1'
        }

        try:
            response = requests.get(self.BASE_URL, params=params, headers=headers, cookies=cookies, timeout=15)
            response.raise_for_status() # Lança um erro para status de resposta HTTP ruins
        except requests.RequestException as e:
            print(f"ERRO STEAM: Falha de comunicação para '{game_name}': {e}")
            return self._format_error("Não encontrado.") # Mensagem de erro padrão

        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Pega os primeiros 5 resultados de busca
        search_results = soup.select("#search_resultsRows a")[:5] # Limita a 5 resultados

        best_match_element = None
        highest_score = 0
        
        if not search_results:
            return self._format_error("Não encontrado.") # Mensagem de erro padrão

        cleaned_game_name = _clean_game_title(game_name)

        # Itera sobre os resultados para encontrar a melhor correspondência
        for result_element in search_results:
            title_element = result_element.select_one("span.title")
            if title_element:
                result_title = title_element.text.strip()
                cleaned_result_title = _clean_game_title(result_title)
                # Calcula a pontuação de semelhança entre o nome buscado e o título do resultado limpos
                score = fuzz.ratio(cleaned_game_name, cleaned_result_title)
                
                if score > highest_score:
                    highest_score = score
                    best_match_element = result_element
        
        # Se não encontrou uma boa correspondência acima do limiar
        if not best_match_element or highest_score < SIMILARITY_THRESHOLD:
            return self._format_error(f"Não encontrado (semelhança: {highest_score}%).") # Mensagem de erro padrão

        # Processa as informações do melhor resultado
        title = best_match_element.select_one("span.title").text.strip()
        game_url = best_match_element['href']

        final_price_str = "Não encontrado" # Mensagem de erro padrão
        # Tenta encontrar o preço com desconto primeiro, depois o preço normal
        discount_price_element = best_match_element.select_one(".search_price.discounted, .discount_final_price")
        if discount_price_element:
            price_text = discount_price_element.text.strip() # Pega o texto completo
            if "gratuito" in price_text.lower() or "free" in price_text.lower() or "grátis" in price_text.lower():
                final_price_str = "Gratuito" # Usa "Gratuito" para ser pego por clean_price_to_float
            else:
                price_text_value = price_text.split("R$")[-1].strip()
                final_price_str = f"R$ {price_text_value}" if price_text_value else "Não encontrado"
        else:
            regular_price_element = best_match_element.select_one(".search_price")
            if regular_price_element:
                price_text = regular_price_element.text.strip() # Pega o texto completo
                if "gratuito" in price_text.lower() or "free" in price_text.lower() or "grátis" in price_text.lower():
                    final_price_str = "Gratuito" # Usa "Gratuito" para ser pego por clean_price_to_float
                else:
                    price_text_value = price_text.split("R$")[-1].strip()
                    final_price_str = f"R$ {price_text_value}" if price_text_value else "Não encontrado"
            else:
                final_price_str = "Não encontrado" # Se nenhum elemento de preço for encontrado
            
        return {
            "found": True,
            "title": title,
            "price_str": final_price_str,
            "price_float": clean_price_to_float(final_price_str),
            "url": game_url,
            "similarity_score": highest_score
        }

    def _format_error(self, message: str) -> dict:
        """
        Formata um dicionário de erro para resultados da Steam.
        A mensagem padrão será "Não encontrado".
        """
        return {
            "found": False,
            "title": None,
            "price_str": "Não encontrado", # Sempre retorna "Não encontrado"
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
            return self._format_error("Não encontrado.") # Mensagem de erro padrão

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
            return self._format_error(f"Não encontrado (semelhança: {highest_score}%).") # Mensagem de erro padrão


        title = 'Nome não encontrado'
        price_str = 'Não encontrado' # Mensagem de erro padrão
        
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
                price_str_raw = temp_price_element.text.strip()
                if "gratuito" in price_str_raw.lower() or "free" in price_str_raw.lower() or "grátis" in price_str_raw.lower():
                    price_str = "Gratuito" # Usa "Gratuito" para ser pego por clean_price_to_float
                else:
                    price_str = price_str_raw
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
                price_str_raw = price_element.text.strip()
                if "gratuito" in price_str_raw.lower() or "free" in price_str_raw.lower() or "grátis" in price_str_raw.lower():
                    price_str = "Gratuito" # Usa "Gratuito" para ser pego por clean_price_to_float
                else:
                    price_str = price_str_raw
        
        return {
            "found": True,
            "title": title,
            "price_str": price_str,
            "price_float": clean_price_to_float(price_str), # Corrigido para usar price_str
            "url": game_url,
            "similarity_score": highest_score
        }

    def _format_error(self, message: str) -> dict:
        """
        Formata um dicionário de erro para resultados da PSN.
        A mensagem padrão será "Não encontrado".
        """
        return {
            "found": False,
            "title": None,
            "price_str": "Não encontrado", # Sempre retorna "Não encontrado"
            "price_float": float('inf'),
            "url": None,
            "similarity_score": 0 # Semelhança 0 em caso de erro/não encontrado
        }


# --- Lógica Principal do Script ---

# Cache global para planilhas e dados
_sheet_cache = {}
_data_cache = {}
_cache_ttl_seconds = 300 # Tempo de vida do cache em segundos (5 minutos)
_last_cache_update = {}

# Configuração da URL da planilha (usando a mesma variável de ambiente do seu API)
class PriceTrackerConfig:
    GOOGLE_SHEETS_CREDENTIALS_JSON = os.environ.get('GSPREAD_SERVICE_ACCOUNT_CREDENTIALS')
    if not GOOGLE_SHEETS_CREDENTIALS_JSON:
        print("CRITICAL ERROR: 'GSPREAD_SERVICE_ACCOUNT_CREDENTIALS' environment variable is not set!")

    GOOGLE_SHEET_URL = os.environ.get('GOOGLE_SHEET_URL')
    if not GOOGLE_SHEET_URL:
        print("CRITICAL ERROR: 'GOOGLE_SHEET_URL' environment variable is not set!")


# --- Função auxiliar para converter número de coluna para letra ---
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


def _get_sheet_for_price_tracker(sheet_name):
    """
    Retorna o objeto da planilha (worksheet) para o Price Tracker, usando cache.
    Autentica com as credenciais da conta de serviço lidas de uma variável de ambiente,
    e abre a planilha pela URL, conforme o sistema da sua API.
    """
    global _sheet_cache
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

        print(f"DEBUG (PriceTracker): Google Sheet URL being used: {google_sheet_url}")

        creds_dict = json.loads(credentials_json)
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        
        gc = gspread.authorize(creds)
        
        print("DEBUG (PriceTracker): Type of 'gc' object after authorize: ", type(gc))
        print(f"DEBUG (PriceTracker): gspread version: {gspread.__version__}")

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
    global _data_cache, _last_cache_update
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
    global _data_cache
    if sheet_name in _data_cache:
        del _data_cache[sheet_name]
        print(f"Cache para a planilha '{sheet_name}' invalidado.")


def run_scraper(google_sheet_url: str, worksheet_name: str = 'Desejos'):
    """
    Função principal que orquestra a leitura da planilha do Google Sheets, o scraping e a atualização.
    """
    steam_scraper = SteamScraper()
    psn_scraper = PsnScraper()
    current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S') # Data atual para registro

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
                df[col] = 'Não encontrado' # Valor padrão para novas colunas

        # Pega os cabeçalhos da planilha para encontrar os índices das colunas target
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
            # Atribui o preço formatado ao DataFrame
            df.at[index, 'Steam Preco Atual'] = format_float_to_price_str(steam_result['price_float'])
            
            current_steam_price_float = steam_result['price_float']
            historical_steam_price_str = df.at[index, 'Steam Menor Preco Historico']
            historical_steam_price_float = clean_price_to_float(historical_steam_price_str)

            if current_steam_price_float < historical_steam_price_float:
                # Atribui o menor preço histórico formatado
                df.at[index, 'Steam Menor Preco Historico'] = format_float_to_price_str(steam_result['price_float'])
                print(f"  STEAM: Novo menor preço histórico para '{game_name}': {format_float_to_price_str(steam_result['price_float'])} (Semelhança: {steam_result['similarity_score']}%)")
            elif historical_steam_price_float == float('inf') and steam_result['found']:
                 # Atribui o primeiro preço formatado
                 df.at[index, 'Steam Menor Preco Historico'] = format_float_to_price_str(steam_result['price_float'])
                 print(f"  STEAM: Primeiro preço registrado para '{game_name}': {format_float_to_price_str(steam_result['price_float'])} (Semelhança: {steam_result['similarity_score']}%)")
            else:
                 print(f"  STEAM: Preço atual para '{game_name}': {format_float_to_price_str(steam_result['price_float'])} (Semelhança: {steam_result['similarity_score']}%)")


            # --- Busca na PSN ---
            psn_result = psn_scraper.search_game_price(game_name)
            # Atribui o preço formatado ao DataFrame
            df.at[index, 'PSN Preco Atual'] = format_float_to_price_str(psn_result['price_float'])

            current_psn_price_float = psn_result['price_float']
            historical_psn_price_str = df.at[index, 'PSN Menor Preco Historico']
            historical_psn_price_float = clean_price_to_float(historical_psn_price_str)

            if current_psn_price_float < historical_psn_price_float:
                # Atribui o menor preço histórico formatado
                df.at[index, 'PSN Menor Preco Historico'] = format_float_to_price_str(psn_result['price_float'])
                print(f"  PSN: Novo menor preço histórico para '{game_name}': {format_float_to_price_str(psn_result['price_float'])} (Semelhança: {psn_result['similarity_score']}%)")
            elif historical_psn_price_float == float('inf') and psn_result['found']:
                 # Atribui o primeiro preço formatado
                 df.at[index, 'PSN Menor Preco Historico'] = format_float_to_price_str(psn_result['price_float'])
                 print(f"  PSN: Primeiro preço registrado para '{game_name}': {format_float_to_price_str(psn_result['price_float'])} (Semelhança: {psn_result['similarity_score']}%)")
            else:
                 print(f"  PSN: Preço atual para '{game_name}': {format_float_to_price_str(psn_result['price_float'])} (Semelhança: {psn_result['similarity_score']}%)")
            
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
        # Correção para a DeprecationWarning: usar argumentos nomeados
        gsheet_worksheet.update(values=updates, range_name=range_to_update)


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
