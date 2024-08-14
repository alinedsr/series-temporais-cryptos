import yfinance as yf
import pandas as pd
from datetime import datetime

# Função para obter a maior data do CSV existente
def get_max_date_from_csv(file_path):
    try:
        df = pd.read_csv(file_path, index_col=0, parse_dates=['Date'])
        if not df.empty:
            return df.index.max().strftime('%Y-%m-%d')
        else:
            return None
    except FileNotFoundError:
        return None
    except Exception as e:
        print(f'Erro ao ler o arquivo CSV: {e}')
        return None

# Definir o intervalo de datas
csv_file = 'cryptocurrencies_data.csv'
start_date_existing = get_max_date_from_csv(csv_file)
end_date = datetime.now().strftime('%Y-%m-%d')

# Definir o intervalo de datas para o novo download
start_date = start_date_existing if start_date_existing else '2014-01-01'

# Lista de criptomoedas e seus símbolos
cryptos = {
    'BTC-USD': 'Bitcoin',
    'ETH-USD': 'Ethereum',
    'LTC-USD': 'Litecoin',
    'LINK-USD': 'Chainlink',
    'UNI7083-USD': 'Uniswap',  # Verifique o símbolo correto para UNI7083
    'MELI': 'Meli Dólar'    # Verifique se é realmente criptomoeda ou ação
}

# Dicionário para armazenar os DataFrames carregados
dataframes = {}

for symbol, name in cryptos.items():
    try:
        # Baixar os dados usando yfinance
        df = yf.download(symbol, start=start_date, end=end_date)
        
        if not df.empty:
            # Adicionar uma coluna com o nome da criptomoeda
            df['Cryptocurrency'] = name
            df['Symbol'] = symbol  # Adiciona a coluna com o símbolo da criptomoeda
            dataframes[symbol] = df
        else:
            print(f'Nenhum dado encontrado para {name} no intervalo de {start_date} a {end_date}.')
    
    except Exception as e:
        print(f'Erro ao processar {symbol}: {e}')

# Verificar se algum DataFrame foi carregado antes de concatenar
if dataframes:
    # Concatenar todos os DataFrames sem adicionar níveis adicionais ao índice
    concatenated_df = pd.concat(dataframes.values())
    
    # Reordenar as colunas para que 'Cryptocurrency' seja a primeira coluna
    concatenated_df = concatenated_df[['Cryptocurrency'] + [col for col in concatenated_df.columns if col != 'Cryptocurrency']]
    
    # Adicionar dados antigos ao DataFrame concatenado se o CSV já existir
    if start_date_existing:
        old_df = pd.read_csv(csv_file, index_col=0, parse_dates=['Date'])
        combined_df = pd.concat([old_df, concatenated_df])
    else:
        combined_df = concatenated_df
    
    # Remover duplicatas e garantir que a coluna 'Date' esteja correta
    combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
    
    # Salvar o DataFrame concatenado em um arquivo CSV
    combined_df.to_csv(csv_file, index=True)
    
    print(f'Dados concatenados e salvos em {csv_file}')
else:
    print('Nenhum dado foi carregado.')