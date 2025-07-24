# Organization Classifier


## 🎯 Funcionalidades

- **Processamento de dados Excel**: Carrega e unifica dados de múltiplas abas da COP29
- **Normalização de organizações**: Agrupa nomes similares usando fuzzy matching
- **Web scraping inteligente**: Busca e extrai conteúdo de sites organizacionais
- **Classificação por IA**: Identifica organizações do setor de seguros usando OpenRouter
- **Interface web**: Dashboard Streamlit para visualização e correção manual
- **Sistema de cache**: Evita reprocessamento desnecessário
- **Tracking detalhado**: Acompanha cada etapa do processo para debugging

## 🚀 Como Usar

### 1. Configuração do Ambiente

```bash
# Criar ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate  # Windows

# Instalar dependências
pip install -r requirements.txt
```

### 2. Configuração

```bash
# Criar arquivo .env com sua API key do OpenRouter
echo "OPENROUTER_API_KEY=sua_chave_aqui" > .env
```

### 3. Preparar Dados

Coloque o arquivo `COP29_FLOP_On-site.xlsx` na pasta `data/raw/`

### 4. Executar

```bash
# Verificar configuração
python main.py

# Teste com dataset pequeno
python main.py --test

# Processamento completo (pode demorar horas)
python run_full_dataset.py

# Interface web
python run_streamlit.py
```

## 📊 Resultados

- `data/results/organizations.csv` - Lista de organizações com classificações
- `data/results/people.csv` - Participantes com informações de seguradoras
- `data/cache/` - Cache de resultados para evitar reprocessamento

## 🏗️ Arquitetura

```
src/
├── core/           # Processamento de dados e cache
├── scraping/       # Web scraping e busca
├── classification/ # Classificação por IA
├── pipeline/       # Pipeline principal
├── utils/          # Utilitários e configuração
└── ui/            # Interface Streamlit
```

## 📈 Pipeline de Processamento

1. **Carregamento**: Lê arquivo Excel e extrai dados relevantes
2. **Normalização**: Agrupa organizações com nomes similares
3. **Web Search**: Busca sites oficiais das organizações
4. **Scraping**: Extrai conteúdo relevante dos sites
5. **Classificação**: Usa IA para identificar seguradoras
6. **Merge**: Combina resultados com dataset original

## 🛠️ Tecnologias

- **Python 3.13+**
- **pandas**: Processamento de dados
- **requests + BeautifulSoup**: Web scraping
- **rapidfuzz**: Fuzzy string matching
- **OpenRouter API**: Classificação por IA
- **Streamlit**: Interface web
- **plotly**: Visualizações

## 📋 Status do Projeto - v1.0 ✅

- [x] **Configuração inicial e logging**
- [x] **Carregamento e processamento de dados Excel**
- [x] **Normalização de nomes de organizações**
- [x] **Sistema de tracking de organizações**
- [x] **Web scraping de sites organizacionais**
- [x] **Classificação por IA**
- [x] **Interface Streamlit**
- [x] **Sistema de cache**
- [x] **Processamento em lote**
- [x] **Merge de resultados**
- [x] **Validação e testes**

## 🎉 Versão 1.0 Completa

O sistema está totalmente funcional e foi testado com sucesso no dataset completo da COP29. Todas as funcionalidades principais foram implementadas e validadas.
