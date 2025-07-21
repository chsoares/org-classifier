# Organization Classifier

### 🎯 Funcionalidades

- **Processamento de dados Excel**: Carrega e unifica dados de múltiplas abas
- **Normalização de organizações**: Agrupa nomes similares usando fuzzy matching
- **Web scraping inteligente**: Busca e extrai conteúdo de sites organizacionais
- **Classificação por IA**: Identifica organizações do setor de seguros
- **Interface web**: Dashboard Streamlit para visualização dos resultados
- **Tracking detalhado**: Acompanha cada etapa do processo para debugging

## 🚀 Como Usar

### 1. Configuração do Ambiente

```bash
# Clonar o repositório
git clone https://github.com/seu-usuario/org-insurance-classifier.git
cd org-insurance-classifier

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
# Copiar arquivo de exemplo
cp .env.example .env

# Editar .env com sua API key do OpenRouter
OPENROUTER_API_KEY=sua_chave_aqui
```

### 3. Preparar Dados

Coloque o arquivo `COP29_FLOP_On-site.xlsx` na pasta `data/raw/`

### 4. Executar

```bash
# Teste inicial
python main.py

# Processamento completo (quando implementado)
python -m src.pipeline.org_processor
```

## 🏗️ Arquitetura

```
src/
├── core/           # Processamento de dados
├── scraping/       # Web scraping
├── classification/ # Classificação por IA
├── pipeline/       # Pipeline principal
├── utils/          # Utilitários
└── ui/            # Interface Streamlit
```

## 📈 Status do Desenvolvimento

- [x] **Etapa 1**: Configuração inicial e logging
- [x] **Etapa 2**: Carregamento e processamento de dados Excel
- [ ] **Etapa 3**: Normalização de nomes de organizações
- [ ] **Etapa 4**: Sistema de tracking de organizações
- [ ] **Etapa 5**: Web scraping de sites organizacionais
- [ ] **Etapa 6**: Classificação por IA
- [ ] **Etapa 7**: Interface Streamlit
- [ ] **Etapa 8**: Testes e validação

## 🛠️ Tecnologias

- **Python 3.13+**
- **pandas**: Processamento de dados
- **requests + BeautifulSoup**: Web scraping
- **rapidfuzz**: Fuzzy string matching
- **OpenRouter API**: Classificação por IA
- **Streamlit**: Interface web
- **plotly**: Visualizações
