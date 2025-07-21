#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import base64
import logging
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pdfkit
import streamlit as st
import yaml

logger = logging.getLogger(__name__)


def get_environment_prefix():
    """
    Retorna o prefixo do ambiente baseado no host da aplicação.
    
    Returns:
        str: "(LOCAL)", "(DEV)" ou string vazia "" para produção
    """
    try:
        # Obter o host do cabeçalho HTTP usando a API oficial do Streamlit
        host = st.context.headers.get("Host", "")
        
        if "localhost" in host:
            return "(LOCAL) "
        elif "dev.copic.app" in host:
            return "(DEV) "
        else:
            return ""
    except Exception:
        # Em caso de erro, retornar string vazia para garantir funcionamento em produção
        return ""


def find_project_root():
    """
    Encontra o diretório raiz do projeto procurando pelo arquivo config.yaml
    """
    current_dir = Path.cwd()

    # Procurar config.yaml subindo os diretórios
    while current_dir != current_dir.parent:
        if (current_dir / "Home.py").exists():
            return current_dir
        current_dir = current_dir.parent

    raise FileNotFoundError("Arquivo Home.py não encontrado na estrutura de diretórios")


def load_config(project_root):
    """
    Carrega o arquivo de configuração YAML
    """
    config_path = project_root / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def clean_directories(config, project_root):
    """
    Limpa os diretórios de output e export antes da execução
    """
    # Lista de diretórios para limpar
    dirs_to_clean = [
        project_root / config["paths"]["output"],
        project_root / config["paths"]["export"],
    ]

    for dir_path in dirs_to_clean:
        if dir_path.exists():
            # Remove todos os arquivos do diretório
            for file in dir_path.glob("*"):
                try:
                    if file.is_file():
                        file.unlink()  # Remove arquivo
                    elif file.is_dir():
                        shutil.rmtree(file)  # Remove diretório e seu conteúdo
                except Exception as e:
                    logger.warning(f"Aviso: Não foi possível remover {file}: {e}")
            logger.debug(f"Diretório limpo: {dir_path}")
        else:
            # Cria o diretório se não existir
            dir_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Diretório criado: {dir_path}")


def generate_pdf(markdown_path, config, project_root, logger=None):
    """
    Gera um arquivo PDF a partir do markdown usando wkhtmltopdf

    Args:
        markdown_path: Caminho do arquivo markdown
        config: Configurações do projeto
        project_root: Diretório raiz do projeto
        logger: Logger a ser usado (opcional)
    """
    try:
        # Usar o logger passado ou o logger do módulo
        log = logger or logging.getLogger(__name__)

        # Extrair o nome base do arquivo markdown sem a extensão
        base_name = Path(markdown_path).stem

        output_dir = project_root / config["paths"]["output"]
        pdf_path = output_dir / f"{base_name}.pdf"
        html_path = output_dir / f"{base_name}_temp.html"
        css_path = project_root / config["paths"]["styles"] / "pdf_style.css"

        # Primeiro converte markdown para HTML usando pandoc
        cmd_to_html = [
            "pandoc",
            str(markdown_path),
            "-o",
            str(html_path),
            "--standalone",
            "--css",
            str(css_path),
        ]

        process = subprocess.run(cmd_to_html, capture_output=True, text=True)

        if process.returncode != 0:
            log.error(f"Erro ao converter para HTML: {process.stderr}")
            return

        try:
            if os.name == "nt":  # Windows
                try:
                    # Tenta encontrar wkhtmltopdf no PATH do Windows
                    process = subprocess.run(
                        ["where", "wkhtmltopdf"], capture_output=True, text=True
                    )
                    wkhtmltopdf_path = process.stdout.strip().split("\n")[0]
                    if not wkhtmltopdf_path:
                        wkhtmltopdf_path = (
                        r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"
                    )
                except Exception:
                    # Fallback para o caminho padrão do Windows
                    wkhtmltopdf_path = (
                        r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"
                    )
            else:  # Linux/Mac
                try:
                    # Tenta encontrar wkhtmltopdf no PATH do Unix
                    process = subprocess.run(
                        ["which", "wkhtmltopdf"], capture_output=True, text=True
                    )
                    wkhtmltopdf_path = process.stdout.strip()
                except Exception:
                    wkhtmltopdf_path = "wkhtmltopdf"

            if not wkhtmltopdf_path:
                raise FileNotFoundError("wkhtmltopdf não encontrado no sistema")

            config_pdf = pdfkit.configuration(wkhtmltopdf=wkhtmltopdf_path)

            options = {
                "page-size": "A4",
                "margin-top": "20mm",
                "margin-right": "20mm",
                "margin-bottom": "20mm",
                "margin-left": "20mm",
                "encoding": "UTF-8",
                "enable-local-file-access": None,
                "load-error-handling": "ignore",
                "load-media-error-handling": "ignore",
            }

            pdfkit.from_file(
                str(html_path), str(pdf_path), options=options, configuration=config_pdf
            )

            log.info(f"PDF gerado com sucesso: {pdf_path}")

            # Remove os arquivos temporários
            html_path.unlink()

        except ImportError:
            log.error("pdfkit não encontrado no sistema.")
        except OSError as e:
            log.error(f"Erro ao executar wkhtmltopdf: {str(e)}")

    except Exception as e:
        if log:
            log.error(f"Erro ao gerar PDF: {str(e)}")
        else:
            print(f"Erro ao gerar PDF: {str(e)}")


def format_period(config):
    """
    Formata o período de análise baseado na configuração
    """
    period = config.get("period", {})
    period_type = period.get("type")
    year = period.get("year")

    if period_type == "annual":
        return str(year)
    elif period_type == "monthly":
        month = period.get("month")
        month_name = datetime(year, month, 1).strftime("%b").lower()
        return f"{month_name}. {year}"
    else:  # custom
        custom = period.get("custom", {})
        start = custom.get("start_date")
        end = custom.get("end_date")
        start_str = start.strftime("%b").lower()
        end_str = end.strftime("%b").lower()
        return (
            f"{start.day} {start_str}. {start.year} a {end.day} {end_str}. {end.year}"
        )


def save_config(config):
    """Salva configuração do projeto mantendo ordem e comentários."""
    try:
        project_root = find_project_root()
        config_path = project_root / "config.yaml"

        # Template da configuração com comentários e ordem definida
        config_template = """# Configuração do copic.app

# Configurações de período de análise
period:
  # Tipo de período (monthly, annual, custom)
  type: {period[type]}
  # Ano da análise
  year: {period[year]}
  # Mês da análise (quando type=monthly)
  month: {period[month]}
  # Período customizado (quando type=custom)
  custom:
    start_date: {period[custom][start_date]}
    end_date: {period[custom][end_date]}

# Configurações de país em destaque
country:
  # Código ISO2 do país (ex: BR, US, FR)
  iso2: {country[iso2]}

# Configurações de fornecedor em destaque
vendor:
  # Nome do fornecedor (ex: Microsoft, Apple, Google) - use all para todos os fornecedores
  name: {vendor[name]}

# Configurações de localização e internacionalização
locale:
  # Localização principal do sistema
  primary: {locale[primary]}
  # Localização de fallback
  fallback: {locale[fallback]}

# Caminhos do projeto
paths:
  data: {paths[data]}
  export: {paths[export]}
  logs: {paths[logs]}
  output: {paths[output]}
  pages: {paths[pages]}
  resources: {paths[resources]}
  scripts: {paths[scripts]}
  styles: {paths[styles]}
  templates: {paths[templates]}

"""
        # Formatar o template com os valores atuais
        config_str = config_template.format(**config)

        with open(config_path, "w", encoding="utf-8") as file:
            file.write(config_str)
        return True

    except Exception as e:
        st.error(f"Erro ao salvar configuração do projeto: {str(e)}")
        return False


def run_analysis(script_path):
    """
    Executa um script de análise.

    Returns:
        tuple: (success, message, details)
            - success (bool): True se executou com sucesso, False caso contrário
            - message (str): Mensagem amigável para o usuário
            - details (str): Detalhes técnicos do erro (se houver)
    """
    try:
        project_root = find_project_root()
        with st.spinner("Executando análise..."):
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                cwd=project_root,
            )

            if result.returncode != 0:
                return (
                    False,
                    "Erro ao executar a análise. Verifique o console para mais detalhes.",
                    result.stderr,
                )

            return (True, "Análise concluída com sucesso!", None)

    except subprocess.CalledProcessError as e:
        return (
            False,
            "Erro ao executar a análise. Verifique o console para mais detalhes.",
            f"Erro na execução do script: {e.stderr}",
        )
    except Exception as e:
        return (
            False,
            "Erro ao executar a análise. Verifique o console para mais detalhes.",
            f"Erro inesperado: {e}",
        )


def markdown_images(markdown):
    """Encontra todas as imagens no markdown."""
    images = re.findall(r"!\[\]\((.*?\.png)\)", markdown)
    return [(f"![]({img})", "", img) for img in images]


def img_to_bytes(img_path):
    """Converte imagem para bytes em base64."""
    img_bytes = Path(img_path).read_bytes()
    encoded = base64.b64encode(img_bytes).decode()
    return encoded


def img_to_html(img_path, img_alt):
    """Converte imagem para tag HTML com dados em base64."""
    img_format = img_path.split(".")[-1]
    img_html = f'<img src="data:image/{img_format.lower()};base64,{img_to_bytes(img_path)}" alt="{img_alt}">'
    return img_html


def markdown_insert_images(markdown, base_path=None):
    """Processa o markdown substituindo imagens por HTML base64."""
    images = markdown_images(markdown)

    for image in images:
        image_markdown = image[0]
        image_alt = image[1]
        image_path = image[2]

        # Se temos um caminho base e o caminho da imagem é relativo
        if base_path and image_path.startswith("../"):
            image_path = str(base_path / image_path.replace("../", ""))

        if os.path.exists(image_path):
            try:
                html = img_to_html(image_path, image_alt)
                markdown = markdown.replace(image_markdown, html)
            except Exception as e:
                logger.error(f"Erro ao processar imagem: {str(e)}")
                st.error("Erro ao processar imagem.")

    return markdown


def markdown_insert_tables_and_images(content, project_root):
    """Insere tabelas e imagens no markdown e exibe o conteúdo formatado."""
    try:
        # Dividir o conteúdo em linhas
        lines = content.split('\n')
        
        # Encontrar o início do sumário executivo
        exec_summary_start = -1
        exec_summary_end = -1
        for i, line in enumerate(lines):
            if line.strip().lower() == "### sumário executivo":
                exec_summary_start = i
            elif exec_summary_start != -1 and line.startswith('## ') and i > exec_summary_start:
                exec_summary_end = i
                break
        
        # Se encontrou o sumário executivo
        if exec_summary_start != -1:
            # Se não encontrou o fim, usar até o final do arquivo
            if exec_summary_end == -1:
                exec_summary_end = len(lines)
            
            # Extrair o sumário executivo
            exec_summary_lines = lines[exec_summary_start:exec_summary_end]
            exec_summary_content = '\n'.join(exec_summary_lines)
            
            # Exibir o sumário executivo em um st.info
            st.info(exec_summary_content)
            
            # Usar apenas o conteúdo após o sumário executivo
            content = '\n'.join(lines[exec_summary_end:])
        
        # Processar o resto do conteúdo normalmente
        # Dividir o conteúdo em blocos de markdown, código e tabelas
        blocks = []
        current_block = []
        in_code_block = False
        in_table = False
        
        for line in content.split('\n'):
            # Detecta comentário de tabela
            if line.strip().startswith('<!-- table:'):
                if current_block:
                    blocks.append(('markdown', '\n'.join(current_block)))
                    current_block = []
                # Extrai o nome do arquivo CSV
                csv_file = line.strip().replace('<!-- table:', '').replace('-->', '').strip()
                in_table = True
                current_block = [csv_file]  # Guarda o nome do arquivo
            # Detecta início de tabela markdown
            elif line.startswith('|') and in_table:
                current_block.append(line)
            # Detecta fim de tabela (linha em branco após tabela)
            elif in_table and not line.strip():
                blocks.append(('table', '\n'.join(current_block)))
                current_block = []
                in_table = False
            # Detecta blocos de código
            elif line.startswith('```'):
                if in_code_block:
                    # Fim do bloco de código
                    current_block.append(line)
                    blocks.append(('code', '\n'.join(current_block)))
                    current_block = []
                else:
                    # Início do bloco de código
                    if current_block:
                        blocks.append(('markdown', '\n'.join(current_block)))
                        current_block = []
                    current_block.append(line)
                in_code_block = not in_code_block
            else:
                current_block.append(line)
        
        # Adicionar o último bloco
        if current_block:
            if in_table:
                blocks.append(('table', '\n'.join(current_block)))
            elif in_code_block:
                blocks.append(('code', '\n'.join(current_block)))
            else:
                blocks.append(('markdown', '\n'.join(current_block)))
        
        # Processar cada bloco
        for block_type, block_content in blocks:
            if block_type == 'markdown':
                # Processar imagens no bloco markdown
                block_content = markdown_insert_images(block_content, project_root)
                st.markdown(block_content, unsafe_allow_html=True)
            elif block_type == 'code':
                # Bloco de código
                st.code(block_content, language='markdown')
            elif block_type == 'table':
                # Processar tabela
                lines = block_content.split('\n')
                csv_file = lines[0]  # Primeira linha é o nome do arquivo
                try:
                    # Carrega o CSV da pasta export
                    csv_path = project_root / "export" / csv_file
                    if csv_path.exists():
                        # Determinar os tipos de dados baseado no nome do arquivo
                        if csv_file == 'vuln_report.csv':
                            dtypes = {
                                'ID': 'string',
                                'Produto': 'string',
                                'CVSS': 'float',
                                'EPSS': 'float',
                                'Risco': 'category',
                                'Exploit': 'category',
                                'Ransomware': 'category'
                            }
                            parse_dates = ['Data']
                        else:
                            # Para outros arquivos, inferir os tipos
                            dtypes = None
                            parse_dates = None
                        
                        # Carregar o CSV com a configuração apropriada
                        df = pd.read_csv(
                            csv_path,
                            dtype=dtypes,
                            parse_dates=parse_dates
                        )
                        
                        # Criar uma cópia para display
                        df_display = df.copy()
                        
                        # Aplicar formatações específicas baseadas nas colunas existentes
                        if csv_file == 'vuln_report.csv':
                            if 'Exploit' in df_display.columns:
                                df_display['Exploit'] = df_display['Exploit'].astype(str).replace({'X': '💥', 'nan': ''})
                            if 'Ransomware' in df_display.columns:
                                df_display['Ransomware'] = df_display['Ransomware'].astype(str).replace({'X': '💀', 'nan': ''})
                        else:
                            # Para outras tabelas, formatar Exploit e Ransomware como inteiros quando são numéricos
                            if 'Exploit' in df_display.columns and pd.api.types.is_numeric_dtype(df_display['Exploit']):
                                df_display['Exploit'] = df_display['Exploit'].map('{:.0f}'.format)
                            if 'Ransomware' in df_display.columns and pd.api.types.is_numeric_dtype(df_display['Ransomware']):
                                df_display['Ransomware'] = df_display['Ransomware'].map('{:.0f}'.format)
                        
                        # Formatação de números para todas as tabelas
                        for col in df_display.columns:
                            # EPSS e variações
                            if 'EPSS' in col:
                                df_display[col] = df_display[col].map('{:.1%}'.format)
                            
                            # CVSS e variações
                            elif 'CVSS' in col:
                                df_display[col] = df_display[col].map('{:.1f}'.format)
                            
                            # Proporção como porcentagem
                            elif col == 'Proporção' or 'Proporção' in col:
                                df_display[col] = df_display[col].map('{:.1%}'.format)
                            
                            # Quantidade sem decimais
                            elif col == 'Quantidade' or col == 'CVEs':
                                df_display[col] = df_display[col].map('{:.0f}'.format)
                        
                        # Configurar a estilização baseada nas colunas disponíveis
                        def style_df(df):
                            styler = df.style
                            
                            # Aplicar estilos apenas se for a tabela principal
                            if csv_file == 'vuln_report.csv':
                                style_columns = []
                                if 'Exploit' in df.columns:
                                    style_columns.append('Exploit')
                                if 'Ransomware' in df.columns:
                                    style_columns.append('Ransomware')
                                    
                                if style_columns:
                                    styler = styler.map(
                                        lambda x: 'background-color: black; color: white' if x in ['💀', '💥'] else '',
                                        subset=style_columns
                                    )
                            
                            if 'Risco' in df.columns:
                                styler = styler.map(
                                    lambda x: {
                                        'Baixo': 'background-color: #008000; color: white',
                                        'Médio': 'background-color: #FF8C00; color: white',
                                        'Alto': 'background-color: #B22222; color: white',
                                        'Crítico': 'background-color: #4B0082; color: white'
                                    }.get(x, ''),
                                    subset=['Risco']
                                )
                            
                            return styler
                        
                        # Configurar as colunas baseado nas colunas disponíveis
                        column_config = {}
                        
                        if 'Data' in df_display.columns:
                            column_config["Data"] = st.column_config.DateColumn(
                                "Data",
                                format="DD/MM/YYYY",
                                help="Data de inclusão na KEV"
                            )
                        
                        if 'Produto' in df_display.columns:
                            column_config["Produto"] = st.column_config.TextColumn(
                                "Produto",
                                width=400
                            )
                        
                        # Configurações específicas para vuln_report.csv
                        if csv_file == 'vuln_report.csv':
                            if 'Risco' in df_display.columns:
                                column_config["Risco"] = st.column_config.TextColumn(
                                    "Risco",
                                    width=30
                                )
                            if 'Exploit' in df_display.columns:
                                column_config["Exploit"] = st.column_config.TextColumn(
                                    "Ex",
                                    width=7
                                )
                            if 'Ransomware' in df_display.columns:
                                column_config["Ransomware"] = st.column_config.TextColumn(
                                    "Rw",
                                    width=7
                                )
                        
                        # Renderiza o DataFrame estilizado
                        st.dataframe(
                            style_df(df_display),
                            use_container_width=True,
                            hide_index=True,
                            column_config=column_config
                        )
                        
                        # Adiciona legenda apenas se necessário e se for a tabela principal
                        if csv_file == 'vuln_report.csv' and ('Exploit' in df_display.columns or 'Ransomware' in df_display.columns):
                            st.caption("💥: Exploit público disponível / 💀: Vulnerabilidade explorada em campanha de ransomware")
                    else:
                        # Fallback: renderiza a tabela markdown original
                        st.markdown('\n'.join(lines[1:]))
                except Exception as e:
                    logger.error(f"Erro ao processar tabela CSV {csv_file}: {e}")
                    # Fallback: renderiza a tabela markdown original
                    st.markdown('\n'.join(lines[1:]))
                
    except Exception as e:
        st.error(f"Erro ao processar o conteúdo do relatório: {str(e)}")
        logger.error(f"Erro ao processar o conteúdo do relatório: {str(e)}")


def create_export_zip(project_root, config):
    """
    Cria um arquivo ZIP com todo o conteúdo da pasta export.
    
    Args:
        project_root: Diretório raiz do projeto
        config: Configurações do projeto
        
    Returns:
        bytes: Conteúdo do arquivo ZIP em bytes
    """
    import io
    import zipfile
    
    # Criar um buffer em memória para o ZIP
    zip_buffer = io.BytesIO()
    
    # Criar arquivo ZIP
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # Adicionar todos os arquivos da pasta export
        export_dir = project_root / config["paths"]["export"]
        for file_path in export_dir.glob('*'):
            # Adicionar arquivo ao ZIP mantendo apenas o nome do arquivo
            zip_file.write(file_path, arcname=file_path.name)
    
    # Retornar o buffer do ZIP
    zip_buffer.seek(0)
    return zip_buffer.getvalue()
