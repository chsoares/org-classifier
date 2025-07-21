#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import sys
import traceback
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import yaml

# Definir novos níveis de log
ANALYSIS = 25  # Entre INFO (20) e WARNING (30)
SUCCESS = 26  # Entre INFO (20) e WARNING (30)

logging.addLevelName(ANALYSIS, "ANALYSIS")
logging.addLevelName(SUCCESS, "SUCCESS!")


def analysis(self, message, *args, **kws):
    if self.isEnabledFor(ANALYSIS):
        self._log(ANALYSIS, message, args, **kws)


def success(self, message, *args, **kws):
    if self.isEnabledFor(SUCCESS):
        self._log(SUCCESS, message, args, **kws)


logging.Logger.analysis = analysis
logging.Logger.success = success


def find_project_root():
    """
    Encontra o diretório raiz do projeto procurando pelo arquivo config.yaml
    """
    current_dir = Path.cwd()

    # Procurar config.yaml subindo os diretórios
    while current_dir != current_dir.parent:
        if (current_dir / "config.yaml").exists():
            return current_dir
        current_dir = current_dir.parent

    raise FileNotFoundError("Arquivo config.yaml não encontrado na estrutura de diretórios")


def load_config(project_root):
    """
    Carrega o arquivo de configuração YAML
    """
    config_path = project_root / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_logger_config(analysis_type):
    """
    Retorna o nome do logger e arquivo de log baseado no tipo de análise

    Args:
        analysis_type: Tipo de análise ('org_classifier', 'web_scraper', etc)

    Returns:
        tuple: (logger_name, log_filename)
    """
    logger_name = f"org_classifier.{analysis_type}"
    log_filename = f"{analysis_type}.log"
    return logger_name, log_filename


def setup_logger(analysis_type="org_classifier", log_to_file=False):
    """
    Configura o sistema de logging com rotação semanal e formatação personalizada

    Args:
        analysis_type: Tipo de análise ('org_classifier', 'web_scraper', etc)
        log_to_file: Se True, cria arquivo de log. Se False, loga apenas no console.
    """
    # Obter configuração do logger
    logger_name, log_filename = get_logger_config(analysis_type)

    # Formatação do log
    LOG_EMOJIS = {
        "DEBUG": "🐞",
        "INFO": "✅",
        "ANALYSIS": "🔍",
        "SUCCESS!": "✨",
        "WARNING": "⚠️",
        "ERROR": "❌",
        "CRITICAL": "🚨",
    }
    # Códigos ANSI para cores no terminal
    ANSI_COLORS = {
        "DEBUG": "\033[94m",  # Azul
        "INFO": "\033[92m",  # Verde
        "ANALYSIS": "\033[96m",  # Ciano
        "SUCCESS!": "\033[96;1m",  # Ciano Brilhante
        "WARNING": "\033[93m",  # Amarelo
        "ERROR": "\033[91m",  # Vermelho
        "CRITICAL": "\033[95m",  # Magenta
    }
    ANSI_RESET = "\033[0m"  # Reseta a cor

    class CustomFormatter(logging.Formatter):
        def __init__(self, use_colors=True):
            super().__init__()
            self.use_colors = use_colors

        def format(self, record):
            emoji = LOG_EMOJIS.get(record.levelname, "❓")
            timestamp = self.formatTime(record, "%H:%M:%S")

            if self.use_colors:
                # Formato com cores ANSI para console
                levelname_colored = f"{ANSI_COLORS.get(record.levelname, '')}{record.levelname}{ANSI_RESET}"
                return (
                    f"[{timestamp}] {emoji} [{levelname_colored}] {record.getMessage()}"
                )
            else:
                # Formato sem cores para arquivo
                return (
                    f"[{timestamp}] {emoji} [{record.levelname}] {record.getMessage()}"
                )

    # Carregar configuração do projeto
    try:
        project_root = find_project_root()
        config = load_config(project_root)
    except FileNotFoundError:
        # Se não encontrar config, usar valores padrão
        project_root = Path.cwd()
        config = {"logging": {"logs": "logs"}}

    # Criar diretório de logs usando o caminho do config
    log_dir = project_root / config.get("logging", {}).get("log_directory", "logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    # Obter ou criar logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)  # Nível base para permitir todos os logs
    logger.propagate = True  # Garantir que os logs sejam propagados

    # Remover handlers existentes para evitar duplicação
    logger.handlers.clear()

    # Handler para arquivo (apenas se log_to_file=True)
    if log_to_file:
        log_file = log_dir / log_filename
        file_handler = TimedRotatingFileHandler(
            filename=log_file,
            when="W6",  # Rotação semanal
            interval=4,
            backupCount=4,
            encoding="utf-8",
        )
        file_handler.setFormatter(CustomFormatter(use_colors=False))
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)

    # Handler para console (sempre presente)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(CustomFormatter(use_colors=True))
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    # Silenciar logs desnecessários de outros módulos
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    return logger, console_handler


def log_exception(logger, exit_after=True):
    """
    Função auxiliar para logar exceções de forma apropriada

    Args:
        logger: Logger configurado para registrar a exceção
        exit_after: Se True, encerra o programa após logar a exceção
    """
    exc_type, exc_value, exc_traceback = sys.exc_info()

    # Cria uma mensagem personalizada para cada handler baseado em seu nível
    for handler in logger.handlers:
        if handler.level <= logging.DEBUG:
            # Para handlers em DEBUG (arquivo), usa traceback completo
            handler.handle(
                logger.makeRecord(
                    logger.name,
                    logging.ERROR,
                    "(unknown file)",
                    0,
                    "\n"
                    + "".join(
                        traceback.format_exception(exc_type, exc_value, exc_traceback)
                    ),
                    None,
                    None,
                )
            )
        else:
            # Para handlers em outros níveis (console), usa apenas a mensagem de erro
            handler.handle(
                logger.makeRecord(
                    logger.name,
                    logging.ERROR,
                    "(unknown file)",
                    0,
                    str(exc_value),
                    None,
                    None,
                )
            )

    if exit_after:
        sys.exit(1)