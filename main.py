#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Organization Insurance Classifier - Main Entry Point

Este script é o ponto de entrada principal para o classificador de organizações
relacionadas ao setor de seguros.
"""

import sys
from pathlib import Path

# Adicionar src ao path para imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.logger_config import setup_logger
from utils.config_manager import config_manager


def main():
    """Função principal do programa"""
    
    # Configurar logging
    logger, console_handler = setup_logger("main", log_to_file=True)
    
    logger.info("🚀 Iniciando Organization Insurance Classifier")
    
    # Testar configuração
    try:
        # Testar carregamento de configuração
        data_config = config_manager.get_data_config()
        logger.info(f"📁 Arquivo Excel configurado: {data_config['excel_file']}")
        
        scraping_config = config_manager.get_scraping_config()
        logger.info(f"🌐 Timeout de scraping: {scraping_config['timeout']}s")
        
        cache_config = config_manager.get_cache_config()
        logger.info(f"💾 Cache habilitado: {cache_config['enabled']}")
        
        # Verificar se arquivo Excel existe
        excel_file = Path(data_config['excel_file'])
        if excel_file.exists():
            logger.success(f"✅ Arquivo Excel encontrado: {excel_file}")
        else:
            logger.warning(f"⚠️ Arquivo Excel não encontrado: {excel_file}")
            logger.info("📝 Coloque o arquivo COP29_FLOP_On-site.xlsx na raiz do projeto")
        
        # Verificar API key
        openrouter_config = config_manager.get_openrouter_config()
        if openrouter_config['api_key']:
            logger.success("🔑 API key do OpenRouter configurada")
        else:
            logger.warning("⚠️ API key do OpenRouter não configurada")
            logger.info("📝 Crie um arquivo .env com OPENROUTER_API_KEY=sua_chave_aqui")
        
        logger.success("✨ Configuração inicial concluída com sucesso!")
        logger.info("🔧 Sistema pronto para implementação das próximas etapas")
        
    except Exception as e:
        logger.error(f"❌ Erro na configuração inicial: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)