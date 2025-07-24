#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Organization Insurance Classifier - Main Entry Point

Este script é o ponto de entrada principal para o classificador de organizações
relacionadas ao setor de seguros.
"""

import sys
import argparse
from pathlib import Path

# Adicionar src ao path para imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.logger_config import setup_logger
from utils.config_manager import config_manager


def check_setup():
    """Verifica se o sistema está configurado corretamente"""
    
    logger, _ = setup_logger("setup_check", log_to_file=False)
    
    print("🔍 VERIFICAÇÃO DE CONFIGURAÇÃO")
    print("=" * 50)
    
    issues = []
    
    try:
        # Verificar arquivo Excel
        data_config = config_manager.get_data_config()
        excel_file = Path(data_config['excel_file'])
        
        if excel_file.exists():
            size_mb = excel_file.stat().st_size / (1024 * 1024)
            print(f"✅ Arquivo Excel: {excel_file} ({size_mb:.1f} MB)")
        else:
            print(f"❌ Arquivo Excel não encontrado: {excel_file}")
            issues.append("Coloque o arquivo COP29_FLOP_On-site.xlsx na pasta data/raw/")
        
        # Verificar API key
        openrouter_config = config_manager.get_openrouter_config()
        if openrouter_config['api_key']:
            print("✅ API key do OpenRouter configurada")
        else:
            print("❌ API key do OpenRouter não configurada")
            issues.append("Crie um arquivo .env com OPENROUTER_API_KEY=sua_chave_aqui")
        
        # Verificar diretórios
        required_dirs = [
            Path("data/raw"),
            Path("data/processed"),
            Path("data/results"),
            Path("data/cache")
        ]
        
        for dir_path in required_dirs:
            if dir_path.exists():
                print(f"✅ Diretório: {dir_path}")
            else:
                print(f"⚠️  Diretório será criado: {dir_path}")
        
        print("\n" + "=" * 50)
        
        if issues:
            print("❌ PROBLEMAS ENCONTRADOS:")
            for i, issue in enumerate(issues, 1):
                print(f"   {i}. {issue}")
            print("\n💡 Resolva os problemas acima antes de continuar")
            return False
        else:
            print("✅ SISTEMA CONFIGURADO CORRETAMENTE!")
            print("\n🚀 Próximos passos:")
            print("   1. python run_full_dataset.py  # Processar dataset completo")
            print("   2. python run_streamlit.py     # Abrir interface web")
            return True
            
    except Exception as e:
        print(f"❌ Erro na verificação: {str(e)}")
        return False


def run_test():
    """Executa teste com dataset pequeno"""
    
    print("🧪 EXECUTANDO TESTE")
    print("=" * 50)
    
    try:
        from main import MasterOrchestrator
        
        orchestrator = MasterOrchestrator()
        
        # Processar apenas 5 organizações para teste
        print("🔄 Processando 5 organizações para teste...")
        results = orchestrator.run_complete_process(limit=5)
        
        print("\n✅ TESTE CONCLUÍDO!")
        print("📁 Arquivos gerados:")
        for key, path in results.items():
            if Path(path).exists():
                print(f"   - {key}: {path}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro no teste: {str(e)}")
        return False


def main():
    """Função principal do programa"""
    
    parser = argparse.ArgumentParser(
        description="Organization Insurance Classifier",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python main.py                    # Verificar configuração
  python main.py --check           # Verificar configuração
  python main.py --test            # Executar teste pequeno
  python run_full_dataset.py       # Processar dataset completo
  python run_streamlit.py          # Abrir interface web
        """
    )
    
    parser.add_argument(
        "--check", 
        action="store_true", 
        help="Verificar se o sistema está configurado corretamente"
    )
    
    parser.add_argument(
        "--test", 
        action="store_true", 
        help="Executar teste com dataset pequeno (5 organizações)"
    )
    
    args = parser.parse_args()
    
    # Se nenhum argumento, fazer verificação por padrão
    if not args.check and not args.test:
        args.check = True
    
    if args.check:
        success = check_setup()
        return 0 if success else 1
    
    if args.test:
        success = run_test()
        return 0 if success else 1
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)