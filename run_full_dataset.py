#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para processar o dataset completo da COP29
"""

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from main import MasterOrchestrator


def main():
    """Processa o dataset completo da COP29"""
    
    print("🌍 PROCESSAMENTO COMPLETO - DATASET COP29")
    print("=" * 60)
    print()
    print("⚠️  ATENÇÃO: Este processo pode demorar várias horas!")
    print("💾 Progresso será salvo automaticamente via cache")
    print("🔄 Se interrompido, pode ser retomado executando novamente")
    print()
    
    # Confirmar execução
    response = input("Deseja continuar? (s/N): ").lower().strip()
    if response not in ['s', 'sim', 'y', 'yes']:
        print("❌ Execução cancelada pelo usuário")
        return
    
    print("\n🚀 Iniciando processamento do dataset completo...")
    print("-" * 60)
    
    try:
        orchestrator = MasterOrchestrator()
        
        # Processar dataset completo (sem limite)
        results = orchestrator.run_complete_process()
        
        print("\n" + "=" * 60)
        print("🎉 PROCESSAMENTO COMPLETO CONCLUÍDO!")
        print("=" * 60)
        
        print(f"\n📁 Arquivos finais gerados:")
        for key, path in results.items():
            file_path = Path(path)
            if file_path.exists():
                size_mb = file_path.stat().st_size / (1024 * 1024)
                print(f"   ✅ {key}: {path} ({size_mb:.2f} MB)")
        
        print(f"\n💡 Próximos passos:")
        print(f"   1. Verificar arquivos em data/results/")
        print(f"   2. Executar interface Streamlit")
        print(f"   3. Analisar resultados finais")
        
    except KeyboardInterrupt:
        print(f"\n⚠️ PROCESSO INTERROMPIDO PELO USUÁRIO")
        print(f"💾 Progresso foi salvo no cache")
        print(f"🔄 Execute novamente para continuar de onde parou")
        
    except Exception as e:
        print(f"\n💥 ERRO: {str(e)}")
        print(f"💾 Progresso foi salvo no cache")
        print(f"🔄 Execute novamente para tentar continuar")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())