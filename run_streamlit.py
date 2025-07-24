#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para executar o Streamlit App
"""

import subprocess
import sys
from pathlib import Path

def main():
    """Executa o Streamlit App"""
    
    print("🚀 Iniciando COP29 Insurance Classification Dashboard")
    print("=" * 60)
    
    # Verificar se arquivos de dados existem
    orgs_path = Path("data/results/organizations.csv")
    people_path = Path("data/results/people.csv")
    
    if not orgs_path.exists() or not people_path.exists():
        print("⚠️  ATENÇÃO: Arquivos de dados não encontrados!")
        print("💡 Execute primeiro: python run_full_dataset.py")
        print()
        
        response = input("Deseja continuar mesmo assim? (s/N): ").lower().strip()
        if response not in ['s', 'sim', 'y', 'yes']:
            print("❌ Execução cancelada")
            return
    
    # Executar Streamlit
    try:
        print("🌐 Abrindo dashboard no navegador...")
        print("📍 URL: http://localhost:8501")
        print("⚠️  Para parar: Ctrl+C")
        print()
        
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            "src/ui/streamlit_app.py",
            "--server.port=8501",
            "--server.address=localhost"
        ])
        
    except KeyboardInterrupt:
        print("\n👋 Dashboard encerrado pelo usuário")
    except Exception as e:
        print(f"\n❌ Erro ao executar Streamlit: {str(e)}")
        print("💡 Certifique-se de que o Streamlit está instalado: pip install streamlit")

if __name__ == "__main__":
    main()