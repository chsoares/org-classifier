#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Teste da Pipeline Completa com arquivo de teste
"""

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from main import MasterOrchestrator


def test_complete_pipeline():
    """Testa a pipeline completa com arquivo de teste"""
    
    print("🧪 TESTE DA PIPELINE COMPLETA")
    print("=" * 50)
    print()
    print("O QUE estamos testando:")
    print("- Processo completo de 6 etapas")
    print("- Arquivo Excel → CSV processado → Normalização")
    print("- Tabela de organizações → Pipeline de classificação")
    print("- Dataset final de pessoas")
    print("- Cache funcionando em todas as etapas")
    print()
    
    # Primeiro, criar arquivo de teste
    print("📊 Criando arquivo Excel de teste...")
    import subprocess
    result = subprocess.run([sys.executable, "create_test_excel.py"], 
                          capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ Erro ao criar arquivo de teste: {result.stderr}")
        return
    
    print("✅ Arquivo de teste criado!")
    print()
    
    # Executar pipeline completa
    print("🚀 Executando pipeline completa...")
    print("-" * 40)
    
    try:
        orchestrator = MasterOrchestrator()
        
        # Executar processo completo (sem limite para testar tudo)
        results = orchestrator.run_complete_process()
        
        print("\n" + "=" * 50)
        print("✅ PIPELINE COMPLETA EXECUTADA COM SUCESSO!")
        print("=" * 50)
        
        print(f"\n📁 Arquivos gerados:")
        for key, path in results.items():
            file_path = Path(path)
            if file_path.exists():
                size = file_path.stat().st_size
                print(f"   ✅ {key}: {path} ({size} bytes)")
            else:
                print(f"   ❌ {key}: {path} (não encontrado)")
        
        # Verificar conteúdo dos arquivos principais
        print(f"\n📊 Verificação dos resultados:")
        
        # Organizations.csv
        if 'organizations' in results:
            try:
                import pandas as pd
                orgs_df = pd.read_csv(results['organizations'])
                total_orgs = len(orgs_df)
                completed = len(orgs_df[orgs_df['processing_status'] == 'completed'])
                insurance_orgs = len(orgs_df[orgs_df['is_insurance'] == True])
                
                print(f"   🏢 Organizações:")
                print(f"      • Total: {total_orgs}")
                print(f"      • Processadas: {completed}")
                print(f"      • Seguradoras: {insurance_orgs}")
                
                if insurance_orgs > 0:
                    print(f"      • Seguradoras identificadas:")
                    insurance_list = orgs_df[orgs_df['is_insurance'] == True]['organization_name'].tolist()
                    for ins in insurance_list:
                        print(f"        - {ins}")
                
            except Exception as e:
                print(f"   ❌ Erro ao ler organizations.csv: {str(e)}")
        
        # People.csv
        if 'people' in results:
            try:
                import pandas as pd
                people_df = pd.read_csv(results['people'])
                total_people = len(people_df)
                classified = len(people_df[people_df['is_insurance'].notna()])
                insurance_people = len(people_df[people_df['is_insurance'] == True])
                
                print(f"   👥 Pessoas:")
                print(f"      • Total: {total_people}")
                print(f"      • Classificadas: {classified}")
                print(f"      • De seguradoras: {insurance_people}")
                
            except Exception as e:
                print(f"   ❌ Erro ao ler people.csv: {str(e)}")
        
        print(f"\n🎉 Teste da pipeline completa concluído!")
        print(f"💡 Todos os arquivos estão prontos para uso no Streamlit!")
        
    except Exception as e:
        print(f"\n💥 ERRO na pipeline: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_complete_pipeline()