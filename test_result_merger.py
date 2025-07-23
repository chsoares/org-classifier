#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Teste do Result Merger com dataset processado
"""

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core.result_merger import ResultMerger
from pipeline.main_processor import MainProcessor


def test_result_merger():
    """Testa o merge de resultados com dataset processado"""
    
    print("🧪 Testando Result Merger")
    print("=" * 50)
    print()
    print("O QUE estamos testando:")
    print("- Carregar dataset processado (merged_data_normalized.csv)")
    print("- Carregar resultados de classificação do cache")
    print("- Fazer merge usando nomes normalizados")
    print("- Exportar resultado final")
    print()
    
    # Inicializar merger
    merger = ResultMerger()
    
    # 1. CARREGAR DATASET PROCESSADO
    print("📂 Carregando dataset processado...")
    dataset_path = "data/processed/merged_data_normalized.csv"
    
    try:
        processed_df = merger.load_original_dataset(dataset_path)
        print(f"✅ Dataset carregado: {len(processed_df)} linhas")
        print(f"📋 Colunas: {list(processed_df.columns)}")
        
        # Mostrar amostra de organizações
        if 'Home organization_normalized' in processed_df.columns:
            unique_orgs = processed_df['Home organization_normalized'].dropna().unique()
            print(f"🏢 Organizações únicas: {len(unique_orgs)}")
            print("   Primeiras 5:")
            for i, org in enumerate(unique_orgs[:5]):
                print(f"     {i+1}. {org}")
        
    except Exception as e:
        print(f"❌ Erro ao carregar dataset: {str(e)}")
        print("💡 Certifique-se de que o arquivo existe em data/processed/merged_data_normalized.csv")
        return
    
    print()
    
    # 2. CARREGAR RESULTADOS DE CLASSIFICAÇÃO
    print("📦 Carregando resultados de classificação...")
    classification_results = merger.load_classification_results()
    
    if not classification_results:
        print("⚠️ Nenhum resultado de classificação encontrado no cache")
        print("💡 Execute primeiro algumas classificações para ter dados para merge")
        
        # Vamos processar algumas organizações para ter dados
        print("\n🔄 Processando algumas organizações para demonstração...")
        processor = MainProcessor()
        
        # Pegar algumas organizações do dataset
        sample_orgs = processed_df['Home organization_normalized'].dropna().unique()[:3]
        
        for org in sample_orgs:
            print(f"   Processando: {org}")
            try:
                result = processor.process_single_organization(org)
                if result['success']:
                    classification = "SEGURADORA" if result['is_insurance'] else "NÃO-SEGURADORA"
                    print(f"   ✅ {classification}")
                else:
                    print(f"   ❌ Falha: {result['error_stage']}")
            except Exception as e:
                print(f"   💥 Erro: {str(e)}")
        
        # Recarregar resultados
        classification_results = merger.load_classification_results()
    
    print(f"📊 Resultados carregados: {len(classification_results)}")
    
    if classification_results:
        successful = len([r for r in classification_results.values() if r['success']])
        insurance = len([r for r in classification_results.values() if r.get('is_insurance') is True])
        
        print(f"   • Sucessos: {successful}")
        print(f"   • Seguradoras: {insurance}")
        
        print("\n📋 Exemplos de resultados:")
        for i, (org, result) in enumerate(list(classification_results.items())[:3]):
            if result['success']:
                status = "SEGURADORA" if result['is_insurance'] else "NÃO-SEGURADORA"
                print(f"     {i+1}. {org}: {status}")
    
    print()
    
    # 3. FAZER MERGE
    print("🔗 Fazendo merge dos resultados...")
    try:
        merged_df = merger.merge_results(processed_df, classification_results)
        
        print("✅ Merge concluído!")
        
        # Mostrar estatísticas
        stats = merger.get_merge_statistics()
        print(f"\n📊 Estatísticas do merge:")
        print(f"   • Total de participantes: {stats['total_participants']}")
        print(f"   • Organizações únicas: {stats['total_organizations']}")
        print(f"   • Organizações classificadas: {stats['organizations_classified']}")
        print(f"   • Taxa de classificação: {stats['classification_rate']:.1f}%")
        print(f"   • Seguradoras identificadas: {stats['insurance_organizations']}")
        print(f"   • Não-seguradoras: {stats['non_insurance_organizations']}")
        
    except Exception as e:
        print(f"❌ Erro no merge: {str(e)}")
        return
    
    print()
    
    # 4. EXPORTAR RESULTADOS
    print("💾 Exportando resultados...")
    try:
        exported_files = merger.export_results(merged_df)
        
        print("✅ Exportação concluída!")
        print(f"\n📁 Arquivos gerados:")
        for file_type, file_path in exported_files.items():
            print(f"   • {file_type.upper()}: {file_path}")
        
    except Exception as e:
        print(f"❌ Erro na exportação: {str(e)}")
        return
    
    print()
    
    # 5. VALIDAÇÃO
    print("🔍 Validando resultados...")
    validation = merger.validate_merge_results(merged_df)
    
    print(f"📊 Validação:")
    print(f"   • Linhas totais: {validation['total_rows']}")
    print(f"   • Com classificação: {validation['rows_with_classification']}")
    print(f"   • Seguradoras: {validation['insurance_companies']}")

    
    print()
    print("✅ Teste de Result Merger concluído!")


if __name__ == "__main__":
    test_result_merger()