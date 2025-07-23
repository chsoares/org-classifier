#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Teste do Pipeline Principal - Versão Simplificada
"""

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from pipeline.main_processor import MainProcessor


def test_pipeline_simple():
    """Teste simples do pipeline com 3 organizações"""
    
    print("🚀 Testando Pipeline Principal")
    print("=" * 50)
    print()
    print("O QUE estamos testando:")
    print("- Pipeline que conecta busca → extração → classificação")
    print("- Tratamento de erros sem parar o processo")
    print("- Estatísticas e logs detalhados")
    print()
    
    # Organizações para teste (mix de seguradoras e não-seguradoras)
    test_orgs = [
        "Allianz SE",           # Seguradora alemã famosa
        "Microsoft Corporation", # Empresa de tecnologia
        "Swiss Re"              # Resseguradora suíça
    ]
    
    print(f"📋 Testando com {len(test_orgs)} organizações:")
    for i, org in enumerate(test_orgs, 1):
        print(f"  {i}. {org}")
    print()
    
    # Inicializar pipeline
    processor = MainProcessor()
    
    # Processar organizações
    results = processor.process_organization_list(test_orgs)
    
    # Mostrar resumo
    print("\n🎯 RESUMO DOS RESULTADOS:")
    print("-" * 30)
    
    for result in results:
        org = result['organization_name']
        
        if result['success']:
            classification = "🏢 SEGURADORA" if result['is_insurance'] else "🏭 NÃO-SEGURADORA"
            time_taken = result['total_time_seconds']
            method = result['search_method']
            source_type = result.get('content_source_type', 'unknown')
            content_length = len(result.get('website_content', ''))
            
            print(f"✅ {org}")
            print(f"   → {classification}")
            print(f"   → Website encontrado via: {method}")
            print(f"   → Conteúdo extraído de: {source_type} ({content_length} caracteres)")
            print(f"   → Tempo total: {time_taken:.1f}s")
            print()
        else:
            error_stage = result['error_stage']
            error_msg = result['error_message']
            
            print(f"❌ {org}")
            print(f"   → FALHA na etapa: {error_stage}")
            print(f"   → Erro: {error_msg}")
            print()


if __name__ == "__main__":
    test_pipeline_simple()