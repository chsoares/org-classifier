#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Teste do Sistema de Validação
"""

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from testing.test_dataset_validator import TestDatasetValidator

def test_validation_system():
    """Teste do sistema de validação"""
    
    print("🧪 TESTANDO SISTEMA DE VALIDAÇÃO")
    print("=" * 60)
    
    try:
        # Inicializar validator
        validator = TestDatasetValidator()
        
        print("✅ Validator inicializado com sucesso")
        print(f"📋 Organizações conhecidas: {len(validator.known_organizations)}")
        
        # Mostrar organizações conhecidas
        print("\n📊 ORGANIZAÇÕES CONHECIDAS:")
        insurance_count = 0
        for org in validator.known_organizations:
            status = "✅ Insurance" if org['expected_classification'] else "❌ Not Insurance"
            print(f"   {org['name']} ({org['category']}) - {status}")
            if org['expected_classification']:
                insurance_count += 1
        
        print(f"\n   Total: {len(validator.known_organizations)} ({insurance_count} insurance, {len(validator.known_organizations) - insurance_count} non-insurance)")
        
        # Testar extração de organizações aleatórias
        print("\n🎲 TESTANDO EXTRAÇÃO DE ORGANIZAÇÕES ALEATÓRIAS:")
        random_orgs = validator.get_random_organizations_from_dataset(5)
        
        if random_orgs:
            print(f"✅ {len(random_orgs)} organizações aleatórias extraídas:")
            for org in random_orgs[:5]:
                print(f"   - {org['name']} (from {org.get('source_sheet', 'unknown')})")
        else:
            print("❌ Falha na extração de organizações aleatórias")
        
        # Testar pipeline em uma organização conhecida
        print("\n🔄 TESTANDO PIPELINE COMPLETO:")
        test_org = validator.known_organizations[0]  # Allianz SE
        print(f"Testando com: {test_org['name']}")
        
        result = validator.run_complete_pipeline_test(test_org)
        
        print(f"\n📊 RESULTADO DO TESTE:")
        print(f"   Organização: {result['organization']}")
        print(f"   Pipeline success: {'✅' if result['pipeline_success'] else '❌'}")
        print(f"   Classificação final: {'✅ Insurance' if result['final_classification'] else '❌ Not Insurance' if result['final_classification'] is False else '⚠️ Failed'}")
        print(f"   Tempo total: {result['total_time']:.2f}s")
        
        # Mostrar resultados por stage
        print(f"\n🔄 RESULTADOS POR STAGE:")
        for stage_name, stage_data in result['stages'].items():
            status = "✅" if stage_data.get('success', False) else "❌"
            time_info = f" ({stage_data.get('time_seconds', 0):.2f}s)" if 'time_seconds' in stage_data else ""
            print(f"   {stage_name}: {status}{time_info}")
            
            # Informações específicas por stage
            if stage_name == 'web_search' and stage_data.get('success'):
                print(f"      URL: {stage_data.get('url_found')}")
                print(f"      Method: {stage_data.get('search_method')}")
            elif stage_name == 'content_extraction' and stage_data.get('success'):
                print(f"      Content length: {stage_data.get('content_length')} chars")
                print(f"      Content type: {stage_data.get('content_type')}")
        
        # Mostrar erros se houver
        if result['errors']:
            print(f"\n⚠️ ERROS ENCONTRADOS:")
            for error in result['errors']:
                print(f"   - {error}")
        
        # Validar se classificação está correta
        if result['expected_classification'] is not None and result['final_classification'] is not None:
            is_correct = result['final_classification'] == result['expected_classification']
            print(f"\n🎯 VALIDAÇÃO:")
            print(f"   Esperado: {'Insurance' if result['expected_classification'] else 'Not Insurance'}")
            print(f"   Obtido: {'Insurance' if result['final_classification'] else 'Not Insurance'}")
            print(f"   Correto: {'✅ Sim' if is_correct else '❌ Não'}")
        
        print("\n" + "=" * 60)
        print("📋 PRÓXIMOS PASSOS:")
        print("1. Execute 'python src/testing/test_dataset_validator.py' para validação completa")
        print("2. Verifique os resultados em 'test_results/' após execução")
        print("3. Ajuste prompts/thresholds baseado nos resultados")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"❌ ERRO NO TESTE: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_validation_system()
    if success:
        print("\n✅ TESTE DO SISTEMA DE VALIDAÇÃO PASSOU!")
    else:
        print("\n❌ TESTE DO SISTEMA DE VALIDAÇÃO FALHOU!")