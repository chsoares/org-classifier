#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Teste do Organization Web Extractor
"""

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from scraping.org_web_extractor import OrganizationWebExtractor

def test_extractor():
    """Teste completo do extractor"""
    extractor = OrganizationWebExtractor()
    
    # Casos de teste
    test_cases = [
        {
            'url': 'https://en.wikipedia.org/wiki/Microsoft',
            'org_name': 'Microsoft Corporation',
            'expected_type': 'wikipedia_summary'
        },
        {
            'url': 'https://en.wikipedia.org/wiki/Allianz',
            'org_name': 'Allianz SE',
            'expected_type': 'wikipedia_summary'
        },
        {
            'url': 'https://www.microsoft.com',
            'org_name': 'Microsoft Corporation',
            'expected_type': 'website_content'
        }
    ]
    
    print("🧪 TESTANDO ORGANIZATION WEB EXTRACTOR")
    print("=" * 60)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{i}. Testando: {test_case['org_name']}")
        print(f"   URL: {test_case['url']}")
        print(f"   Tipo esperado: {test_case['expected_type']}")
        
        try:
            result = extractor.extract_organization_content(
                test_case['url'], 
                test_case['org_name']
            )
            
            if result:
                print(f"   ✅ SUCESSO!")
                print(f"   📊 Dados extraídos:")
                print(f"      - Tipo: {result.get('content_type')}")
                print(f"      - Título: {result.get('title')}")
                print(f"      - Tamanho: {len(result.get('content', ''))} caracteres")
                print(f"      - URL fonte: {result.get('source_url')}")
                
                # Validar relevância
                is_relevant = extractor.validate_content_relevance(
                    result.get('content', ''), 
                    test_case['org_name']
                )
                print(f"      - Relevante: {'✅ Sim' if is_relevant else '❌ Não'}")
                
                # Mostrar amostra do conteúdo
                content_sample = result.get('content', '')[:300]
                print(f"      - Amostra: {content_sample}...")
                
                # Verificar se tipo está correto
                if result.get('content_type') == test_case['expected_type']:
                    print(f"      - Tipo correto: ✅")
                else:
                    print(f"      - Tipo incorreto: ❌ (esperado: {test_case['expected_type']})")
                
            else:
                print(f"   ❌ FALHA - Nenhum conteúdo extraído")
                
        except Exception as e:
            print(f"   ❌ ERRO: {str(e)}")
        
        print("-" * 40)
    
    print("\n📋 RESUMO DOS TESTES:")
    print("- Wikipedia: Deve extrair resumo, infobox e seções relevantes")
    print("- Website próprio: Deve extrair conteúdo principal e seções About")
    print("- Validação: Deve verificar relevância do conteúdo")
    print("- Limite: Conteúdo deve ter máximo 2000 caracteres")

if __name__ == "__main__":
    test_extractor()