#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para debugar o funcionamento do cache
"""

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core.cache_manager import CacheManager


def test_cache_for_organization(org_name: str):
    """Testa se o cache está funcionando para uma organização específica"""
    
    print(f"🔍 Testando cache para: {org_name}")
    print("=" * 50)
    
    cache_manager = CacheManager()
    
    # Testar cada tipo de cache
    cache_types = ['web_search', 'content_extraction', 'classification', 'full_results']
    
    for cache_type in cache_types:
        print(f"\n📦 Testando cache tipo: {cache_type}")
        
        # Verificar se existe
        exists = cache_manager.is_cached(cache_type, org_name)
        print(f"  Existe: {exists}")
        
        if exists:
            # Tentar carregar
            try:
                data = cache_manager.load_from_cache(cache_type, org_name)
                if data:
                    print(f"  ✅ Carregado com sucesso")
                    if cache_type == 'full_results':
                        print(f"    - Success: {data.get('success')}")
                        print(f"    - Is Insurance: {data.get('is_insurance')}")
                        print(f"    - Search Method: {data.get('search_method')}")
                else:
                    print(f"  ❌ Falha ao carregar (retornou None)")
            except Exception as e:
                print(f"  ❌ Erro ao carregar: {str(e)}")
        
        # Mostrar info do arquivo
        info = cache_manager.get_cache_info(cache_type, org_name)
        if info:
            print(f"  Timestamp: {info['timestamp']}")
            print(f"  Tamanho: {info['file_size']} bytes")


def test_main_processor_cache():
    """Testa se o MainProcessor está usando o cache corretamente"""
    
    print("\n🧪 Testando MainProcessor")
    print("=" * 50)
    
    from pipeline.main_processor import MainProcessor
    
    processor = MainProcessor()
    
    # Testar com UNICEF que sabemos que tem cache
    org_name = "UNICEF"
    
    print(f"🔄 Processando {org_name} (deveria usar cache)...")
    
    result = processor.process_single_organization(org_name)
    
    print(f"\nResultado:")
    print(f"  Success: {result.get('success')}")
    print(f"  Is Insurance: {result.get('is_insurance')}")
    print(f"  Search Method: {result.get('search_method')}")
    print(f"  Total Time: {result.get('total_time_seconds'):.3f}s")
    
    # Se demorou mais que 1 segundo, provavelmente não usou cache
    if result.get('total_time_seconds', 0) > 1.0:
        print("  ⚠️ SUSPEITO: Tempo muito alto, pode não ter usado cache")
    else:
        print("  ✅ Tempo baixo, provavelmente usou cache")


def main():
    """Função principal de debug"""
    
    print("🐛 DEBUG DO SISTEMA DE CACHE")
    print("=" * 60)
    
    # Testar organizações que sabemos que têm cache
    test_organizations = ["UNICEF", "Asian Development Bank"]
    
    for org in test_organizations:
        test_cache_for_organization(org)
        print("\n" + "-" * 60)
    
    # Testar MainProcessor
    test_main_processor_cache()


if __name__ == "__main__":
    main()