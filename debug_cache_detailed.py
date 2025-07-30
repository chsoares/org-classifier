#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para debugar detalhadamente o problema do cache
"""

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core.cache_manager import CacheManager
from pipeline.main_processor import MainProcessor


def test_cache_step_by_step():
    """Testa o cache passo a passo para entender onde está falhando"""
    
    print("🔍 TESTE DETALHADO DO CACHE")
    print("=" * 60)
    
    org_name = "UNICEF"
    
    # 1. Testar CacheManager diretamente
    print(f"\n1️⃣ Testando CacheManager diretamente para {org_name}")
    print("-" * 40)
    
    cache_manager = CacheManager()
    
    # Verificar se existe
    exists = cache_manager.is_cached('full_results', org_name)
    print(f"Cache exists: {exists}")
    
    # Tentar carregar
    cached_data = cache_manager.load_from_cache('full_results', org_name)
    print(f"Cache loaded: {cached_data is not None}")
    
    if cached_data:
        print(f"Cache success: {cached_data.get('success')}")
        print(f"Cache is_insurance: {cached_data.get('is_insurance')}")
    
    # 2. Testar MainProcessor com debug
    print(f"\n2️⃣ Testando MainProcessor com debug para {org_name}")
    print("-" * 40)
    
    processor = MainProcessor()
    
    # Adicionar debug temporário
    print("Verificando cache no MainProcessor...")
    
    # Simular exatamente o que o MainProcessor faz
    cached_result = processor.cache_manager.load_from_cache('full_results', org_name)
    print(f"MainProcessor cache check: {cached_result is not None}")
    
    if cached_result:
        print("✅ Cache encontrado pelo MainProcessor")
        print(f"  Success: {cached_result.get('success')}")
        print(f"  Is Insurance: {cached_result.get('is_insurance')}")
    else:
        print("❌ Cache NÃO encontrado pelo MainProcessor")
        
        # Vamos debugar por que não encontrou
        print("\n🔍 Debugando por que não encontrou...")
        
        # Verificar se o arquivo existe
        cache_file_path = processor.cache_manager._get_cache_file_path('full_results', org_name)
        print(f"Cache file path: {cache_file_path}")
        print(f"File exists: {cache_file_path.exists()}")
        
        if cache_file_path.exists():
            # Tentar ler manualmente
            try:
                import json
                with open(cache_file_path, 'r', encoding='utf-8') as f:
                    raw_data = json.load(f)
                print(f"Raw file content keys: {list(raw_data.keys())}")
                print(f"Organization name in file: {raw_data.get('organization_name')}")
            except Exception as e:
                print(f"Error reading file: {e}")
    
    # 3. Testar processamento real
    print(f"\n3️⃣ Testando processamento real para {org_name}")
    print("-" * 40)
    
    result = processor.process_single_organization(org_name)
    print(f"Processing time: {result.get('total_time_seconds', 0):.3f}s")
    
    if result.get('total_time_seconds', 0) < 1.0:
        print("✅ Tempo baixo - provavelmente usou cache")
    else:
        print("❌ Tempo alto - provavelmente NÃO usou cache")


def test_multiple_organizations():
    """Testa com múltiplas organizações para ver o padrão"""
    
    print(f"\n4️⃣ Testando múltiplas organizações")
    print("-" * 40)
    
    test_orgs = ["UNICEF", "Presidential Court", "World Bank Group"]
    
    cache_manager = CacheManager()
    
    for org in test_orgs:
        exists = cache_manager.is_cached('full_results', org)
        print(f"{org}: cache exists = {exists}")


def main():
    """Função principal de debug detalhado"""
    
    test_cache_step_by_step()
    test_multiple_organizations()


if __name__ == "__main__":
    main()