#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Teste do Sistema de Cache
"""

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from pipeline.main_processor import MainProcessor


def test_cache_system():
    """Testa o sistema de cache do pipeline"""
    
    print("🧪 Testando Sistema de Cache")
    print("=" * 50)
    print()
    print("O QUE estamos testando:")
    print("- Cache salva resultados para evitar reprocessamento")
    print("- Segunda execução usa cache (muito mais rápida)")
    print("- Diferentes tipos de cache (busca, extração, classificação)")
    print()
    
    processor = MainProcessor()
    
    # Limpar cache para teste limpo
    print("🗑️ Limpando cache para teste limpo...")
    removed = processor.clear_cache()
    print(f"   Removidos: {removed} arquivos")
    print()
    
    # Organização para teste
    test_org = "Microsoft Corporation"
    
    print(f"📋 Testando com: {test_org}")
    print()
    
    # PRIMEIRA EXECUÇÃO (sem cache)
    print("🔄 PRIMEIRA EXECUÇÃO (processamento completo):")
    print("-" * 40)
    
    import time
    start_time = time.time()
    
    result1 = processor.process_single_organization(test_org)
    
    first_time = time.time() - start_time
    
    if result1['success']:
        classification = "SEGURADORA" if result1['is_insurance'] else "NÃO-SEGURADORA"
        print(f"✅ Resultado: {classification}")
        print(f"⏱️ Tempo: {first_time:.2f}s")
        print(f"🌐 Website: {result1['website_url']}")
        print(f"📄 Conteúdo: {len(result1.get('website_content', ''))} caracteres")
    else:
        print(f"❌ Falha: {result1['error_message']}")
        return
    
    print()
    
    # Verificar cache criado
    print("📊 Cache criado:")
    cache_stats = processor.get_cache_statistics()
    print(f"   Total de arquivos: {cache_stats['total_files']}")
    print(f"   Tamanho: {cache_stats['total_size_mb']:.2f} MB")
    
    for cache_type, stats in cache_stats['by_type'].items():
        if stats['files'] > 0:
            print(f"   • {cache_type}: {stats['files']} arquivo(s)")
    
    print()
    
    # SEGUNDA EXECUÇÃO (com cache)
    print("🚀 SEGUNDA EXECUÇÃO (usando cache):")
    print("-" * 40)
    
    start_time = time.time()
    
    result2 = processor.process_single_organization(test_org)
    
    second_time = time.time() - start_time
    
    if result2['success']:
        classification = "SEGURADORA" if result2['is_insurance'] else "NÃO-SEGURADORA"
        print(f"✅ Resultado: {classification}")
        print(f"⏱️ Tempo: {second_time:.2f}s")
        print(f"🌐 Website: {result2['website_url']}")
        print(f"📄 Conteúdo: {len(result2.get('website_content', ''))} caracteres")
    else:
        print(f"❌ Falha: {result2['error_message']}")
        return
    
    print()
    
    # Comparar resultados
    print("🔍 COMPARAÇÃO:")
    print("-" * 20)
    print(f"Primeira execução: {first_time:.2f}s")
    print(f"Segunda execução:  {second_time:.2f}s")
    
    if second_time < first_time:
        speedup = first_time / second_time
        print(f"🚀 Aceleração: {speedup:.1f}x mais rápido!")
    
    # Verificar se resultados são idênticos
    same_classification = result1['is_insurance'] == result2['is_insurance']
    same_website = result1['website_url'] == result2['website_url']
    
    print(f"📋 Classificação idêntica: {'✅' if same_classification else '❌'}")
    print(f"🌐 Website idêntico: {'✅' if same_website else '❌'}")
    
    print()
    
    # Testar limpeza de cache específico
    print("🧹 Testando limpeza de cache:")
    print("-" * 30)
    
    # Limpar apenas cache de classificação
    removed = processor.clear_cache('classification', test_org)
    print(f"Cache de classificação removido: {removed} arquivo(s)")
    
    # Listar organizações no cache
    cached_orgs = processor.list_cached_organizations()
    print(f"Organizações ainda no cache: {len(cached_orgs)}")
    for org in cached_orgs:
        print(f"  • {org}")
    
    print()
    print("✅ Teste de cache concluído!")


if __name__ == "__main__":
    test_cache_system()