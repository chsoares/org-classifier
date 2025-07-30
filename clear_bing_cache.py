#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para limpar cache de resultados gerados usando Bing

Este script:
1. Lê todos os JSONs em data/cache/full_results
2. Identifica organizações que foram processadas usando search_method = "bing"
3. Remove todos os tipos de cache para essas organizações
"""

import json
import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core.cache_manager import CacheManager


def find_bing_organizations():
    """
    Encontra todas as organizações que foram processadas usando Bing
    
    Returns:
        Set com nomes das organizações que usaram Bing
    """
    bing_organizations = set()
    full_results_dir = Path("data/cache/full_results")
    
    if not full_results_dir.exists():
        print(f"❌ Diretório não encontrado: {full_results_dir}")
        return bing_organizations
    
    print(f"🔍 Procurando organizações com search_method = 'bing' em {full_results_dir}")
    
    json_files = list(full_results_dir.glob("*.json"))
    print(f"📁 Encontrados {len(json_files)} arquivos JSON")
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Verificar se tem search_method = "bing" dentro de data
            if 'data' in data and isinstance(data['data'], dict):
                search_method = data['data'].get('search_method')
                organization_name = data['data'].get('organization_name')
                
                if search_method == 'bing' and organization_name:
                    bing_organizations.add(organization_name)
                    print(f"  ✅ Encontrado: {organization_name}")
        
        except Exception as e:
            print(f"  ⚠️ Erro ao ler {json_file.name}: {str(e)}")
            continue
    
    return bing_organizations


def clear_bing_cache():
    """
    Limpa todo o cache das organizações que foram processadas usando Bing
    """
    print("🧹 Iniciando limpeza de cache do Bing")
    print("=" * 50)
    
    # Encontrar organizações que usaram Bing
    bing_organizations = find_bing_organizations()
    
    if not bing_organizations:
        print("✅ Nenhuma organização encontrada com search_method = 'bing'")
        return
    
    print(f"\n📋 Encontradas {len(bing_organizations)} organizações que usaram Bing:")
    for org in sorted(bing_organizations):
        print(f"  • {org}")
    
    # Confirmar limpeza
    print(f"\n⚠️ Isso irá remover TODOS os tipos de cache para essas {len(bing_organizations)} organizações")
    response = input("Continuar? (s/N): ").strip().lower()
    
    if response not in ['s', 'sim', 'y', 'yes']:
        print("❌ Operação cancelada")
        return
    
    # Inicializar cache manager
    cache_manager = CacheManager()
    
    # Limpar cache para cada organização
    total_removed = 0
    
    print(f"\n🗑️ Limpando cache...")
    
    for org_name in sorted(bing_organizations):
        print(f"  Limpando: {org_name}")
        
        # Limpar todos os tipos de cache para esta organização
        removed = cache_manager.clear_cache(cache_type=None, org_name=org_name)
        total_removed += removed
        
        if removed > 0:
            print(f"    ✅ {removed} arquivos removidos")
        else:
            print(f"    ℹ️ Nenhum arquivo encontrado")
    
    print(f"\n✅ Limpeza concluída!")
    print(f"📊 Total de arquivos removidos: {total_removed}")
    print(f"🏢 Organizações processadas: {len(bing_organizations)}")


if __name__ == "__main__":
    clear_bing_cache()