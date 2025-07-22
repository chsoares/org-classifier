#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Teste rápido do Web Searcher corrigido
"""

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from scraping.web_searcher import WebSearcher

def test_web_searcher():
    """Teste rápido das correções"""
    searcher = WebSearcher()
    
    # Testar com uma organização conhecida
    test_org = "Microsoft Corporation"
    print(f"\n🧪 Testando busca para: {test_org}")
    
    try:
        url, method = searcher.search_organization_website(test_org)
        
        if url:
            print(f"✅ Sucesso! URL encontrada via {method}: {url}")
        else:
            print(f"❌ Falha na busca ({method})")
            
    except Exception as e:
        print(f"❌ Erro durante teste: {str(e)}")
    
    # Verificar se Bing foi removido
    if hasattr(searcher, 'search_bing'):
        print("⚠️ AVISO: Método search_bing ainda existe!")
    else:
        print("✅ Método search_bing foi removido corretamente")

if __name__ == "__main__":
    test_web_searcher()