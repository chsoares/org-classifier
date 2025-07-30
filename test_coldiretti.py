#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Teste específico para o caso Coldiretti
"""

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from scraping.web_searcher import WebSearcher

def test_coldiretti():
    """Testa busca específica para Coldiretti"""
    
    searcher = WebSearcher()
    
    print("🧪 Testando busca melhorada para Coldiretti...")
    
    url, method = searcher.search_organization_website("Coldiretti")
    
    print(f"Resultado: {url} (via {method})")
    
    if url:
        if "coldiretti.it" in url and not url.startswith("https://polo."):
            print("✅ SUCESSO: Encontrou domínio principal coldiretti.it!")
        elif "coldiretti.it" in url:
            print("⚠️ PARCIAL: Encontrou coldiretti.it mas é subdomínio")
        else:
            print("❌ ERRO: Não encontrou coldiretti.it")
    else:
        print("❌ ERRO: Nenhuma URL encontrada")

if __name__ == "__main__":
    test_coldiretti()