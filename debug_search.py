#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Debug do Web Searcher - Diagnóstico detalhado
"""

import requests
import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import sys
from pathlib import Path

# Desabilitar avisos de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def debug_google_search():
    """Debug detalhado da busca Google"""
    
    org_name = "Microsoft Corporation"
    query = f'"{org_name}"'
    search_url = f"https://www.google.com/search?q={requests.utils.quote(query)}&num=10&hl=en"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    
    print(f"🔍 Testando busca Google para: {org_name}")
    print(f"URL de busca: {search_url}")
    
    try:
        response = requests.get(
            search_url,
            headers=headers,
            timeout=10,
            verify=False,
            allow_redirects=True
        )
        
        print(f"Status code: {response.status_code}")
        print(f"Content length: {len(response.text)}")
        
        if response.status_code != 200:
            print("❌ Google não retornou status 200")
            return
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Debug: salvar HTML para análise
        with open('google_response.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        print("📄 HTML salvo em google_response.html")
        
        # Verificar links encontrados
        all_links = soup.find_all('a', href=True)
        print(f"Total de links encontrados: {len(all_links)}")
        
        http_links = [link for link in all_links if link.get('href', '').startswith('http')]
        print(f"Links HTTP encontrados: {len(http_links)}")
        
        # Mostrar primeiros 10 links HTTP
        print("\n📋 Primeiros 10 links HTTP:")
        for i, link in enumerate(http_links[:10]):
            href = link.get('href', '')
            text = link.get_text(strip=True)[:50]
            print(f"  {i+1}. {href}")
            print(f"     Texto: {text}")
        
        # Testar seletores específicos
        selectors = [
            'a[href^="http"]',
            'div.g a',
            'h3 a',
            'div.yuRUbf a',
            '.r a'
        ]
        
        print("\n🎯 Testando seletores:")
        for selector in selectors:
            links = soup.select(selector)
            print(f"  {selector}: {len(links)} links")
            
            for link in links[:3]:  # Mostrar primeiros 3
                href = link.get('href', '')
                if href.startswith('http'):
                    print(f"    - {href}")
        
        # Testar validação de uma URL conhecida
        test_url = "https://www.microsoft.com"
        print(f"\n🧪 Testando validação de URL conhecida: {test_url}")
        
        try:
            response = requests.head(
                test_url,
                headers=headers,
                timeout=5,
                verify=False,
                allow_redirects=True
            )
            print(f"  Status: {response.status_code}")
            print(f"  Válida: {response.status_code in [200, 301, 302, 303, 307, 308]}")
        except Exception as e:
            print(f"  Erro na validação: {str(e)}")
        
    except Exception as e:
        print(f"❌ Erro na busca: {str(e)}")

if __name__ == "__main__":
    debug_google_search()