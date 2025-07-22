#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Teste individual de cada motor de busca
"""

import requests
import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urlparse, quote
import time
import json

# Desabilitar avisos de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class SearchEngineTester:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        self.timeout = 10
    
    def test_google(self, org_name: str):
        """Testar Google"""
        print(f"\n🔍 TESTANDO GOOGLE para: {org_name}")
        
        query = f'"{org_name}"'
        search_url = f"https://www.google.com/search?q={quote(query)}&num=10&hl=en"
        
        print(f"URL: {search_url}")
        
        try:
            response = requests.get(
                search_url,
                headers=self.headers,
                timeout=self.timeout,
                verify=False,
                allow_redirects=True
            )
            
            print(f"Status: {response.status_code}")
            print(f"Content length: {len(response.text)}")
            
            if response.status_code == 429:
                print("❌ Rate limited (429)")
                return None
            elif response.status_code != 200:
                print(f"❌ Status não é 200: {response.status_code}")
                return None
            
            # Salvar HTML para debug
            with open(f'google_{org_name.replace(" ", "_")}.html', 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Contar links
            all_links = soup.find_all('a', href=True)
            http_links = [link for link in all_links if link.get('href', '').startswith('http')]
            
            print(f"Total links: {len(all_links)}")
            print(f"HTTP links: {len(http_links)}")
            
            # Mostrar primeiros 5 HTTP links
            print("Primeiros 5 HTTP links:")
            for i, link in enumerate(http_links[:5]):
                href = link.get('href', '')
                text = link.get_text(strip=True)[:50]
                print(f"  {i+1}. {href}")
                print(f"     Texto: {text}")
            
            return http_links[:5] if http_links else None
            
        except Exception as e:
            print(f"❌ Erro: {str(e)}")
            return None
    
    def test_duckduckgo(self, org_name: str):
        """Testar DuckDuckGo"""
        print(f"\n🦆 TESTANDO DUCKDUCKGO para: {org_name}")
        
        query = f'"{org_name}"'
        search_url = f"https://duckduckgo.com/html/?q={quote(query)}&kl=us-en"
        
        print(f"URL: {search_url}")
        
        try:
            response = requests.get(
                search_url,
                headers=self.headers,
                timeout=self.timeout,
                verify=False,
                allow_redirects=True
            )
            
            print(f"Status: {response.status_code}")
            print(f"Content length: {len(response.text)}")
            
            if response.status_code != 200:
                print(f"❌ Status não é 200: {response.status_code}")
                return None
            
            # Salvar HTML para debug
            with open(f'duckduckgo_{org_name.replace(" ", "_")}.html', 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Testar seletores DuckDuckGo
            selectors = [
                '.result__url',
                '.result__a',
                'a.result__a[href]',
                '.result-link',
                'a[href^="http"]'
            ]
            
            found_links = []
            
            for selector in selectors:
                results = soup.select(selector)
                print(f"Seletor '{selector}': {len(results)} resultados")
                
                for result in results[:3]:
                    href = result.get('href', '') or result.get_text(strip=True)
                    if href and 'http' in href:
                        found_links.append(href)
                        print(f"  - {href}")
            
            return found_links[:5] if found_links else None
            
        except Exception as e:
            print(f"❌ Erro: {str(e)}")
            return None
    
    def test_bing(self, org_name: str):
        """Testar Bing"""
        print(f"\n🅱️ TESTANDO BING para: {org_name}")
        
        query = f'"{org_name}"'
        search_url = f"https://www.bing.com/search?q={quote(query)}&count=10"
        
        print(f"URL: {search_url}")
        
        try:
            response = requests.get(
                search_url,
                headers=self.headers,
                timeout=self.timeout,
                verify=False,
                allow_redirects=True
            )
            
            print(f"Status: {response.status_code}")
            print(f"Content length: {len(response.text)}")
            
            if response.status_code != 200:
                print(f"❌ Status não é 200: {response.status_code}")
                return None
            
            # Salvar HTML para debug
            with open(f'bing_{org_name.replace(" ", "_")}.html', 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Testar seletores Bing
            selectors = [
                'li.b_algo h2 a',
                '.b_algo a[href^="http"]',
                'a[href^="http"]'
            ]
            
            found_links = []
            
            for selector in selectors:
                results = soup.select(selector)
                print(f"Seletor '{selector}': {len(results)} resultados")
                
                for result in results[:3]:
                    href = result.get('href', '')
                    if href and href.startswith('http'):
                        found_links.append(href)
                        print(f"  - {href}")
            
            return found_links[:5] if found_links else None
            
        except Exception as e:
            print(f"❌ Erro: {str(e)}")
            return None
    
    def test_searx(self, org_name: str):
        """Testar Searx"""
        print(f"\n🔍 TESTANDO SEARX para: {org_name}")
        
        # Instâncias Searx para testar
        instances = [
            "https://searx.be",
            "https://search.sapti.me",
            "https://searx.info"
        ]
        
        query = f'"{org_name}"'
        
        for instance in instances:
            print(f"\nTestando instância: {instance}")
            
            try:
                # Testar formato JSON
                search_url = f"{instance}/search?q={quote(query)}&format=json"
                print(f"URL JSON: {search_url}")
                
                response = requests.get(
                    search_url,
                    headers=self.headers,
                    timeout=self.timeout,
                    verify=False
                )
                
                print(f"Status: {response.status_code}")
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        results = data.get('results', [])
                        print(f"Resultados JSON: {len(results)}")
                        
                        for i, result in enumerate(results[:3]):
                            url = result.get('url', '')
                            title = result.get('title', '')
                            print(f"  {i+1}. {url}")
                            print(f"     Título: {title}")
                        
                        if results:
                            return [r.get('url', '') for r in results[:5]]
                            
                    except json.JSONDecodeError:
                        print("❌ Resposta não é JSON válido")
                
                # Testar formato HTML se JSON falhar
                search_url = f"{instance}/search?q={quote(query)}"
                print(f"URL HTML: {search_url}")
                
                response = requests.get(
                    search_url,
                    headers=self.headers,
                    timeout=self.timeout,
                    verify=False
                )
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    links = soup.select('a[href^="http"]')
                    print(f"Links HTML encontrados: {len(links)}")
                    
                    found_links = []
                    for link in links[:3]:
                        href = link.get('href', '')
                        if href:
                            found_links.append(href)
                            print(f"  - {href}")
                    
                    if found_links:
                        return found_links
                
            except Exception as e:
                print(f"❌ Erro na instância {instance}: {str(e)}")
                continue
        
        return None
    
    def test_wikipedia(self, org_name: str):
        """Testar Wikipedia"""
        print(f"\n📚 TESTANDO WIKIPEDIA para: {org_name}")
        
        # Buscar na Wikipedia
        search_url = f"https://en.wikipedia.org/w/api.php"
        params = {
            'action': 'query',
            'format': 'json',
            'list': 'search',
            'srsearch': org_name,
            'srlimit': 5
        }
        
        try:
            response = requests.get(
                search_url,
                params=params,
                headers=self.headers,
                timeout=self.timeout,
                verify=False
            )
            
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                search_results = data.get('query', {}).get('search', [])
                
                print(f"Resultados encontrados: {len(search_results)}")
                
                found_links = []
                for result in search_results:
                    title = result.get('title', '')
                    page_url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"
                    found_links.append(page_url)
                    print(f"  - {page_url}")
                    print(f"    Título: {title}")
                
                return found_links
            
        except Exception as e:
            print(f"❌ Erro: {str(e)}")
        
        return None

def main():
    """Testar todos os motores de busca"""
    tester = SearchEngineTester()
    
    # Organização para testar
    test_org = "Microsoft Corporation"
    
    print(f"🧪 TESTANDO TODOS OS MOTORES DE BUSCA")
    print(f"Organização: {test_org}")
    print("=" * 60)
    
    # Testar cada motor
    results = {}
    
    results['google'] = tester.test_google(test_org)
    time.sleep(2)  # Pausa entre testes
    
    results['duckduckgo'] = tester.test_duckduckgo(test_org)
    time.sleep(2)
    
    results['bing'] = tester.test_bing(test_org)
    time.sleep(2)
    
    results['searx'] = tester.test_searx(test_org)
    time.sleep(2)
    
    results['wikipedia'] = tester.test_wikipedia(test_org)
    
    # Resumo final
    print("\n" + "=" * 60)
    print("📊 RESUMO DOS RESULTADOS:")
    print("=" * 60)
    
    for engine, links in results.items():
        status = "✅ FUNCIONOU" if links else "❌ FALHOU"
        count = len(links) if links else 0
        print(f"{engine.upper()}: {status} ({count} links)")
        
        if links:
            print("  Primeiros links:")
            for i, link in enumerate(links[:3]):
                print(f"    {i+1}. {link}")
        print()

if __name__ == "__main__":
    main()