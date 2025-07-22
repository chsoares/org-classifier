#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Web Searcher - Sistema de busca de websites de organizações

Este módulo é responsável por:
1. Buscar websites de organizações usando múltiplos motores de busca
2. Tentar Google primeiro, depois DuckDuckGo, depois Bing
3. Filtrar resultados irrelevantes (redes sociais, etc.)
4. Validar URLs encontradas
5. Implementar retry logic e tratamento de erros
"""

import requests
import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import re
import time
from typing import Optional, Tuple
import sys
from pathlib import Path

# Desabilitar avisos de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Adicionar src ao path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger_config import setup_logger
from utils.config_manager import config_manager


class WebSearcher:
    """
    Sistema de busca de websites de organizações
    """
    
    def __init__(self):
        self.logger, _ = setup_logger("web_searcher", log_to_file=True)
        self.scraping_config = config_manager.get_scraping_config()
        
        self.logger.info("🔍 Inicializando Web Searcher")
        
        # Configurações de busca
        self.timeout = self.scraping_config['timeout']
        self.max_retries = self.scraping_config['max_retries']
        self.retry_delay = self.scraping_config['retry_delay']
        
        # Headers para requisições
        self.headers = {
            "User-Agent": self.scraping_config['user_agent'],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        
        # Sites irrelevantes para filtrar (removida Wikipedia - é boa fonte)
        self.irrelevant_domains = {
            'facebook.com', 'instagram.com', 'twitter.com', 'x.com', 'linkedin.com',
            'youtube.com', 'tiktok.com', 'pinterest.com', 'reddit.com',
            'google.com', 'bing.com', 'yahoo.com', 'duckduckgo.com',
            'webcache.googleusercontent.com', 'translate.google.com',
            'amazon.com', 'ebay.com', 'alibaba.com'
        }
        
        self.logger.debug(f"Configurações: timeout={self.timeout}s, retries={self.max_retries}")
    
    def search_organization_website(self, org_name: str) -> Tuple[Optional[str], str]:
        """
        Busca website de uma organização usando a estratégia do web_extractor
        
        Args:
            org_name: Nome da organização
            
        Returns:
            Tuple com (URL encontrada, método usado)
        """
        self.logger.info(f"🔍 Buscando website para: {org_name}")
        
        # 1. Tentar Wikipedia primeiro (sabemos que funciona)
        try:
            url = self.search_wikipedia(org_name)
            if url:
                self.logger.success(f"✨ Website encontrado via Wikipedia: {url}")
                return url, "wikipedia"
        except Exception as e:
            self.logger.debug(f"Falha na busca Wikipedia: {str(e)}")
        
        # 2. Usar estratégia do web_extractor (DuckDuckGo + Bing)
        try:
            url = self.search_integrated(org_name)
            if url:
                self.logger.success(f"✨ Website encontrado via busca integrada: {url}")
                return url, "integrated"
        except Exception as e:
            self.logger.warning(f"⚠️ Falha na busca integrada: {str(e)}")
        
        self.logger.error(f"❌ Nenhum website encontrado para: {org_name}")
        return None, "failed"
    
    def search_wikipedia(self, org_name: str) -> Optional[str]:
        """
        Busca na Wikipedia usando API
        
        Args:
            org_name: Nome da organização
            
        Returns:
            URL da Wikipedia encontrada ou None
        """
        self.logger.debug(f"🔍 Buscando na Wikipedia: {org_name}")
        
        try:
            # Usar API da Wikipedia
            search_url = "https://en.wikipedia.org/w/api.php"
            params = {
                'action': 'query',
                'format': 'json',
                'list': 'search',
                'srsearch': org_name,
                'srlimit': 3
            }
            
            response = requests.get(
                search_url,
                params=params,
                headers=self.headers,
                timeout=self.timeout,
                verify=False
            )
            
            if response.status_code == 200:
                data = response.json()
                search_results = data.get('query', {}).get('search', [])
                
                if search_results:
                    # Pegar o primeiro resultado
                    first_result = search_results[0]
                    title = first_result.get('title', '')
                    page_url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"
                    
                    self.logger.debug(f"Wikipedia encontrada: {page_url}")
                    return page_url
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Erro na busca Wikipedia: {str(e)}")
            return None
    
    def search_integrated(self, org_name: str) -> Optional[str]:
        """
        Busca integrada usando a estratégia do web_extractor
        Tenta DuckDuckGo primeiro, depois Bing
        
        Args:
            org_name: Nome da organização
            
        Returns:
            URL encontrada ou None
        """
        self.logger.debug(f"🔍 Busca integrada para: {org_name}")
        
        # Preparar query de busca
        search_query = f'"{org_name}"'
        
        # Headers específicos como no web_extractor
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        
        # 1. Tentar DuckDuckGo primeiro
        try:
            search_url = f"https://duckduckgo.com/html/?q={requests.utils.quote(search_query)}"
            
            response = requests.get(
                search_url, 
                headers=headers, 
                timeout=self.timeout, 
                verify=False
            )
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                
                # Usar seletor específico do web_extractor
                for result in soup.select(".result__url"):
                    href = result.get("href", "")
                    if href:
                        # Decodificar URL se necessário
                        if href.startswith("/"):
                            href = f"https:{href}"
                        
                        # Verificar se é uma URL válida (filtros do web_extractor)
                        if not any(
                            x in href.lower()
                            for x in [
                                "google.com", "youtube.com", "facebook.com", 
                                "instagram.com", "twitter.com", "wikipedia.org",
                                "webcache", "duckduckgo.com", "bing.com", "yahoo.com",
                            ]
                        ):
                            # Validar se a URL responde
                            if self._validate_url_response(href, headers):
                                self.logger.debug(f"URL válida encontrada via DuckDuckGo: {href}")
                                return href
        
        except Exception as e:
            self.logger.debug(f"Erro no DuckDuckGo integrado: {str(e)}")
        
        # 2. Fallback para Bing
        try:
            bing_url = f"https://www.bing.com/search?q={requests.utils.quote(search_query)}"
            response = requests.get(
                bing_url, 
                headers=headers, 
                timeout=self.timeout, 
                verify=False
            )
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                
                # Usar seletor específico do web_extractor
                for result in soup.select("li.b_algo h2 a"):
                    href = result.get("href", "")
                    if (
                        href
                        and href.startswith("http")
                        and not any(
                            x in href.lower()
                            for x in [
                                "google.com", "youtube.com", "facebook.com",
                                "instagram.com", "twitter.com", "wikipedia.org",
                                "webcache", "duckduckgo.com", "bing.com", "yahoo.com",
                            ]
                        )
                    ):
                        # Validar se a URL responde
                        if self._validate_url_response(href, headers):
                            self.logger.debug(f"URL válida encontrada via Bing: {href}")
                            return href
        
        except Exception as e:
            self.logger.debug(f"Erro no Bing integrado: {str(e)}")
        
        return None
    
    def _validate_url_response(self, url: str, headers: dict) -> bool:
        """
        Valida se uma URL responde (como no web_extractor)
        
        Args:
            url: URL para validar
            headers: Headers para usar na requisição
            
        Returns:
            True se a URL responde
        """
        try:
            response = requests.head(
                url,
                headers=headers,
                timeout=self.timeout,
                verify=False,
                allow_redirects=True,
            )
            return response.status_code == 200
        except Exception:
            return False
    
    def search_google(self, org_name: str) -> Optional[str]:
        """
        Busca no Google com tratamento de rate limiting
        
        Args:
            org_name: Nome da organização
            
        Returns:
            URL encontrada ou None
        """
        self.logger.debug(f"🔍 Buscando no Google: {org_name}")
        
        # Headers mais variados para evitar detecção
        google_headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,pt;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0"
        }
        
        # Usar busca simples
        query = f'"{org_name}"'
        search_url = f"https://www.google.com/search?q={requests.utils.quote(query)}&num=10&hl=en"
        
        # Tentar com retry para rate limiting
        for attempt in range(3):
            try:
                # Delay progressivo para evitar rate limiting
                if attempt > 0:
                    delay = 2 ** attempt  # 2s, 4s, 8s
                    self.logger.debug(f"Aguardando {delay}s antes da tentativa {attempt + 1}")
                    time.sleep(delay)
                
                response = requests.get(
                    search_url,
                    headers=google_headers,
                    timeout=self.timeout,
                    verify=False,
                    allow_redirects=True
                )
                
                if response.status_code == 429:
                    self.logger.debug(f"Rate limited (429) na tentativa {attempt + 1}")
                    continue
                elif response.status_code != 200:
                    self.logger.debug(f"Google retornou status {response.status_code}")
                    continue
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Debug: verificar se encontrou algum link
                all_links = soup.find_all('a', href=True)
                http_links = [link for link in all_links if link.get('href', '').startswith('http')]
                self.logger.debug(f"Total de links HTTP encontrados: {len(http_links)}")
                
                # Seletores mais simples e abrangentes
                selectors = [
                    'a[href^="http"]',  # Qualquer link HTTP
                    'div.g a',          # Links em resultados
                    'h3 a',             # Links em títulos
                ]
                
                found_urls = []
                
                for selector in selectors:
                    links = soup.select(selector)
                    self.logger.debug(f"Seletor '{selector}' encontrou {len(links)} links")
                    
                    for link in links[:10]:  # Mais resultados
                        href = link.get('href', '')
                        if not href or not href.startswith('http'):
                            continue
                        
                        # Limpar URLs do Google
                        if '/url?q=' in href:
                            try:
                                from urllib.parse import parse_qs, urlparse
                                parsed = urlparse(href)
                                if parsed.query:
                                    query_params = parse_qs(parsed.query)
                                    if 'q' in query_params:
                                        href = query_params['q'][0]
                            except:
                                continue
                        
                        if href not in found_urls:
                            found_urls.append(href)
                            self.logger.debug(f"Testando URL: {href}")
                            
                            if self._is_valid_result(href, org_name):
                                self.logger.debug(f"URL válida encontrada: {href}")
                                return href
                
                self.logger.debug(f"Nenhuma URL válida encontrada entre {len(found_urls)} URLs testadas")
                return None
                
            except Exception as e:
                self.logger.debug(f"Erro na busca Google (tentativa {attempt + 1}): {str(e)}")
                continue
        
        self.logger.debug("Todas as tentativas do Google falharam")
        return None
    

    
    def search_duckduckgo(self, org_name: str) -> Optional[str]:
        """
        Busca no DuckDuckGo com melhor parsing
        
        Args:
            org_name: Nome da organização
            
        Returns:
            URL encontrada ou None
        """
        self.logger.debug(f"🔍 Buscando no DuckDuckGo: {org_name}")
        
        # Preparar query de busca
        query = f'"{org_name}"'
        search_url = f"https://duckduckgo.com/html/?q={requests.utils.quote(query)}&kl=us-en"
        
        try:
            response = self._make_request(search_url)
            if not response:
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Múltiplos seletores para DuckDuckGo
            selectors = [
                '.result__url',           # Seletor principal
                '.result__a',             # Alternativo
                'a.result__a[href]'       # Mais específico
            ]
            
            for selector in selectors:
                results = soup.select(selector)
                for result in results[:5]:  # Apenas primeiros 5
                    href = result.get('href', '')
                    if not href:
                        continue
                    
                    # Limpar URLs de redirecionamento do DuckDuckGo
                    if href.startswith('/l/?uddg='):
                        continue
                    
                    # Normalizar URL
                    if not href.startswith('http'):
                        if href.startswith('//'):
                            href = f"https:{href}"
                        elif href.startswith('/'):
                            continue  # Skip relative URLs
                        else:
                            href = f"https://{href}"
                    
                    if self._is_valid_result(href, org_name):
                        return href
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Erro na busca DuckDuckGo: {str(e)}")
            return None
    
    def search_searx(self, org_name: str) -> Optional[str]:
        """
        Busca usando Searx (motor de busca open source)
        
        Args:
            org_name: Nome da organização
            
        Returns:
            URL encontrada ou None
        """
        self.logger.debug(f"🔍 Buscando no Searx: {org_name}")
        
        # Instâncias públicas do Searx
        searx_instances = [
            "https://searx.be",
            "https://search.sapti.me",
            "https://searx.info",
        ]
        
        query = f'"{org_name}"'
        
        for instance in searx_instances:
            try:
                search_url = f"{instance}/search?q={requests.utils.quote(query)}&format=json"
                
                response = requests.get(
                    search_url,
                    headers=self.headers,
                    timeout=self.timeout,
                    verify=False
                )
                
                if response.status_code != 200:
                    continue
                
                data = response.json()
                results = data.get('results', [])
                
                for result in results[:5]:
                    url = result.get('url', '')
                    if url and self._is_valid_result(url, org_name):
                        return url
                        
            except Exception as e:
                self.logger.debug(f"Erro na instância Searx {instance}: {str(e)}")
                continue
        
        return None
    

    
    def _make_request(self, url: str) -> Optional[requests.Response]:
        """
        Faz requisição HTTP com retry logic
        
        Args:
            url: URL para requisição
            
        Returns:
            Response object ou None
        """
        for attempt in range(self.max_retries + 1):
            try:
                response = requests.get(
                    url,
                    headers=self.headers,
                    timeout=self.timeout,
                    verify=False,
                    allow_redirects=True
                )
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:  # Rate limited
                    if attempt < self.max_retries:
                        wait_time = self.retry_delay * (2 ** attempt)
                        self.logger.debug(f"Rate limited, aguardando {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                else:
                    self.logger.debug(f"Status code {response.status_code} para {url}")
                    return None
                    
            except requests.exceptions.Timeout:
                if attempt < self.max_retries:
                    self.logger.debug(f"Timeout na tentativa {attempt + 1}, tentando novamente...")
                    time.sleep(self.retry_delay)
                    continue
                else:
                    self.logger.debug(f"Timeout final para {url}")
                    return None
                    
            except Exception as e:
                if attempt < self.max_retries:
                    self.logger.debug(f"Erro na tentativa {attempt + 1}: {str(e)}")
                    time.sleep(self.retry_delay)
                    continue
                else:
                    self.logger.debug(f"Erro final para {url}: {str(e)}")
                    return None
        
        return None
    
    def _is_valid_result(self, url: str, org_name: str) -> bool:
        """
        Valida se um resultado de busca é relevante com lógica mais permissiva
        
        Args:
            url: URL do resultado
            org_name: Nome da organização
            
        Returns:
            True se é um resultado válido
        """
        try:
            # Parse da URL
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            full_url = url.lower()
            
            self.logger.debug(f"Validando URL: {url} para organização: {org_name}")
            
            # Remover www.
            if domain.startswith('www.'):
                domain = domain[4:]
            
            # Filtrar domínios irrelevantes (mais restritivo)
            if any(irrelevant in domain for irrelevant in self.irrelevant_domains):
                self.logger.debug(f"URL rejeitada - domínio irrelevante: {domain}")
                return False
            
            # Verificar se é um domínio válido
            if not domain or '.' not in domain:
                self.logger.debug(f"URL rejeitada - domínio inválido: {domain}")
                return False
            
            # Verificar se não é um subdomínio suspeito
            suspicious_subdomains = ['translate.', 'webcache.', 'cached.']
            if any(domain.startswith(sub) for sub in suspicious_subdomains):
                self.logger.debug(f"URL rejeitada - subdomínio suspeito: {domain}")
                return False
            
            # Filtrar URLs que claramente não são sites oficiais (mais permissivo)
            bad_patterns = [
                '/search?', '/q=', '/query=', '/results?'
            ]
            
            if any(pattern in full_url for pattern in bad_patterns):
                self.logger.debug(f"URL rejeitada - padrão suspeito na URL: {full_url}")
                return False
            
            # Validação mais permissiva - aceitar mais URLs
            # Primeiro, verificar se a URL responde
            if not self.validate_website_url(url):
                self.logger.debug(f"URL rejeitada - não responde: {url}")
                return False
            
            # Se chegou até aqui, é uma URL válida
            self.logger.debug(f"URL aceita: {url}")
            return True
            
        except Exception as e:
            self.logger.debug(f"Erro na validação de {url}: {str(e)}")
            return False
    
    def validate_website_url(self, url: str) -> bool:
        """
        Valida se uma URL é acessível
        
        Args:
            url: URL para validar
            
        Returns:
            True se a URL é válida e acessível
        """
        try:
            # Fazer HEAD request para verificar se o site responde
            response = requests.head(
                url,
                headers=self.headers,
                timeout=self.timeout,
                verify=False,
                allow_redirects=True
            )
            
            # Considerar códigos de sucesso
            return response.status_code in [200, 301, 302, 303, 307, 308]
            
        except Exception:
            # Se HEAD falhar, tentar GET rápido
            try:
                response = requests.get(
                    url,
                    headers=self.headers,
                    timeout=self.timeout // 2,  # Timeout menor para validação
                    verify=False,
                    allow_redirects=True,
                    stream=True  # Não baixar o conteúdo completo
                )
                return response.status_code in [200, 301, 302, 303, 307, 308]
            except Exception:
                return False
    
    def search_with_retry(self, org_name: str, max_attempts: int = 3) -> Tuple[Optional[str], str]:
        """
        Busca com retry automático em caso de falha
        
        Args:
            org_name: Nome da organização
            max_attempts: Número máximo de tentativas
            
        Returns:
            Tuple com (URL encontrada, método usado)
        """
        last_error = None
        
        for attempt in range(max_attempts):
            try:
                result = self.search_organization_website(org_name)
                if result[0]:  # Se encontrou URL
                    return result
                
                # Se não encontrou, aguardar antes da próxima tentativa
                if attempt < max_attempts - 1:
                    wait_time = self.retry_delay * (attempt + 1)
                    self.logger.debug(f"Tentativa {attempt + 1} falhou, aguardando {wait_time}s...")
                    time.sleep(wait_time)
                    
            except Exception as e:
                last_error = e
                self.logger.warning(f"Erro na tentativa {attempt + 1}: {str(e)}")
                
                if attempt < max_attempts - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
        
        # Se chegou aqui, todas as tentativas falharam
        error_msg = f"failed_after_{max_attempts}_attempts"
        if last_error:
            error_msg += f": {str(last_error)}"
        
        return None, error_msg


def main():
    """Função para testar o web searcher"""
    searcher = WebSearcher()
    
    # Testar com algumas organizações conhecidas
    test_orgs = [
        "Microsoft Corporation",
        "World Bank Group", 
        "United Nations",
        "Allianz SE",
        "Swiss Re"
    ]
    
    print(f"\n🧪 Testando Web Searcher com {len(test_orgs)} organizações:")
    
    for org in test_orgs:
        print(f"\n🔍 Buscando: {org}")
        url, method = searcher.search_organization_website(org)
        
        if url:
            print(f"  ✅ Encontrado via {method}: {url}")
        else:
            print(f"  ❌ Não encontrado ({method})")


if __name__ == "__main__":
    main()