#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Web Searcher - Sistema de busca de websites de organizações

Este módulo implementa busca inteligente com:
1. Wikipedia primeiro com validação de relevância
2. Bing como fallback se Wikipedia não for relevante
3. Validação de URLs encontradas
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
    Sistema de busca de websites de organizações com Wikipedia + Bing
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
        
        # Sites irrelevantes para filtrar
        self.irrelevant_domains = {
            # Redes sociais
            'facebook.com', 'instagram.com', 'twitter.com', 'x.com', 'linkedin.com',
            'youtube.com', 'tiktok.com', 'pinterest.com', 'reddit.com',
            
            # Motores de busca
            'google.com', 'bing.com', 'yahoo.com', 'duckduckgo.com',
            'webcache.googleusercontent.com', 'translate.google.com',
            'baidu.com', 'sogou.com', 'yandex.com', 'ask.com',
            
            # E-commerce
            'amazon.com', 'ebay.com', 'alibaba.com', 'mercadolivre.com.br',
            
            # Fóruns e Q&A
            'stackoverflow.com', 'stackexchange.com', 'superuser.com',
            'serverfault.com', 'askubuntu.com', 'mathoverflow.net',
            'quora.com', 'answers.yahoo.com', 'answers.com',
            'reddit.com', 'discourse.org',
            
            # Fóruns específicos por domínio
            'forum.', 'forums.', 'community.', 'discuss.', 'discussion.',
            
            # Sites de notícias/blogs genéricos
            'medium.com', 'blogger.com', 'wordpress.com', 'tumblr.com',
            'substack.com', 'ghost.org',
            
            # Diretórios e agregadores
            'yellowpages.com', 'whitepages.com', 'yelp.com', 'foursquare.com',
            'zoominfo.com', 'crunchbase.com', 'bloomberg.com',
            
            # Sites de tradução e cache
            'translate.google.com', 'translate.bing.com', 'deepl.com',
            'webcache.googleusercontent.com', 'archive.org', 'web.archive.org',
            
            # Plataformas de desenvolvimento
            'github.com', 'gitlab.com', 'bitbucket.org', 'sourceforge.net',
            
            # Sites de emprego
            'indeed.com', 'glassdoor.com', 'monster.com', 'careerbuilder.com',
            'jobs.com', 'workday.com'
        }
        
        self.logger.debug(f"Configurações: timeout={self.timeout}s, retries={self.max_retries}")
    
    def search_organization_website(self, org_name: str) -> Tuple[Optional[str], str]:
        """
        Busca website de uma organização com validação inteligente
        1. Tenta Wikipedia primeiro com validação de relevância
        2. Se Wikipedia não for relevante, usa Bing como fallback
        3. Se Bing falhar, usa Wikipedia mesmo que irrelevante
        
        Args:
            org_name: Nome da organização
            
        Returns:
            Tuple com (URL encontrada, método usado)
        """
        self.logger.info(f"🔍 Buscando website para: {org_name}")
        
        # 1. Tentar Wikipedia primeiro
        try:
            wiki_url, wiki_title = self.search_wikipedia_with_validation(org_name)
            if wiki_url and self._is_wikipedia_result_relevant(wiki_title, org_name):
                self.logger.info(f"✨ Website encontrado via Wikipedia (relevante): {wiki_url}")
                return wiki_url, "wikipedia"
            elif wiki_url:
                self.logger.warning(f"⚠️ Wikipedia encontrada mas irrelevante: {wiki_title} para {org_name}")
                # Guardar para usar como fallback se necessário
                fallback_wiki = (wiki_url, wiki_title)
        except Exception as e:
            self.logger.debug(f"Falha na busca Wikipedia: {str(e)}")
            fallback_wiki = None
        
        # 2. Fallback para Bing (única engine que funciona)
        try:
            url = self.search_bing_working(org_name)
            if url:
                self.logger.info(f"✨ Website encontrado via Bing: {url}")
                return url, "bing"
        except Exception as e:
            self.logger.warning(f"⚠️ Falha na busca Bing: {str(e)}")
        
        # 3. Se Bing falhar, usar Wikipedia mesmo que irrelevante (melhor que nada)
        if 'fallback_wiki' in locals() and fallback_wiki:
            wiki_url, wiki_title = fallback_wiki
            self.logger.warning(f"⚠️ Usando Wikipedia irrelevante como último recurso: {wiki_url}")
            return wiki_url, "wikipedia_fallback"
        
        self.logger.error(f"❌ Nenhum website encontrado para: {org_name}")
        return None, "failed"
    
    def search_wikipedia_with_validation(self, org_name: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Busca na Wikipedia e retorna URL + título para validação
        
        Args:
            org_name: Nome da organização
            
        Returns:
            Tuple com (URL, título) ou (None, None)
        """
        self.logger.debug(f"📚 Buscando na Wikipedia: {org_name}")
        
        try:
            # Usar API da Wikipedia
            search_url = "https://en.wikipedia.org/w/api.php"
            params = {
                'action': 'query',
                'format': 'json',
                'list': 'search',
                'srsearch': org_name,
                'srlimit': 1  # Apenas primeiro resultado
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
                    first_result = search_results[0]
                    title = first_result.get('title', '')
                    page_url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"
                    
                    self.logger.debug(f"Wikipedia encontrada: {title} -> {page_url}")
                    return page_url, title
            
            return None, None
            
        except Exception as e:
            self.logger.debug(f"Erro na busca Wikipedia: {str(e)}")
            return None, None
    
    def _is_wikipedia_result_relevant(self, wiki_title: str, org_name: str) -> bool:
        """
        Valida se o resultado da Wikipedia é relevante para a organização
        Verifica se palavras-chave da organização aparecem no título
        
        Args:
            wiki_title: Título da página da Wikipedia
            org_name: Nome da organização
            
        Returns:
            True se o resultado é relevante
        """
        if not wiki_title or not org_name:
            return False
        
        # Normalizar strings
        title_lower = wiki_title.lower()
        org_lower = org_name.lower()
        
        # Extrair palavras significativas da organização (> 2 caracteres)
        org_words = [word for word in org_lower.split() if len(word) > 2]
        
        # Remover palavras comuns que não são distintivas
        common_words = {
            'ltd', 'inc', 'corp', 'corporation', 'company', 'group', 'limited', 
            'co', 'llc', 'se', 'sa', 'ag', 'gmbh', 'bv', 'nv', 'spa', 'srl', 
            'the', 'and', 'of', 'for', 'in', 'on', 'at', 'to', 'by', 'with'
        }
        distinctive_words = [word for word in org_words if word not in common_words]
        
        # Se não há palavras distintivas, usar todas as palavras > 2 chars
        words_to_check = distinctive_words if distinctive_words else org_words
        
        if not words_to_check:
            return False
        
        # Contar quantas palavras aparecem no título
        matches = 0
        for word in words_to_check:
            if word in title_lower:
                matches += 1
        
        # Calcular score de relevância
        relevance_score = matches / len(words_to_check)
        
        # Log para debug
        self.logger.debug(f"Relevância Wikipedia: '{wiki_title}' para '{org_name}'")
        self.logger.debug(f"  Palavras para verificar: {words_to_check}")
        self.logger.debug(f"  Matches: {matches}/{len(words_to_check)} = {relevance_score:.2f}")
        
        # Considerar relevante se pelo menos 50% das palavras coincidem
        # OU se é uma correspondência exata de palavra única
        is_relevant = relevance_score >= 0.5 or (len(words_to_check) == 1 and matches == 1)
        
        if is_relevant:
            self.logger.debug(f"  ✅ Relevante (score: {relevance_score:.2f})")
        else:
            self.logger.debug(f"  ❌ Irrelevante (score: {relevance_score:.2f})")
        
        return is_relevant
    
    def search_bing_working(self, org_name: str) -> Optional[str]:
        """
        Busca no Bing usando seletores que funcionam
        
        Args:
            org_name: Nome da organização
            
        Returns:
            URL encontrada ou None
        """
        self.logger.debug(f"🅱️ Buscando no Bing: {org_name}")
        
        query = f'"{org_name}"'
        search_url = f"https://www.bing.com/search?q={requests.utils.quote(query)}&count=10"
        
        try:
            response = requests.get(
                search_url,
                headers=self.headers,
                timeout=self.timeout,
                verify=False,
                allow_redirects=True
            )
            
            if response.status_code != 200:
                self.logger.debug(f"Bing retornou status {response.status_code}")
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Usar seletores mais precisos baseado em testes reais
            working_selectors = [
                'li.b_algo h2 a',           # Seletor principal - mais preciso
                'h2 a',                     # Alternativo para títulos principais  
                '.b_algo h2 a'              # Backup sem li
            ]
            
            # Coletar todas as URLs candidatas primeiro
            candidate_urls = []
            
            for selector in working_selectors:
                results = soup.select(selector)
                self.logger.debug(f"Seletor '{selector}': {len(results)} resultados")
                
                for result in results[:5]:  # Primeiros 5 resultados
                    href = result.get('href', '')
                    if href and href.startswith('http'):
                        candidate_urls.append(href)
            
            # Remover duplicatas mantendo ordem
            seen = set()
            unique_urls = []
            for url in candidate_urls:
                if url not in seen:
                    seen.add(url)
                    unique_urls.append(url)
            
            # Ordenar por relevância de domínio
            url_scores = []
            for url in unique_urls[:10]:  # Limitar a 10 para performance
                parsed = urlparse(url)
                domain = parsed.netloc.lower()
                if domain.startswith('www.'):
                    domain = domain[4:]
                
                relevance = self._calculate_domain_relevance(domain, org_name)
                
                # Bonus adicional para domínios que parecem oficiais
                if self._is_likely_official_domain(domain, org_name):
                    relevance *= 1.3
                    self.logger.debug(f"  🏢 Bonus para domínio oficial: {domain}")
                
                url_scores.append((url, relevance, domain))
            
            # Ordenar por relevância (maior primeiro), depois por simplicidade do domínio
            url_scores.sort(key=lambda x: (x[1], -len(x[2].split('.'))), reverse=True)
            
            # Testar URLs em ordem de relevância
            for url, score, domain in url_scores:
                self.logger.debug(f"Testando URL (relevância {score:.2f}): {url}")
                if self._is_valid_result(url, org_name):
                    self.logger.debug(f"URL válida encontrada: {url}")
                    return url
            
            self.logger.debug("Nenhuma URL válida encontrada no Bing")
            return None
            
        except Exception as e:
            self.logger.debug(f"Erro na busca Bing: {str(e)}")
            return None
    
    def _is_valid_result(self, url: str, org_name: str) -> bool:
        """
        Valida se um resultado de busca é relevante
        
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
            
            self.logger.debug(f"Validando URL: {url}")
            
            # Remover www.
            if domain.startswith('www.'):
                domain = domain[4:]
            
            # Filtrar domínios irrelevantes (mas permitir domínios principais de empresas conhecidas)
            main_corporate_domains = ['google.com', 'microsoft.com', 'apple.com', 'amazon.com']
            is_main_corporate = any(domain == corp_domain for corp_domain in main_corporate_domains)
            
            if not is_main_corporate and any(irrelevant in domain for irrelevant in self.irrelevant_domains):
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
            
            # Filtrar URLs que claramente não são sites oficiais
            bad_patterns = [
                # Padrões de busca
                '/search?', '/q=', '/query=', '/results?', '/find?',
                
                # Padrões de fóruns e Q&A
                '/questions/', '/question/', '/answers/', '/answer/',
                '/forum/', '/forums/', '/community/', '/discuss/',
                '/thread/', '/topic/', '/post/', '/posts/',
                '/ask/', '/help/', '/support/',
                
                # Padrões de conteúdo genérico
                '/blog/', '/news/', '/article/', '/articles/',
                '/review/', '/reviews/', '/rating/', '/ratings/',
                '/tag/', '/tags/', '/category/', '/categories/',
                
                # Padrões de sites específicos
                'stackoverflow.com/', 'stackexchange.com/', 'quora.com/',
                'reddit.com/', 'medium.com/', 'answers.yahoo.com/',
                
                # Padrões de tradução e cache
                'translate.google.com/', 'webcache.googleusercontent.com/',
                'archive.org/', 'web.archive.org/',
                
                # Padrões de diretórios
                'yellowpages.com/', 'whitepages.com/', 'yelp.com/',
                'zoominfo.com/', 'crunchbase.com/'
            ]
            
            if any(pattern in full_url for pattern in bad_patterns):
                self.logger.debug(f"URL rejeitada - padrão suspeito na URL: {full_url}")
                return False
            
            # Verificação específica para sites de fóruns e Q&A
            forum_indicators = [
                'forum', 'community', 'discuss', 'questions', 'answers',
                'stackoverflow', 'stackexchange', 'quora', 'reddit'
            ]
            
            if any(indicator in domain or indicator in full_url for indicator in forum_indicators):
                self.logger.debug(f"URL rejeitada - site de fórum/Q&A detectado: {full_url}")
                return False
            
            # NOVO: Validar relevância do domínio com o nome da organização
            relevance_score = self._calculate_domain_relevance(domain, org_name)
            self.logger.debug(f"Score de relevância do domínio: {relevance_score:.2f}")
            
            # Se o domínio é muito irrelevante, rejeitar mesmo que responda
            if relevance_score < 0.1:
                self.logger.debug(f"URL rejeitada - domínio irrelevante (score: {relevance_score:.2f})")
                return False
            
            # Para domínios com alta relevância, ser menos restritivo na validação de conectividade
            if relevance_score >= 0.7:
                # Domínios muito relevantes: apenas verificar se não são claramente inválidos
                self.logger.debug(f"URL com alta relevância - validação relaxada: {url}")
                return True
            
            # Para domínios com relevância média/baixa, validar conectividade
            if not self.validate_website_url(url):
                self.logger.debug(f"URL rejeitada - não responde: {url}")
                return False
            
            # Se chegou até aqui, é uma URL válida
            self.logger.debug(f"URL aceita: {url} (relevância: {relevance_score:.2f})")
            return True
            
        except Exception as e:
            self.logger.debug(f"Erro na validação de {url}: {str(e)}")
            return False
    
    def _calculate_domain_relevance(self, domain: str, org_name: str) -> float:
        """
        Calcula score de relevância entre domínio e nome da organização
        
        Args:
            domain: Domínio da URL (ex: coldiretti.it)
            org_name: Nome da organização (ex: Coldiretti)
            
        Returns:
            Score de 0.0 a 1.0 (1.0 = muito relevante)
        """
        if not domain or not org_name:
            return 0.0
        
        domain_clean = domain.lower().replace('-', ' ').replace('_', ' ')
        org_clean = org_name.lower().replace('-', ' ').replace('_', ' ')
        
        # Extrair palavras significativas (> 2 caracteres)
        org_words = [word for word in org_clean.split() if len(word) > 2]
        
        # Remover palavras comuns
        common_words = {
            'ltd', 'inc', 'corp', 'corporation', 'company', 'group', 'limited', 
            'co', 'llc', 'se', 'sa', 'ag', 'gmbh', 'the', 'and', 'of', 'for'
        }
        distinctive_words = [word for word in org_words if word not in common_words]
        words_to_check = distinctive_words if distinctive_words else org_words
        
        if not words_to_check:
            return 0.0
        
        # Separar domínio principal de subdomínios
        domain_parts = domain.split('.')
        main_domain_part = domain_parts[0] if domain_parts else domain
        
        # Contar matches no domínio completo
        matches = 0
        for word in words_to_check:
            if word in domain_clean:
                matches += 1
        
        # Calcular score base
        base_score = matches / len(words_to_check)
        
        # NOVO: Bonus significativo para domínios principais vs subdomínios
        if len(words_to_check) == 1:
            main_word = words_to_check[0]
            
            # Caso ideal: palavra exata no domínio principal (ex: coldiretti.it)
            if main_word == main_domain_part:
                base_score = 1.0
                self.logger.debug(f"  🎯 Match perfeito no domínio principal: {main_word} = {main_domain_part}")
            
            # Caso bom: palavra no domínio principal mas não exata (ex: coldiretti-news.it)
            elif main_word in main_domain_part:
                base_score = 0.9
                self.logger.debug(f"  ✅ Match no domínio principal: {main_word} em {main_domain_part}")
            
            # Caso subdomínio: palavra em subdomínio (ex: polo.coldiretti.it)
            elif any(main_word in part for part in domain_parts[1:]):
                base_score = 0.7  # Menor score para subdomínios
                self.logger.debug(f"  ⚠️ Match em subdomínio: {main_word} em {domain}")
        
        # Bonus para domínios mais simples (menos subdomínios)
        subdomain_count = len(domain_parts) - 2  # -2 para remover nome e TLD
        if subdomain_count == 0:
            base_score *= 1.2  # Bonus para domínio principal
        elif subdomain_count == 1:
            base_score *= 1.0  # Neutro para um subdomínio
        else:
            base_score *= 0.8  # Penalidade para múltiplos subdomínios
        
        # Penalty para domínios claramente irrelevantes
        irrelevant_indicators = [
            # Fóruns e discussões
            'forum', 'forums', 'community', 'discuss', 'discussion', 'board', 'boards',
            'qa', 'questions', 'answers', 'ask', 'help', 'support',
            
            # Conteúdo genérico
            'blog', 'news', 'article', 'tag', 'search', 'directory', 'wiki',
            'review', 'reviews', 'rating', 'ratings',
            
            # Sites específicos problemáticos
            'stackoverflow', 'stackexchange', 'quora', 'reddit', 'medium',
            'baidu', 'sina', 'qq', 'sogou', 'yandex',
            'uol.com', 'globo.com', 'tuacasa', 'espaco-zen',
            
            # Padrões de subdomínios suspeitos
            'translate', 'cache', 'webcache', 'archive', 'wayback',
            'jobs', 'careers', 'hiring', 'recruitment',
            'shop', 'store', 'buy', 'sell', 'marketplace',
            
            # Agregadores e diretórios
            'yellowpages', 'whitepages', 'yelp', 'foursquare', 'zoominfo',
            'crunchbase', 'bloomberg', 'reuters', 'associated-press'
        ]
        
        for indicator in irrelevant_indicators:
            if indicator in domain_clean:
                base_score *= 0.1  # Penalidade severa
                self.logger.debug(f"  ❌ Penalidade por indicador irrelevante: {indicator}")
                break
        
        # Penalty para subdomínios suspeitos
        suspicious_subdomains = [
            'www', 'blog', 'news', 'forum', 'forums', 'community', 'discuss',
            'shop', 'store', 'portal', 'polo', 'help', 'support', 'ask',
            'questions', 'answers', 'qa', 'wiki', 'docs', 'translate'
        ]
        if domain_parts and domain_parts[0] in suspicious_subdomains:
            base_score *= 0.8
            self.logger.debug(f"  ⚠️ Penalidade por subdomínio suspeito: {domain_parts[0]}")
        
        return min(base_score, 1.0)
    
    def _is_likely_official_domain(self, domain: str, org_name: str) -> bool:
        """
        Verifica se um domínio parece ser o site oficial da organização
        
        Args:
            domain: Domínio a verificar
            org_name: Nome da organização
            
        Returns:
            True se parece ser domínio oficial
        """
        if not domain or not org_name:
            return False
        
        domain_parts = domain.split('.')
        if len(domain_parts) < 2:
            return False
        
        main_part = domain_parts[0]
        org_clean = org_name.lower().replace(' ', '').replace('-', '').replace('_', '')
        
        # Casos que indicam domínio oficial:
        # 1. Nome da organização exato no domínio principal
        if org_clean in main_part or main_part in org_clean:
            # 2. TLD apropriado (.com, .org, .it, .de, etc.)
            tld = domain_parts[-1]
            official_tlds = ['com', 'org', 'net', 'gov', 'edu', 'it', 'de', 'fr', 'uk', 'co', 'io']
            if tld in official_tlds:
                # 3. Não é subdomínio suspeito
                if len(domain_parts) <= 3:  # dominio.com ou subdominio.dominio.com
                    return True
        
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
                timeout=5,  # Timeout mais curto para validação
                verify=False,
                allow_redirects=True
            )
            
            # Considerar códigos de sucesso e alguns erros que podem ser temporários
            success_codes = [200, 301, 302, 303, 307, 308, 403, 405]  # 403/405 podem ser sites válidos com restrições
            return response.status_code in success_codes
            
        except Exception:
            # Se HEAD falhar, tentar GET rápido
            try:
                response = requests.get(
                    url,
                    headers=self.headers,
                    timeout=3,  # Timeout ainda menor para GET
                    verify=False,
                    allow_redirects=True,
                    stream=True  # Não baixar o conteúdo completo
                )
                success_codes = [200, 301, 302, 303, 307, 308, 403, 405]
                return response.status_code in success_codes
            except Exception:
                # Se ambos falharem, assumir que pode ser um problema temporário
                # Para domínios que parecem legítimos, ser mais tolerante
                parsed = urlparse(url)
                domain = parsed.netloc.lower()
                if domain.startswith('www.'):
                    domain = domain[4:]
                
                # Domínios conhecidos que podem ter problemas temporários
                known_domains = [
                    'microsoft.com', 'apple.com', 'google.com', 'amazon.com',
                    'facebook.com', 'twitter.com', 'linkedin.com', 'github.com',
                    'coldiretti.it', 'allianz.com', 'bmw.com', 'volkswagen.com'
                ]
                
                if any(known in domain for known in known_domains):
                    self.logger.debug(f"Domínio conhecido com problema temporário: {domain}")
                    return True
                
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