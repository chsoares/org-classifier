#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Organization Web Extractor - Sistema de extração de conteúdo de organizações

Este módulo é responsável por:
1. Extrair conteúdo relevante de websites de organizações
2. Priorizar Wikipedia como fonte primária
3. Extrair seções "About" de websites próprios como fallback
4. Limpar e normalizar texto extraído
5. Limitar conteúdo a 2000 caracteres mantendo relevância
"""

import requests
import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import re
import time
from typing import Optional, Dict, List
import sys
from pathlib import Path

# Desabilitar avisos de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Adicionar src ao path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger_config import setup_logger
from utils.config_manager import config_manager


class OrganizationWebExtractor:
    """
    Sistema de extração de conteúdo web para organizações
    Adaptado para priorizar Wikipedia e extrair informações relevantes
    """
    
    def __init__(self):
        self.logger, _ = setup_logger("org_web_extractor", log_to_file=True)
        self.scraping_config = config_manager.get_scraping_config()
        
        self.logger.info("🌐 Inicializando Organization Web Extractor")
        
        # Configurações
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
        
        # Palavras-chave para identificar seções relevantes
        self.about_keywords = [
            # Inglês
            "about", "about us", "company", "who we are", "our company", 
            "overview", "corporate", "organization", "mission", "vision",
            "history", "background", "profile", "description",
            # Português
            "sobre", "quem somos", "empresa", "institucional", "nossa empresa",
            "missão", "visão", "história", "perfil", "descrição",
            # Francês
            "à propos", "qui sommes-nous", "entreprise", "société", "aperçu",
            # Alemão
            "über uns", "unternehmen", "wer wir sind", "firma", "überblick",
            # Espanhol
            "sobre nosotros", "empresa", "quiénes somos", "compañía"
        ]
        
        # Seletores CSS para conteúdo relevante
        self.content_selectors = [
            "main", "article", ".content", ".main-content", 
            ".about", ".company", ".overview", ".description",
            "#about", "#company", "#overview", "#main"
        ]
        
        self.logger.debug(f"Configurações: timeout={self.timeout}s, keywords={len(self.about_keywords)}")
    
    def extract_organization_content(self, url: str, org_name: str) -> Optional[Dict[str, str]]:
        """
        Extrai conteúdo relevante de uma organização
        
        Args:
            url: URL para extrair conteúdo
            org_name: Nome da organização (para validação)
            
        Returns:
            Dict com informações extraídas ou None
        """
        self.logger.info(f"🌐 Extraindo conteúdo para: {org_name}")
        self.logger.debug(f"URL: {url}")
        
        try:
            # Determinar tipo de fonte
            source_type = self._determine_source_type(url)
            
            # Fazer requisição HTTP
            response = self._make_request(url)
            if not response:
                self.logger.error(f"❌ Falha na requisição para {url}")
                return None
            
            # Parse do HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extrair conteúdo baseado no tipo de fonte
            if source_type == "wikipedia":
                content_data = self._extract_wikipedia_content(soup, org_name)
            else:
                content_data = self._extract_website_content(soup, org_name, url)
            
            if not content_data:
                self.logger.warning(f"⚠️ Nenhum conteúdo relevante extraído de {url}")
                return None
            
            # Adicionar metadados
            content_data.update({
                'source_url': url,
                'source_type': source_type,
                'extraction_timestamp': time.time()
            })
            
            self.logger.success(f"✨ Conteúdo extraído com sucesso: {len(content_data.get('content', ''))} caracteres")
            return content_data
            
        except Exception as e:
            self.logger.error(f"❌ Erro na extração de {url}: {str(e)}")
            return None
    
    def _determine_source_type(self, url: str) -> str:
        """
        Determina o tipo de fonte baseado na URL
        
        Args:
            url: URL para analisar
            
        Returns:
            Tipo da fonte: 'wikipedia', 'website'
        """
        if 'wikipedia.org' in url.lower():
            return 'wikipedia'
        else:
            return 'website'
    
    def _extract_wikipedia_content(self, soup: BeautifulSoup, org_name: str) -> Optional[Dict[str, str]]:
        """
        Extrai conteúdo específico da Wikipedia
        
        Args:
            soup: BeautifulSoup object da página
            org_name: Nome da organização
            
        Returns:
            Dict com conteúdo extraído
        """
        self.logger.debug(f"📚 Extraindo conteúdo da Wikipedia para: {org_name}")
        
        try:
            content_parts = []
            
            # 1. Extrair título da página
            title_elem = soup.find('h1', class_='firstHeading')
            if title_elem:
                title = title_elem.get_text().strip()
                content_parts.append(f"Title: {title}")
            
            # 2. Extrair primeiro parágrafo (resumo)
            first_paragraph = soup.find('div', class_='mw-parser-output')
            if first_paragraph:
                # Pegar os primeiros parágrafos antes da primeira seção
                paragraphs = first_paragraph.find_all('p', recursive=False)
                for p in paragraphs[:3]:  # Primeiros 3 parágrafos
                    text = self._clean_text(p.get_text())
                    if text and len(text) > 50:  # Apenas parágrafos substanciais
                        content_parts.append(text)
            
            # 3. Extrair infobox (caixa de informações)
            infobox = soup.find('table', class_='infobox')
            if infobox:
                infobox_data = self._extract_infobox_data(infobox)
                if infobox_data:
                    content_parts.append(f"Key Information: {infobox_data}")
            
            # 4. Extrair seções relevantes (History, Operations, etc.)
            relevant_sections = self._extract_wikipedia_sections(soup)
            content_parts.extend(relevant_sections)
            
            if not content_parts:
                return None
            
            # Juntar conteúdo e limitar tamanho
            full_content = " ".join(content_parts)
            limited_content = self._limit_content_length(full_content, 2000)
            
            return {
                'content': limited_content,
                'title': title if 'title' in locals() else org_name,
                'content_type': 'wikipedia_summary'
            }
            
        except Exception as e:
            self.logger.error(f"Erro na extração Wikipedia: {str(e)}")
            return None
    
    def _extract_website_content(self, soup: BeautifulSoup, org_name: str, url: str) -> Optional[Dict[str, str]]:
        """
        Extrai conteúdo de website próprio da organização
        
        Args:
            soup: BeautifulSoup object da página
            org_name: Nome da organização
            url: URL da página
            
        Returns:
            Dict com conteúdo extraído
        """
        self.logger.debug(f"🌐 Extraindo conteúdo do website para: {org_name}")
        
        try:
            content_parts = []
            
            # 1. Extrair título da página
            title = self._extract_page_title(soup)
            if title:
                content_parts.append(f"Title: {title}")
            
            # 2. Extrair conteúdo principal usando seletores
            main_content = self._extract_main_content(soup)
            if main_content:
                content_parts.append(main_content)
            
            # 3. Buscar e extrair seções "About"
            about_content = self._extract_about_sections(soup, url)
            if about_content:
                content_parts.extend(about_content)
            
            # 4. Extrair meta description
            meta_desc = self._extract_meta_description(soup)
            if meta_desc:
                content_parts.append(f"Description: {meta_desc}")
            
            if not content_parts:
                return None
            
            # Juntar conteúdo e limitar tamanho
            full_content = " ".join(content_parts)
            limited_content = self._limit_content_length(full_content, 2000)
            
            return {
                'content': limited_content,
                'title': title or org_name,
                'content_type': 'website_content'
            }
            
        except Exception as e:
            self.logger.error(f"Erro na extração website: {str(e)}")
            return None
    
    def _extract_infobox_data(self, infobox: BeautifulSoup) -> str:
        """
        Extrai dados relevantes do infobox da Wikipedia
        
        Args:
            infobox: Elemento infobox
            
        Returns:
            String com dados formatados
        """
        try:
            data_parts = []
            
            # Procurar por linhas de dados
            rows = infobox.find_all('tr')
            for row in rows:
                header = row.find('th')
                data = row.find('td')
                
                if header and data:
                    header_text = self._clean_text(header.get_text())
                    data_text = self._clean_text(data.get_text())
                    
                    # Filtrar informações relevantes
                    relevant_fields = [
                        'type', 'industry', 'founded', 'headquarters', 'founder',
                        'products', 'services', 'revenue', 'employees', 'website'
                    ]
                    
                    if any(field in header_text.lower() for field in relevant_fields):
                        if len(data_text) < 200:  # Evitar dados muito longos
                            data_parts.append(f"{header_text}: {data_text}")
            
            return "; ".join(data_parts[:5])  # Máximo 5 campos
            
        except Exception as e:
            self.logger.debug(f"Erro na extração infobox: {str(e)}")
            return ""
    
    def _extract_wikipedia_sections(self, soup: BeautifulSoup) -> List[str]:
        """
        Extrai seções relevantes da Wikipedia
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Lista de conteúdos de seções
        """
        sections = []
        
        try:
            # Procurar por headings de seções relevantes
            relevant_headings = ['history', 'operations', 'business', 'overview', 'activities']
            
            for heading in soup.find_all(['h2', 'h3']):
                heading_text = heading.get_text().lower()
                
                if any(keyword in heading_text for keyword in relevant_headings):
                    # Extrair conteúdo da seção
                    section_content = []
                    
                    # Pegar parágrafos seguintes até próximo heading
                    for sibling in heading.find_next_siblings():
                        if sibling.name in ['h2', 'h3']:
                            break
                        if sibling.name == 'p':
                            text = self._clean_text(sibling.get_text())
                            if text and len(text) > 30:
                                section_content.append(text)
                    
                    if section_content:
                        section_text = " ".join(section_content[:2])  # Máximo 2 parágrafos por seção
                        if len(section_text) < 500:  # Limitar tamanho da seção
                            sections.append(section_text)
            
        except Exception as e:
            self.logger.debug(f"Erro na extração de seções: {str(e)}")
        
        return sections[:3]  # Máximo 3 seções
    
    def _extract_page_title(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extrai título da página
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Título da página ou None
        """
        # Tentar diferentes seletores para título
        title_selectors = ['title', 'h1', '.page-title', '.main-title']
        
        for selector in title_selectors:
            element = soup.select_one(selector)
            if element:
                title = self._clean_text(element.get_text())
                if title and len(title) < 200:
                    return title
        
        return None
    
    def _extract_main_content(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extrai conteúdo principal da página
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Conteúdo principal ou None
        """
        content_parts = []
        
        # Tentar seletores de conteúdo principal
        for selector in self.content_selectors:
            elements = soup.select(selector)
            for element in elements:
                text = self._clean_text(element.get_text())
                if text and len(text) > 100:  # Apenas conteúdo substancial
                    content_parts.append(text)
                    break  # Pegar apenas o primeiro conteúdo relevante
            
            if content_parts:
                break
        
        # Se não encontrou conteúdo específico, pegar parágrafos principais
        if not content_parts:
            paragraphs = soup.find_all('p')
            for p in paragraphs[:5]:  # Primeiros 5 parágrafos
                text = self._clean_text(p.get_text())
                if text and len(text) > 50:
                    content_parts.append(text)
        
        return " ".join(content_parts[:3]) if content_parts else None  # Máximo 3 partes
    
    def _extract_about_sections(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """
        Busca e extrai seções "About" da página
        
        Args:
            soup: BeautifulSoup object
            base_url: URL base para resolver links relativos
            
        Returns:
            Lista de conteúdos de seções "About"
        """
        about_sections = []
        
        try:
            # 1. Procurar seções "About" na página atual
            for keyword in self.about_keywords:
                # Procurar por IDs
                element = soup.find(id=lambda x: x and keyword.lower() in x.lower())
                if element:
                    text = self._clean_text(element.get_text())
                    if text and len(text) > 50:
                        about_sections.append(text)
                        continue
                
                # Procurar por classes
                element = soup.find(class_=lambda x: x and keyword.lower() in ' '.join(x).lower())
                if element:
                    text = self._clean_text(element.get_text())
                    if text and len(text) > 50:
                        about_sections.append(text)
                        continue
            
            # 2. Procurar links para páginas "About" (limitado a 1 página)
            if not about_sections:
                about_link = self._find_about_link(soup, base_url)
                if about_link:
                    about_content = self._extract_about_page_content(about_link)
                    if about_content:
                        about_sections.append(about_content)
        
        except Exception as e:
            self.logger.debug(f"Erro na extração de seções About: {str(e)}")
        
        return about_sections[:2]  # Máximo 2 seções About
    
    def _find_about_link(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """
        Encontra link para página "About"
        
        Args:
            soup: BeautifulSoup object
            base_url: URL base
            
        Returns:
            URL da página About ou None
        """
        try:
            for link in soup.find_all('a', href=True):
                href = link.get('href', '').lower()
                link_text = link.get_text().lower()
                
                # Verificar se é link About
                is_about_link = any(
                    keyword in link_text or keyword.replace(' ', '-') in href
                    for keyword in ['about', 'about us', 'company', 'who we are']
                )
                
                if is_about_link and href != '#':
                    # Resolver URL relativa
                    if href.startswith('/'):
                        parsed_base = urlparse(base_url)
                        about_url = f"{parsed_base.scheme}://{parsed_base.netloc}{href}"
                    elif href.startswith('http'):
                        about_url = href
                    else:
                        continue
                    
                    return about_url
        
        except Exception as e:
            self.logger.debug(f"Erro na busca de link About: {str(e)}")
        
        return None
    
    def _extract_about_page_content(self, about_url: str) -> Optional[str]:
        """
        Extrai conteúdo de página About específica
        
        Args:
            about_url: URL da página About
            
        Returns:
            Conteúdo da página About ou None
        """
        try:
            response = self._make_request(about_url)
            if not response:
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extrair conteúdo principal da página About
            main_content = self._extract_main_content(soup)
            if main_content and len(main_content) > 100:
                return main_content[:800]  # Limitar conteúdo About
            
        except Exception as e:
            self.logger.debug(f"Erro na extração página About: {str(e)}")
        
        return None
    
    def _extract_meta_description(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extrai meta description da página
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Meta description ou None
        """
        try:
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc and meta_desc.get('content'):
                desc = self._clean_text(meta_desc.get('content'))
                if desc and len(desc) > 20:
                    return desc
        except Exception:
            pass
        
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
    
    def _clean_text(self, text: str) -> str:
        """
        Limpa e normaliza texto extraído
        
        Args:
            text: Texto para limpar
            
        Returns:
            Texto limpo
        """
        if not text:
            return ""
        
        # Remover quebras de linha e espaços extras
        text = " ".join(text.split())
        
        # Remover caracteres especiais mantendo pontuação básica
        text = re.sub(r'[^\w\s\-.,;:?!()\[\]{}"\']', ' ', text)
        
        # Remover múltiplos espaços
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def _limit_content_length(self, content: str, max_length: int = 2000) -> str:
        """
        Limita o tamanho do conteúdo mantendo as partes mais relevantes
        
        Args:
            content: Conteúdo para limitar
            max_length: Tamanho máximo em caracteres
            
        Returns:
            Conteúdo limitado
        """
        if len(content) <= max_length:
            return content
        
        # Dividir em sentenças
        sentences = content.split('.')
        
        # Priorizar sentenças com palavras-chave relevantes
        relevant_sentences = []
        other_sentences = []
        
        keywords = ['company', 'organization', 'business', 'industry', 'founded', 'headquarters']
        
        for sentence in sentences:
            sentence = sentence.strip()
            if sentence:
                if any(keyword in sentence.lower() for keyword in keywords):
                    relevant_sentences.append(sentence)
                else:
                    other_sentences.append(sentence)
        
        # Construir conteúdo limitado
        result = []
        current_length = 0
        
        # Adicionar sentenças relevantes primeiro
        for sentence in relevant_sentences:
            if current_length + len(sentence) + 1 <= max_length:
                result.append(sentence)
                current_length += len(sentence) + 1
            else:
                break
        
        # Adicionar outras sentenças se houver espaço
        for sentence in other_sentences:
            if current_length + len(sentence) + 1 <= max_length:
                result.append(sentence)
                current_length += len(sentence) + 1
            else:
                break
        
        return '. '.join(result) + '.' if result else content[:max_length]
    
    def validate_content_relevance(self, content: str, org_name: str) -> bool:
        """
        Valida se o conteúdo extraído é relevante para a organização
        
        Args:
            content: Conteúdo extraído
            org_name: Nome da organização
            
        Returns:
            True se o conteúdo é relevante
        """
        if not content or len(content) < 50:
            return False
        
        content_lower = content.lower()
        org_words = org_name.lower().split()
        
        # Verificar se pelo menos uma palavra da organização aparece no conteúdo
        org_match = any(word in content_lower for word in org_words if len(word) > 3)
        
        # Verificar se contém palavras-chave organizacionais
        org_keywords = [
            'company', 'organization', 'corporation', 'business', 'firm',
            'enterprise', 'group', 'association', 'foundation', 'institute'
        ]
        keyword_match = any(keyword in content_lower for keyword in org_keywords)
        
        return org_match or keyword_match


def main():
    """Função para testar o extractor"""
    extractor = OrganizationWebExtractor()
    
    # Testar com URLs de exemplo
    test_cases = [
        ("https://en.wikipedia.org/wiki/Microsoft", "Microsoft Corporation"),
        ("https://www.microsoft.com", "Microsoft Corporation"),
    ]
    
    print(f"\n🧪 Testando Organization Web Extractor:")
    
    for url, org_name in test_cases:
        print(f"\n🌐 Testando: {org_name}")
        print(f"URL: {url}")
        
        result = extractor.extract_organization_content(url, org_name)
        
        if result:
            print(f"✅ Sucesso!")
            print(f"  Tipo: {result.get('content_type')}")
            print(f"  Título: {result.get('title')}")
            print(f"  Tamanho: {len(result.get('content', ''))} caracteres")
            print(f"  Conteúdo (primeiros 200 chars): {result.get('content', '')[:200]}...")
        else:
            print(f"❌ Falha na extração")


if __name__ == "__main__":
    main()