#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Main Processing Pipeline - Coordena todo o processo de classificação

Este é o "cérebro" do sistema que:
1. Pega cada organização da lista
2. Busca o website dela
3. Extrai o conteúdo do site
4. Classifica se é seguradora usando IA
5. Salva o resultado
6. Trata erros sem parar o processo todo

É como uma linha de produção automatizada!
"""

import sys
import pandas as pd
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import traceback

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger_config import setup_logger
from utils.config_manager import config_manager
from scraping.web_searcher import WebSearcher
from scraping.org_web_extractor import OrganizationWebExtractor
from classification.insurance_classifier import InsuranceClassifier
from core.cache_manager import CacheManager


class MainProcessor:
    """
    Pipeline principal que coordena todo o processo de classificação
    
    Pensa nele como um gerente de fábrica que:
    - Coordena todas as etapas
    - Controla a qualidade
    - Trata problemas sem parar a produção
    - Mantém registro de tudo
    """
    
    def __init__(self):
        """Inicializa o pipeline com todos os componentes necessários"""
        self.logger, _ = setup_logger("main_processor", log_to_file=True)
        
        self.logger.info("🚀 Inicializando Pipeline Principal")
        
        # Inicializar componentes do pipeline
        self.web_searcher = WebSearcher()
        self.web_extractor = OrganizationWebExtractor()
        self.classifier = InsuranceClassifier()
        self.cache_manager = CacheManager()
        
        # Estatísticas do processamento
        self.stats = {
            'total_processed': 0,
            'successful_classifications': 0,
            'failed_web_search': 0,
            'failed_content_extraction': 0,
            'failed_classification': 0,
            'errors': []
        }
        
        self.logger.info("✅ Pipeline inicializado com sucesso")
    
    def process_single_organization(self, org_name: str, org_id: Optional[str] = None) -> Dict:
        """
        Processa uma única organização através de todo o pipeline
        
        Esta é a função mais importante! Ela pega uma organização e:
        1. Busca o website
        2. Extrai conteúdo 
        3. Classifica com IA
        4. Retorna resultado completo
        
        Args:
            org_name: Nome da organização (ex: "Allianz SE")
            org_id: ID opcional para tracking
            
        Returns:
            Dict com todos os resultados do processamento
        """
        start_time = datetime.now()
        
        # Estrutura do resultado - sempre retornamos isso
        result = {
            'organization_name': org_name,
            'organization_id': org_id,
            'processing_start': start_time,
            'processing_end': None,
            'total_time_seconds': 0,
            'success': False,
            'is_insurance': None,
            'website_url': None,
            'website_content': None,
            'content_source_type': None,
            'content_title': None,
            'search_method': None,
            'error_stage': None,
            'error_message': None,
            'stages': {
                'web_search': {'success': False, 'time_seconds': 0, 'error': None},
                'content_extraction': {'success': False, 'time_seconds': 0, 'error': None},
                'classification': {'success': False, 'time_seconds': 0, 'error': None}
            }
        }
        
        self.logger.info(f"🔍 Processando organização: {org_name}")
        
        # Verificar se já temos resultado completo no cache
        cached_result = self.cache_manager.load_from_cache('full_results', org_name)
        if cached_result:
            self.logger.info(f"📦 Resultado encontrado no cache para {org_name}")
            # Atualizar timestamps e estatísticas
            cached_result['processing_start'] = start_time
            cached_result['processing_end'] = datetime.now()
            cached_result['total_time_seconds'] = 0.1  # Tempo mínimo para cache
            self.stats['total_processed'] += 1
            if cached_result.get('success'):
                self.stats['successful_classifications'] += 1
            return cached_result
        
        try:
            # ETAPA 1: BUSCAR WEBSITE
            self.logger.debug(f"Etapa 1: Buscando website para {org_name}")
            stage_start = datetime.now()
            
            # Verificar cache de busca
            cached_search = self.cache_manager.load_from_cache('web_search', org_name)
            if cached_search:
                website_url = cached_search.get('website_url')
                search_method = cached_search.get('search_method')
                self.logger.debug(f"📦 Busca carregada do cache: {website_url}")
            else:
                website_url, search_method = self.web_searcher.search_organization_website(org_name)
                
                # Salvar no cache se encontrou
                if website_url:
                    search_cache_data = {
                        'website_url': website_url,
                        'search_method': search_method
                    }
                    self.cache_manager.save_to_cache('web_search', org_name, search_cache_data)
            
            stage_time = (datetime.now() - stage_start).total_seconds()
            result['stages']['web_search']['time_seconds'] = stage_time
            
            if not website_url:
                # Falha na busca de website
                error_msg = f"Não foi possível encontrar website para {org_name}"
                self.logger.warning(f"⚠️ {error_msg}")
                
                result['error_stage'] = 'web_search'
                result['error_message'] = error_msg
                result['stages']['web_search']['error'] = error_msg
                self.stats['failed_web_search'] += 1
                return self._finalize_result(result)
            
            # Sucesso na busca
            result['website_url'] = website_url
            result['search_method'] = search_method
            result['stages']['web_search']['success'] = True
            
            self.logger.info(f"✅ Website encontrado: {website_url} (via {search_method})")
            
            # ETAPA 2: EXTRAIR CONTEÚDO
            self.logger.debug(f"Etapa 2: Extraindo conteúdo de {website_url}")
            stage_start = datetime.now()
            
            # Verificar cache de extração
            cached_content = self.cache_manager.load_from_cache('content_extraction', org_name)
            if cached_content:
                content_data = cached_content
                self.logger.debug(f"📦 Conteúdo carregado do cache")
            else:
                content_data = self.web_extractor.extract_organization_content(website_url, org_name)
                
                # Salvar no cache se extraiu
                if content_data:
                    self.cache_manager.save_to_cache('content_extraction', org_name, content_data)
            
            stage_time = (datetime.now() - stage_start).total_seconds()
            result['stages']['content_extraction']['time_seconds'] = stage_time
            
            # Extrair texto do dicionário retornado
            if not content_data or not isinstance(content_data, dict):
                content_text = None
            else:
                content_text = content_data.get('content', '')
            
            if not content_text or len(content_text.strip()) < 50:
                # Falha na extração de conteúdo
                error_msg = f"Não foi possível extrair conteúdo relevante de {website_url}"
                self.logger.warning(f"⚠️ {error_msg}")
                
                result['error_stage'] = 'content_extraction'
                result['error_message'] = error_msg
                result['stages']['content_extraction']['error'] = error_msg
                self.stats['failed_content_extraction'] += 1
                return self._finalize_result(result)
            
            # Sucesso na extração
            result['website_content'] = content_text
            result['stages']['content_extraction']['success'] = True
            
            # Adicionar informações extras do extrator
            result['content_source_type'] = content_data.get('source_type', 'unknown')
            result['content_title'] = content_data.get('title', org_name)
            
            self.logger.info(f"✅ Conteúdo extraído: {len(content_text)} caracteres (fonte: {result['content_source_type']})")
            
            # ETAPA 3: CLASSIFICAR COM IA
            self.logger.debug(f"Etapa 3: Classificando {org_name} com IA")
            stage_start = datetime.now()
            
            # Verificar cache de classificação
            cached_classification = self.cache_manager.load_from_cache('classification', org_name)
            if cached_classification:
                is_insurance = cached_classification.get('is_insurance')
                self.logger.debug(f"📦 Classificação carregada do cache: {is_insurance}")
            else:
                is_insurance = self.classifier.classify_organization(content_text, org_name)
                
                # Salvar no cache
                classification_cache_data = {
                    'is_insurance': is_insurance,
                    'content_preview': content_text[:200] + "..." if len(content_text) > 200 else content_text
                }
                self.cache_manager.save_to_cache('classification', org_name, classification_cache_data)
            
            stage_time = (datetime.now() - stage_start).total_seconds()
            result['stages']['classification']['time_seconds'] = stage_time
            
            if is_insurance is None:
                # Falha na classificação
                error_msg = f"Falha na classificação por IA para {org_name}"
                self.logger.warning(f"⚠️ {error_msg}")
                
                result['error_stage'] = 'classification'
                result['error_message'] = error_msg
                result['stages']['classification']['error'] = error_msg
                self.stats['failed_classification'] += 1
                return self._finalize_result(result)
            
            # Sucesso na classificação
            result['is_insurance'] = is_insurance
            result['stages']['classification']['success'] = True
            result['success'] = True
            
            classification_text = "SIM" if is_insurance else "NÃO"
            self.logger.info(f"✅ Classificação: {classification_text}")
            
            self.stats['successful_classifications'] += 1
            
        except Exception as e:
            # Erro inesperado - não queremos que pare todo o processo
            error_msg = f"Erro inesperado ao processar {org_name}: {str(e)}"
            self.logger.error(f"💥 {error_msg}")
            self.logger.debug(f"Stack trace: {traceback.format_exc()}")
            
            result['error_stage'] = 'unexpected_error'
            result['error_message'] = error_msg
            
            self.stats['errors'].append({
                'organization': org_name,
                'error': error_msg,
                'timestamp': datetime.now()
            })
        
        final_result = self._finalize_result(result)
        
        # Salvar resultado completo no cache (apenas se foi processado, não carregado do cache)
        if 'cached_result' not in locals() or not cached_result:
            self.cache_manager.save_to_cache('full_results', org_name, final_result)
        
        return final_result
    
    def _finalize_result(self, result: Dict) -> Dict:
        """
        Finaliza o resultado calculando tempo total e atualizando estatísticas
        
        Args:
            result: Dicionário com resultado do processamento
            
        Returns:
            Resultado finalizado
        """
        result['processing_end'] = datetime.now()
        result['total_time_seconds'] = (
            result['processing_end'] - result['processing_start']
        ).total_seconds()
        
        self.stats['total_processed'] += 1
        
        return result
    
    def process_organization_list(self, organizations: List[str], 
                                max_organizations: Optional[int] = None) -> List[Dict]:
        """
        Processa uma lista de organizações
        
        Esta função é como um gerente que coordena o processamento de várias organizações:
        - Processa uma por vez
        - Não para se uma falhar
        - Mostra progresso
        - Coleta estatísticas
        
        Args:
            organizations: Lista de nomes de organizações
            max_organizations: Limite máximo para processar (para testes)
            
        Returns:
            Lista com resultados de todas as organizações
        """
        if max_organizations:
            organizations = organizations[:max_organizations]
        
        total_orgs = len(organizations)
        self.logger.info(f"🎯 Iniciando processamento de {total_orgs} organizações")
        
        results = []
        
        for i, org_name in enumerate(organizations, 1):
            self.logger.info(f"\n[{i:3d}/{total_orgs}] Processando: {org_name}")
            self.logger.info("-" * 60)
            
            try:
                result = self.process_single_organization(org_name, str(i))
                results.append(result)
                
                # Log do resultado
                if result['success']:
                    classification = "SEGURADORA" if result['is_insurance'] else "NÃO-SEGURADORA"
                    self.logger.info(f"✅ {org_name} → {classification}")
                else:
                    self.logger.warning(f"❌ {org_name} → FALHA ({result['error_stage']})")
                
            except Exception as e:
                # Erro crítico - logar mas continuar
                self.logger.error(f"💥 Erro crítico ao processar {org_name}: {str(e)}")
                continue
            
            # Pequena pausa para não sobrecarregar APIs
            if i < total_orgs:
                time.sleep(1)
        
        self._log_final_statistics(results)
        return results
    
    def _log_final_statistics(self, results: List[Dict]):
        """
        Mostra estatísticas finais do processamento
        
        Args:
            results: Lista com todos os resultados
        """
        total = len(results)
        successful = len([r for r in results if r['success']])
        failed = total - successful
        
        if total > 0:
            success_rate = (successful / total) * 100
        else:
            success_rate = 0
        
        # Contar classificações
        insurance_count = len([r for r in results if r.get('is_insurance') is True])
        non_insurance_count = len([r for r in results if r.get('is_insurance') is False])
        
        # Contar falhas por etapa
        web_search_failures = len([r for r in results if r.get('error_stage') == 'web_search'])
        content_failures = len([r for r in results if r.get('error_stage') == 'content_extraction'])
        classification_failures = len([r for r in results if r.get('error_stage') == 'classification'])
        
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 ESTATÍSTICAS FINAIS DO PROCESSAMENTO")
        self.logger.info("=" * 60)
        self.logger.info(f"Total processado: {total}")
        self.logger.info(f"Sucessos: {successful} ({success_rate:.1f}%)")
        self.logger.info(f"Falhas: {failed} ({100-success_rate:.1f}%)")
        self.logger.info("")
        self.logger.info("🏢 CLASSIFICAÇÕES:")
        self.logger.info(f"  Seguradoras: {insurance_count}")
        self.logger.info(f"  Não-seguradoras: {non_insurance_count}")
        self.logger.info("")
        self.logger.info("❌ FALHAS POR ETAPA:")
        self.logger.info(f"  Busca de website: {web_search_failures}")
        self.logger.info(f"  Extração de conteúdo: {content_failures}")
        self.logger.info(f"  Classificação IA: {classification_failures}")
        self.logger.info("=" * 60)
    
    def get_processing_statistics(self) -> Dict:
        """
        Retorna estatísticas atuais do processamento
        
        Returns:
            Dicionário com estatísticas
        """
        return self.stats.copy()
    
    def get_cache_statistics(self) -> Dict:
        """
        Retorna estatísticas do cache
        
        Returns:
            Dicionário com estatísticas do cache
        """
        return self.cache_manager.get_cache_statistics()
    
    def clear_cache(self, cache_type: Optional[str] = None, org_name: Optional[str] = None) -> int:
        """
        Limpa cache
        
        Args:
            cache_type: Tipo específico para limpar (None = todos)
            org_name: Organização específica (None = todas)
            
        Returns:
            Número de arquivos removidos
        """
        return self.cache_manager.clear_cache(cache_type, org_name)
    
    def list_cached_organizations(self, cache_type: Optional[str] = None) -> List[str]:
        """
        Lista organizações que estão no cache
        
        Args:
            cache_type: Tipo específico (None = todos os tipos)
            
        Returns:
            Lista de nomes de organizações
        """
        return self.cache_manager.list_cached_organizations(cache_type)


def main():
    """Função para testar o pipeline principal"""
    processor = MainProcessor()
    
    # Testar com algumas organizações conhecidas
    test_organizations = [
        "Allianz SE",
        "Swiss Re", 
        "Microsoft Corporation",
        "Harvard University",
        "Lloyd's of London"
    ]
    
    print("🧪 Testando Pipeline Principal")
    print("=" * 50)
    
    results = processor.process_organization_list(test_organizations)
    
    print(f"\n📋 Resultados detalhados:")
    for result in results:
        org = result['organization_name']
        if result['success']:
            classification = "SEGURADORA" if result['is_insurance'] else "NÃO-SEGURADORA"
            confidence = result['confidence']
            time_taken = result['total_time_seconds']
            print(f"  ✅ {org}: {classification} (confiança: {confidence:.2f}, tempo: {time_taken:.1f}s)")
        else:
            error_stage = result['error_stage']
            print(f"  ❌ {org}: FALHA na etapa '{error_stage}'")


if __name__ == "__main__":
    main()