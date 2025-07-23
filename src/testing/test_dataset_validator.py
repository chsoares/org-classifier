#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test Dataset Validator - Sistema de teste e validação para classificação de seguros

Este módulo é responsável por:
1. Criar dataset de teste com organizações conhecidas
2. Extrair organizações aleatórias do dataset real
3. Executar pipeline completo de teste
4. Validar precisão da classificação
5. Gerar relatórios de performance
"""

import pandas as pd
import random
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import json
import sys

# Adicionar src ao path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger_config import setup_logger
from utils.config_manager import config_manager
from core.data_processor import DataProcessor
from scraping.web_searcher import WebSearcher
from scraping.org_web_extractor import OrganizationWebExtractor
from classification.insurance_classifier import InsuranceClassifier


class TestDatasetValidator:
    """
    Sistema de validação com dataset de teste
    """
    
    def __init__(self):
        self.logger, _ = setup_logger("test_validator", log_to_file=True)
        
        # Inicializar componentes do pipeline
        self.data_processor = DataProcessor()
        self.web_searcher = WebSearcher()
        self.web_extractor = OrganizationWebExtractor()
        self.classifier = InsuranceClassifier()
        
        self.logger.info("🧪 Test Dataset Validator inicializado")
        
        # Dataset de organizações conhecidas
        self.known_organizations = self._create_known_dataset()
        
        # Resultados dos testes
        self.test_results = []
        self.validation_stats = {}
    
    def _create_known_dataset(self) -> List[Dict]:
        """
        Cria dataset com organizações conhecidas (ground truth)
        
        Returns:
            Lista de organizações com classificação conhecida
        """
        known_dataset = [
            # INSURANCE COMPANIES (3 organizações)
            {
                'name': 'Allianz SE',
                'expected_classification': True,
                'category': 'Insurance',
                'description': 'German multinational insurance company, world\'s largest insurer'
            },
            {
                'name': 'Swiss Re',
                'expected_classification': True,
                'category': 'Reinsurance',
                'description': 'Swiss multinational reinsurance company'
            },
            {
                'name': 'Lloyd\'s of London',
                'expected_classification': True,
                'category': 'Insurance Market',
                'description': 'Insurance and reinsurance market in London'
            },
            
            # NON-INSURANCE COMPANIES (7 organizações)
            {
                'name': 'Microsoft Corporation',
                'expected_classification': False,
                'category': 'Technology',
                'description': 'American multinational technology corporation'
            },
            {
                'name': 'Harvard University',
                'expected_classification': False,
                'category': 'Education',
                'description': 'Private Ivy League research university'
            },
            {
                'name': 'Red Cross',
                'expected_classification': False,
                'category': 'Non-Profit',
                'description': 'International humanitarian movement'
            },
            {
                'name': 'JPMorgan Chase',
                'expected_classification': False,
                'category': 'Banking',
                'description': 'American multinational investment bank'
            },
            {
                'name': 'World Bank',
                'expected_classification': False,
                'category': 'International Organization',
                'description': 'International financial institution'
            },
            {
                'name': 'United Nations',
                'expected_classification': False,
                'category': 'International Organization',
                'description': 'International organization for global cooperation'
            },
            {
                'name': 'Coca-Cola Company',
                'expected_classification': False,
                'category': 'Consumer Goods',
                'description': 'American multinational beverage corporation'
            }
        ]
        
        self.logger.info(f"📋 Dataset conhecido criado: {len(known_dataset)} organizações")
        self.logger.info(f"   - Seguros: {sum(1 for org in known_dataset if org['expected_classification'])}")
        self.logger.info(f"   - Não-seguros: {sum(1 for org in known_dataset if not org['expected_classification'])}")
        
        return known_dataset
    
    def get_random_organizations_from_dataset(self, count: int = 10) -> List[Dict]:
        """
        Extrai organizações aleatórias do dataset real
        
        Args:
            count: Número de organizações para extrair
            
        Returns:
            Lista de organizações aleatórias
        """
        self.logger.info(f"🎲 Extraindo {count} organizações aleatórias do dataset real")
        
        try:
            # Carregar dados reais
            excel_data = self.data_processor.load_excel_data()
            
            if not excel_data:
                self.logger.error("❌ Falha ao carregar dados do Excel")
                return []
            
            # Combinar todas as organizações
            all_organizations = []
            
            for sheet_name, df in excel_data.items():
                if 'Home organization' in df.columns:
                    orgs = df['Home organization'].dropna().unique()
                    for org in orgs:
                        if isinstance(org, str) and len(org.strip()) > 3:
                            all_organizations.append({
                                'name': org.strip(),
                                'source_sheet': sheet_name,
                                'expected_classification': None,  # Desconhecido
                                'category': 'Real Dataset',
                                'description': f'Organization from {sheet_name} sheet'
                            })
            
            # Remover duplicatas
            unique_orgs = {}
            for org in all_organizations:
                name_key = org['name'].lower()
                if name_key not in unique_orgs:
                    unique_orgs[name_key] = org
            
            unique_list = list(unique_orgs.values())
            
            # Selecionar aleatoriamente
            if len(unique_list) < count:
                self.logger.warning(f"⚠️ Apenas {len(unique_list)} organizações disponíveis, menos que {count} solicitadas")
                selected = unique_list
            else:
                selected = random.sample(unique_list, count)
            
            self.logger.success(f"✅ {len(selected)} organizações aleatórias selecionadas")
            
            return selected
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao extrair organizações aleatórias: {str(e)}")
            return []
    
    def run_complete_pipeline_test(self, organization: Dict) -> Dict:
        """
        Executa pipeline completo para uma organização
        
        Args:
            organization: Dict com dados da organização
            
        Returns:
            Dict com resultados do teste
        """
        org_name = organization['name']
        self.logger.info(f"🔄 Executando pipeline completo para: {org_name}")
        
        start_time = datetime.now()
        
        result = {
            'organization': org_name,
            'expected_classification': organization.get('expected_classification'),
            'category': organization.get('category', 'Unknown'),
            'description': organization.get('description', ''),
            'start_time': start_time.isoformat(),
            'stages': {},
            'final_classification': None,
            'pipeline_success': False,
            'total_time': 0,
            'errors': []
        }
        
        try:
            # Stage 1: Web Search
            self.logger.debug(f"Stage 1: Buscando website para {org_name}")
            search_start = datetime.now()
            
            url, search_method = self.web_searcher.search_organization_website(org_name)
            
            search_time = (datetime.now() - search_start).total_seconds()
            
            result['stages']['web_search'] = {
                'success': url is not None,
                'url_found': url,
                'search_method': search_method,
                'time_seconds': search_time
            }
            
            if not url:
                result['errors'].append("Web search failed - no URL found")
                self.logger.warning(f"⚠️ Nenhuma URL encontrada para {org_name}")
                return result
            
            # Stage 2: Content Extraction
            self.logger.debug(f"Stage 2: Extraindo conteúdo de {url}")
            extraction_start = datetime.now()
            
            content_data = self.web_extractor.extract_organization_content(url, org_name)
            
            extraction_time = (datetime.now() - extraction_start).total_seconds()
            
            result['stages']['content_extraction'] = {
                'success': content_data is not None,
                'content_length': len(content_data.get('content', '')) if content_data else 0,
                'content_type': content_data.get('content_type') if content_data else None,
                'time_seconds': extraction_time
            }
            
            if not content_data or not content_data.get('content'):
                result['errors'].append("Content extraction failed - no content extracted")
                self.logger.warning(f"⚠️ Falha na extração de conteúdo para {org_name}")
                return result
            
            # Stage 3: AI Classification
            self.logger.debug(f"Stage 3: Classificando {org_name}")
            classification_start = datetime.now()
            
            classification = self.classifier.classify_organization(
                content_data['content'], 
                org_name
            )
            
            classification_time = (datetime.now() - classification_start).total_seconds()
            
            result['stages']['ai_classification'] = {
                'success': classification is not None,
                'classification': classification,
                'time_seconds': classification_time
            }
            
            if classification is None:
                result['errors'].append("AI classification failed")
                self.logger.warning(f"⚠️ Falha na classificação AI para {org_name}")
                return result
            
            # Pipeline Success
            result['final_classification'] = classification
            result['pipeline_success'] = True
            
            # Validar se classificação está correta (apenas para organizações conhecidas)
            if result['expected_classification'] is not None:
                result['classification_correct'] = (classification == result['expected_classification'])
            
            self.logger.success(f"✅ Pipeline completo para {org_name}: {'Insurance' if classification else 'Not Insurance'}")
            
        except Exception as e:
            result['errors'].append(f"Pipeline error: {str(e)}")
            self.logger.error(f"❌ Erro no pipeline para {org_name}: {str(e)}")
        
        finally:
            result['total_time'] = (datetime.now() - start_time).total_seconds()
            result['end_time'] = datetime.now().isoformat()
        
        return result
    
    def run_validation_test(self, include_random: bool = True, random_count: int = 10) -> Dict:
        """
        Executa teste de validação completo
        
        Args:
            include_random: Se deve incluir organizações aleatórias
            random_count: Número de organizações aleatórias
            
        Returns:
            Dict com resultados da validação
        """
        self.logger.info("🧪 Iniciando teste de validação completo")
        
        test_start_time = datetime.now()
        
        # Preparar dataset de teste
        test_organizations = self.known_organizations.copy()
        
        if include_random:
            random_orgs = self.get_random_organizations_from_dataset(random_count)
            test_organizations.extend(random_orgs)
        
        self.logger.info(f"📊 Dataset de teste: {len(test_organizations)} organizações")
        self.logger.info(f"   - Conhecidas: {len(self.known_organizations)}")
        if include_random:
            self.logger.info(f"   - Aleatórias: {len(random_orgs)}")
        
        # Executar testes
        results = []
        
        for i, org in enumerate(test_organizations, 1):
            self.logger.info(f"Processando {i}/{len(test_organizations)}: {org['name']}")
            
            result = self.run_complete_pipeline_test(org)
            results.append(result)
            
            # Log de progresso
            if i % 5 == 0:
                self.logger.info(f"Progresso: {i}/{len(test_organizations)} organizações processadas")
        
        # Calcular estatísticas
        validation_stats = self._calculate_validation_stats(results)
        
        # Salvar resultados
        self._save_test_results(results, validation_stats)
        
        total_time = (datetime.now() - test_start_time).total_seconds()
        
        self.logger.info(f"🎯 Teste de validação concluído em {total_time:.2f}s")
        self.logger.info(f"   - Pipeline success rate: {validation_stats['pipeline_success_rate']:.1f}%")
        self.logger.info(f"   - Classification accuracy: {validation_stats['classification_accuracy']:.1f}%")
        
        return {
            'results': results,
            'statistics': validation_stats,
            'total_time': total_time,
            'test_timestamp': test_start_time.isoformat()
        }
    
    def _calculate_validation_stats(self, results: List[Dict]) -> Dict:
        """
        Calcula estatísticas de validação
        
        Args:
            results: Lista de resultados dos testes
            
        Returns:
            Dict com estatísticas
        """
        total_tests = len(results)
        successful_pipelines = len([r for r in results if r['pipeline_success']])
        
        # Estatísticas por stage
        web_search_success = len([r for r in results if r['stages'].get('web_search', {}).get('success', False)])
        content_extraction_success = len([r for r in results if r['stages'].get('content_extraction', {}).get('success', False)])
        ai_classification_success = len([r for r in results if r['stages'].get('ai_classification', {}).get('success', False)])
        
        # Precisão da classificação (apenas para organizações conhecidas)
        known_results = [r for r in results if r['expected_classification'] is not None]
        correct_classifications = len([r for r in known_results if r.get('classification_correct', False)])
        
        # Distribuição de classificações
        insurance_classifications = len([r for r in results if r['final_classification'] is True])
        non_insurance_classifications = len([r for r in results if r['final_classification'] is False])
        
        # Tempos médios
        avg_total_time = sum(r['total_time'] for r in results) / total_tests if total_tests > 0 else 0
        avg_search_time = sum(r['stages'].get('web_search', {}).get('time_seconds', 0) for r in results) / total_tests if total_tests > 0 else 0
        avg_extraction_time = sum(r['stages'].get('content_extraction', {}).get('time_seconds', 0) for r in results) / total_tests if total_tests > 0 else 0
        avg_classification_time = sum(r['stages'].get('ai_classification', {}).get('time_seconds', 0) for r in results) / total_tests if total_tests > 0 else 0
        
        stats = {
            'total_tests': total_tests,
            'successful_pipelines': successful_pipelines,
            'pipeline_success_rate': (successful_pipelines / total_tests * 100) if total_tests > 0 else 0,
            
            'stage_success_rates': {
                'web_search': (web_search_success / total_tests * 100) if total_tests > 0 else 0,
                'content_extraction': (content_extraction_success / total_tests * 100) if total_tests > 0 else 0,
                'ai_classification': (ai_classification_success / total_tests * 100) if total_tests > 0 else 0
            },
            
            'classification_accuracy': (correct_classifications / len(known_results) * 100) if known_results else 0,
            'known_organizations_tested': len(known_results),
            'correct_classifications': correct_classifications,
            
            'classification_distribution': {
                'insurance': insurance_classifications,
                'non_insurance': non_insurance_classifications,
                'failed': total_tests - insurance_classifications - non_insurance_classifications
            },
            
            'average_times': {
                'total_pipeline': avg_total_time,
                'web_search': avg_search_time,
                'content_extraction': avg_extraction_time,
                'ai_classification': avg_classification_time
            }
        }
        
        return stats
    
    def _save_test_results(self, results: List[Dict], stats: Dict):
        """
        Salva resultados dos testes em arquivo
        
        Args:
            results: Lista de resultados
            stats: Estatísticas calculadas
        """
        try:
            # Criar diretório de resultados
            results_dir = Path("test_results")
            results_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Salvar resultados detalhados
            results_file = results_dir / f"validation_results_{timestamp}.json"
            with open(results_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'results': results,
                    'statistics': stats,
                    'timestamp': datetime.now().isoformat()
                }, f, indent=2, ensure_ascii=False)
            
            # Salvar relatório resumido
            report_file = results_dir / f"validation_report_{timestamp}.txt"
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("RELATÓRIO DE VALIDAÇÃO - INSURANCE CLASSIFIER\n")
                f.write("=" * 50 + "\n\n")
                
                f.write(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total de testes: {stats['total_tests']}\n")
                f.write(f"Pipelines bem-sucedidos: {stats['successful_pipelines']}\n")
                f.write(f"Taxa de sucesso: {stats['pipeline_success_rate']:.1f}%\n\n")
                
                f.write("TAXA DE SUCESSO POR STAGE:\n")
                f.write(f"- Web Search: {stats['stage_success_rates']['web_search']:.1f}%\n")
                f.write(f"- Content Extraction: {stats['stage_success_rates']['content_extraction']:.1f}%\n")
                f.write(f"- AI Classification: {stats['stage_success_rates']['ai_classification']:.1f}%\n\n")
                
                f.write("PRECISÃO DA CLASSIFICAÇÃO:\n")
                f.write(f"- Organizações conhecidas testadas: {stats['known_organizations_tested']}\n")
                f.write(f"- Classificações corretas: {stats['correct_classifications']}\n")
                f.write(f"- Precisão: {stats['classification_accuracy']:.1f}%\n\n")
                
                f.write("DISTRIBUIÇÃO DE CLASSIFICAÇÕES:\n")
                f.write(f"- Insurance: {stats['classification_distribution']['insurance']}\n")
                f.write(f"- Non-Insurance: {stats['classification_distribution']['non_insurance']}\n")
                f.write(f"- Failed: {stats['classification_distribution']['failed']}\n\n")
                
                f.write("TEMPOS MÉDIOS:\n")
                f.write(f"- Pipeline completo: {stats['average_times']['total_pipeline']:.2f}s\n")
                f.write(f"- Web Search: {stats['average_times']['web_search']:.2f}s\n")
                f.write(f"- Content Extraction: {stats['average_times']['content_extraction']:.2f}s\n")
                f.write(f"- AI Classification: {stats['average_times']['ai_classification']:.2f}s\n")
            
            self.logger.success(f"✅ Resultados salvos:")
            self.logger.info(f"   - Detalhados: {results_file}")
            self.logger.info(f"   - Relatório: {report_file}")
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao salvar resultados: {str(e)}")
    
    def generate_validation_report(self, results: Dict):
        """
        Gera relatório de validação formatado
        
        Args:
            results: Resultados da validação
        """
        stats = results['statistics']
        
        print("\n" + "=" * 60)
        print("🧪 RELATÓRIO DE VALIDAÇÃO - INSURANCE CLASSIFIER")
        print("=" * 60)
        
        print(f"\n📊 RESUMO GERAL:")
        print(f"   Total de testes: {stats['total_tests']}")
        print(f"   Pipelines bem-sucedidos: {stats['successful_pipelines']}")
        print(f"   Taxa de sucesso: {stats['pipeline_success_rate']:.1f}%")
        print(f"   Tempo total: {results['total_time']:.2f}s")
        
        print(f"\n🔄 TAXA DE SUCESSO POR STAGE:")
        print(f"   Web Search: {stats['stage_success_rates']['web_search']:.1f}%")
        print(f"   Content Extraction: {stats['stage_success_rates']['content_extraction']:.1f}%")
        print(f"   AI Classification: {stats['stage_success_rates']['ai_classification']:.1f}%")
        
        print(f"\n🎯 PRECISÃO DA CLASSIFICAÇÃO:")
        print(f"   Organizações conhecidas: {stats['known_organizations_tested']}")
        print(f"   Classificações corretas: {stats['correct_classifications']}")
        print(f"   Precisão: {stats['classification_accuracy']:.1f}%")
        
        print(f"\n📈 DISTRIBUIÇÃO DE CLASSIFICAÇÕES:")
        print(f"   Insurance: {stats['classification_distribution']['insurance']}")
        print(f"   Non-Insurance: {stats['classification_distribution']['non_insurance']}")
        print(f"   Failed: {stats['classification_distribution']['failed']}")
        
        print(f"\n⏱️ TEMPOS MÉDIOS:")
        print(f"   Pipeline completo: {stats['average_times']['total_pipeline']:.2f}s")
        print(f"   Web Search: {stats['average_times']['web_search']:.2f}s")
        print(f"   Content Extraction: {stats['average_times']['content_extraction']:.2f}s")
        print(f"   AI Classification: {stats['average_times']['ai_classification']:.2f}s")
        
        # Análise de qualidade
        print(f"\n🔍 ANÁLISE DE QUALIDADE:")
        if stats['classification_accuracy'] >= 90:
            print("   ✅ EXCELENTE - Precisão muito alta")
        elif stats['classification_accuracy'] >= 80:
            print("   ✅ BOM - Precisão aceitável")
        elif stats['classification_accuracy'] >= 70:
            print("   ⚠️ REGULAR - Precisa de ajustes")
        else:
            print("   ❌ RUIM - Requer revisão significativa")
        
        if stats['pipeline_success_rate'] >= 90:
            print("   ✅ Pipeline muito estável")
        elif stats['pipeline_success_rate'] >= 80:
            print("   ✅ Pipeline estável")
        else:
            print("   ⚠️ Pipeline precisa de melhorias")


def main():
    """Função principal para executar validação"""
    validator = TestDatasetValidator()
    
    print("🧪 INICIANDO TESTE DE VALIDAÇÃO")
    print("=" * 50)
    
    # Executar validação completa
    results = validator.run_validation_test(
        include_random=True,
        random_count=10
    )
    
    # Gerar relatório
    validator.generate_validation_report(results)
    
    print("\n✅ Validação concluída!")


if __name__ == "__main__":
    main()