#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MASTER ORCHESTRATOR - Sistema completo de classificação de seguros COP29

Este é o script principal que executa TODO o processo de ponta a ponta:

1. Carrega arquivo do config.yaml (data/cop29.xlsx)
2. Merge dos dados → processed/merged_data.csv
3. Normalização → processed/merged_data_normalized.csv  
4. Cria tabela de organizações → results/organizations.csv
5. Pipeline de classificação (com cache) → atualiza results/organizations.csv
6. Dataset final de pessoas → results/people.csv
7. (Streamlit será implementado depois)

ESTE É O SCRIPT QUE O USUÁRIO VAI RODAR PARA PROCESSAR TUDO!
"""

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import yaml

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent))

from utils.logger_config import setup_logger
from utils.config_manager import config_manager
from core.data_processor import DataProcessor
from core.org_normalizer import OrganizationNormalizer
from pipeline.main_processor import MainProcessor
from core.cache_manager import CacheManager


class MasterOrchestrator:
    """
    Orquestrador principal que executa todo o processo de classificação
    
    Este é o "maestro" que coordena todas as etapas do processo,
    garantindo que tudo funcione como um sistema integrado.
    """
    
    def __init__(self):
        """Inicializa o orquestrador principal"""
        self.logger, _ = setup_logger("master_orchestrator", log_to_file=True)
        
        # Carregar configurações
        self.data_config = config_manager.get_data_config()
        
        # Inicializar componentes
        self.data_processor = DataProcessor()
        self.normalizer = OrganizationNormalizer()
        self.main_processor = MainProcessor()
        self.cache_manager = CacheManager()
        
        # Caminhos dos arquivos (seguindo o processo definido)
        self.paths = {
            'input_excel': self.data_config['excel_file'],
            'merged_data': 'data/processed/merged_data.csv',
            'normalized_data': 'data/processed/merged_data_normalized.csv',
            'organizations': 'data/results/organizations.csv',
            'people': 'data/results/people.csv'
        }
        
        # Criar diretórios necessários
        for path_key, file_path in self.paths.items():
            if path_key != 'input_excel':  # Não criar diretório do arquivo de entrada
                Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        
        self.logger.info("🎯 Master Orchestrator inicializado")
        self.logger.info(f"📂 Arquivo de entrada: {self.paths['input_excel']}")
    
    def run_complete_process(self, max_organizations: Optional[int] = None) -> Dict[str, str]:
        """
        Executa o processo completo de classificação
        
        Args:
            max_organizations: Limite de organizações para processar (para testes)
            
        Returns:
            Dicionário com caminhos dos arquivos gerados
        """
        self.logger.info("🚀 INICIANDO PROCESSO COMPLETO DE CLASSIFICAÇÃO")
        self.logger.info("=" * 60)
        
        start_time = datetime.now()
        results = {}
        
        try:
            # ETAPA 1: Carregar arquivo Excel
            self.logger.info("📊 ETAPA 1: Carregando arquivo Excel")
            self.logger.info("-" * 40)
            
            if not Path(self.paths['input_excel']).exists():
                raise FileNotFoundError(f"Arquivo não encontrado: {self.paths['input_excel']}")
            
            self.logger.info(f"✅ Arquivo encontrado: {self.paths['input_excel']}")
            
            # ETAPA 2: Merge dos dados
            self.logger.info("\n🔄 ETAPA 2: Fazendo merge dos dados")
            self.logger.info("-" * 40)
            
            merged_df = self.data_processor.process_excel_file(self.paths['input_excel'])
            merged_df.to_csv(self.paths['merged_data'], index=False)
            results['merged_data'] = self.paths['merged_data']
            
            self.logger.info(f"✅ Dados merged salvos: {self.paths['merged_data']}")
            self.logger.info(f"📊 Total de registros: {len(merged_df)}")
            
            # ETAPA 3: Normalização
            self.logger.info("\n🧹 ETAPA 3: Normalizando dados")
            self.logger.info("-" * 40)
            
            # Executar normalização
            normalized_df, _ = self.normalizer.process_normalization(self.paths['merged_data'])
            normalized_df.to_csv(self.paths['normalized_data'], index=False)
            results['normalized_data'] = self.paths['normalized_data']
            
            unique_orgs = normalized_df['Home organization'].nunique()
            self.logger.info(f"✅ Dados normalizados salvos: {self.paths['normalized_data']}")
            self.logger.info(f"🏢 Organizações únicas: {unique_orgs}")
            
            # ETAPA 4: Criar tabela de organizações
            self.logger.info("\n📋 ETAPA 4: Criando tabela de organizações")
            self.logger.info("-" * 40)
            
            organizations_df = self._create_organizations_table(normalized_df)
            
            # Aplicar limite se especificado (para testes)
            if max_organizations:
                organizations_df = organizations_df.head(max_organizations)
                self.logger.info(f"⚠️ Limitando a {max_organizations} organizações para teste")
            
            organizations_df.to_csv(self.paths['organizations'], index=False)
            results['organizations'] = self.paths['organizations']
            
            self.logger.info(f"✅ Tabela de organizações criada: {self.paths['organizations']}")
            self.logger.info(f"📊 Organizações para processar: {len(organizations_df)}")
            
            # ETAPA 5: Pipeline de classificação
            self.logger.info("\n🤖 ETAPA 5: Executando pipeline de classificação")
            self.logger.info("-" * 40)
            
            self._run_classification_pipeline(organizations_df)
            
            self.logger.info(f"✅ Pipeline de classificação concluída")
            
            # ETAPA 6: Criar dataset final de pessoas
            self.logger.info("\n👥 ETAPA 6: Criando dataset final de pessoas")
            self.logger.info("-" * 40)
            
            people_df = self._create_people_dataset(normalized_df)
            people_df.to_csv(self.paths['people'], index=False)
            results['people'] = self.paths['people']
            
            self.logger.info(f"✅ Dataset de pessoas criado: {self.paths['people']}")
            
            # Estatísticas finais
            self._log_final_statistics(results)
            
            total_time = (datetime.now() - start_time).total_seconds()
            self.logger.info(f"\n🎉 PROCESSO COMPLETO CONCLUÍDO EM {total_time:.1f}s!")
            
            return results
            
        except Exception as e:
            self.logger.error(f"💥 ERRO NO PROCESSO: {str(e)}")
            raise
    
    def _create_organizations_table(self, normalized_df: pd.DataFrame) -> pd.DataFrame:
        """
        Cria tabela de organizações únicas para tracking da classificação
        
        Args:
            normalized_df: DataFrame normalizado
            
        Returns:
            DataFrame de organizações
        """
        # Extrair organizações únicas
        org_counts = normalized_df['Home organization'].value_counts()
        
        organizations_df = pd.DataFrame({
            'organization_name': org_counts.index,
            'participant_count': org_counts.values,
            'is_insurance': None,  # Será preenchido pela pipeline
            'website_url': None,
            'search_method': None,
            'content_source': None,
            'processing_status': 'pending',
            'error_message': None,
            'processed_at': None
        })
        
        self.logger.info(f"📋 Criadas {len(organizations_df)} organizações únicas")
        
        return organizations_df
    
    def _run_classification_pipeline(self, organizations_df: pd.DataFrame) -> None:
        """
        Executa pipeline de classificação para cada organização
        ATUALIZA o arquivo organizations.csv conforme o processo
        
        Args:
            organizations_df: DataFrame de organizações
        """
        total_orgs = len(organizations_df)
        processed_count = 0
        
        self.logger.info(f"🔄 Processando {total_orgs} organizações...")
        
        for idx, row in organizations_df.iterrows():
            org_name = row['organization_name']
            
            self.logger.info(f"\n[{idx+1:3d}/{total_orgs}] Processando: {org_name}")
            
            try:
                # Executar pipeline completo (com cache)
                result = self.main_processor.process_single_organization(org_name)
                
                # Atualizar tabela de organizações
                if result['success']:
                    organizations_df.at[idx, 'is_insurance'] = result['is_insurance']
                    organizations_df.at[idx, 'website_url'] = result['website_url']
                    organizations_df.at[idx, 'search_method'] = result['search_method']
                    organizations_df.at[idx, 'content_source'] = result.get('content_source_type')
                    organizations_df.at[idx, 'processing_status'] = 'completed'
                    organizations_df.at[idx, 'processed_at'] = datetime.now().isoformat()
                    
                    classification = "SEGURADORA" if result['is_insurance'] else "NÃO-SEGURADORA"
                    self.logger.info(f"  ✅ {classification}")
                    processed_count += 1
                else:
                    organizations_df.at[idx, 'processing_status'] = 'failed'
                    organizations_df.at[idx, 'error_message'] = result['error_message']
                    organizations_df.at[idx, 'processed_at'] = datetime.now().isoformat()
                    
                    self.logger.warning(f"  ❌ FALHA: {result['error_stage']}")
                
                # Salvar progresso a cada 10 organizações
                if (idx + 1) % 10 == 0:
                    organizations_df.to_csv(self.paths['organizations'], index=False)
                    self.logger.info(f"  💾 Progresso salvo ({idx + 1}/{total_orgs})")
                
            except Exception as e:
                self.logger.error(f"  💥 ERRO: {str(e)}")
                organizations_df.at[idx, 'processing_status'] = 'error'
                organizations_df.at[idx, 'error_message'] = str(e)
                organizations_df.at[idx, 'processed_at'] = datetime.now().isoformat()
        
        # Salvar resultado final
        organizations_df.to_csv(self.paths['organizations'], index=False)
        
        success_rate = (processed_count / total_orgs * 100) if total_orgs > 0 else 0
        self.logger.info(f"\n📊 Pipeline concluída: {processed_count}/{total_orgs} sucessos ({success_rate:.1f}%)")
    
    def _create_people_dataset(self, normalized_df: pd.DataFrame) -> pd.DataFrame:
        """
        Cria dataset final de pessoas com coluna is_insurance
        
        Args:
            normalized_df: DataFrame normalizado
            
        Returns:
            DataFrame final de pessoas
        """
        # Carregar tabela de organizações atualizada
        organizations_df = pd.read_csv(self.paths['organizations'])
        
        # Criar lookup de organizações
        org_lookup = {}
        for _, row in organizations_df.iterrows():
            org_name = row['organization_name']
            is_insurance = row['is_insurance']
            if pd.notna(is_insurance):
                org_lookup[org_name] = bool(is_insurance) if is_insurance != 'None' else None
            else:
                org_lookup[org_name] = None
        
        # Criar dataset de pessoas
        people_df = normalized_df.copy()
        people_df['is_insurance'] = None
        
        # Fazer matching
        matched_count = 0
        for idx, row in people_df.iterrows():
            org_name = row['Home organization']
            if org_name in org_lookup:
                people_df.at[idx, 'is_insurance'] = org_lookup[org_name]
                matched_count += 1
        
        classification_rate = (matched_count / len(people_df) * 100) if len(people_df) > 0 else 0
        self.logger.info(f"📊 Pessoas classificadas: {matched_count}/{len(people_df)} ({classification_rate:.1f}%)")
        
        return people_df
    
    def _log_final_statistics(self, results: Dict[str, str]) -> None:
        """
        Mostra estatísticas finais do processo
        
        Args:
            results: Dicionário com caminhos dos arquivos
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 ESTATÍSTICAS FINAIS")
        self.logger.info("=" * 60)
        
        try:
            # Estatísticas de organizações
            if 'organizations' in results:
                orgs_df = pd.read_csv(results['organizations'])
                total_orgs = len(orgs_df)
                completed = len(orgs_df[orgs_df['processing_status'] == 'completed'])
                insurance_orgs = len(orgs_df[orgs_df['is_insurance'] == True])
                
                self.logger.info(f"🏢 ORGANIZAÇÕES:")
                self.logger.info(f"   • Total: {total_orgs}")
                self.logger.info(f"   • Processadas: {completed}")
                self.logger.info(f"   • Seguradoras: {insurance_orgs}")
            
            # Estatísticas de pessoas
            if 'people' in results:
                people_df = pd.read_csv(results['people'])
                total_people = len(people_df)
                classified_people = len(people_df[people_df['is_insurance'].notna()])
                insurance_people = len(people_df[people_df['is_insurance'] == True])
                
                self.logger.info(f"👥 PESSOAS:")
                self.logger.info(f"   • Total: {total_people}")
                self.logger.info(f"   • Classificadas: {classified_people}")
                self.logger.info(f"   • De seguradoras: {insurance_people}")
            
            # Arquivos gerados
            self.logger.info(f"\n📁 ARQUIVOS GERADOS:")
            for key, path in results.items():
                self.logger.info(f"   • {key}: {path}")
                
        except Exception as e:
            self.logger.warning(f"⚠️ Erro ao calcular estatísticas: {str(e)}")


def main():
    """Função principal - ponto de entrada do sistema"""
    print("🎯 SISTEMA DE CLASSIFICAÇÃO DE SEGUROS COP29")
    print("=" * 50)
    print()
    
    orchestrator = MasterOrchestrator()
    
    try:
        # Executar processo completo
        results = orchestrator.run_complete_process(max_organizations=5)  # Limite para teste
        
        print("\n✅ PROCESSO CONCLUÍDO COM SUCESSO!")
        print("\n📁 Arquivos gerados:")
        for key, path in results.items():
            print(f"   • {key}: {path}")
        
    except Exception as e:
        print(f"\n💥 ERRO: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())