#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Progress Tracker - Sistema de tracking do progresso de classificação de organizações

Este módulo é responsável por:
1. Criar DataFrame de tracking para organizações únicas
2. Acompanhar cada etapa do processo (website, scraping, classificação)
3. Registrar erros e timestamps
4. Gerar relatórios de progresso
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any
import sys
from datetime import datetime
import json

# Adicionar src ao path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger_config import setup_logger
from utils.config_manager import config_manager


class ProgressTracker:
    """
    Sistema de tracking do progresso de classificação de organizações
    """
    
    def __init__(self):
        self.logger, _ = setup_logger("progress_tracker", log_to_file=True)
        
        self.logger.info("📊 Inicializando Progress Tracker")
        
        # Caminhos dos arquivos
        self.tracking_file = Path("data/processed/organizations_tracking.csv")
        self.progress_file = Path("data/processed/progress_report.json")
    
    def create_tracking_dataframe(self, org_list: List[str]) -> pd.DataFrame:
        """
        Cria DataFrame de tracking para lista de organizações
        
        Args:
            org_list: Lista de nomes de organizações únicas
            
        Returns:
            DataFrame de tracking inicializado
        """
        self.logger.info(f"📋 Criando DataFrame de tracking para {len(org_list)} organizações")
        
        # Estrutura do DataFrame de tracking
        tracking_data = {
            # Identificação da organização
            'home_organization': org_list,
            'normalized_name': org_list,  # Será atualizado se houver normalização
            'occurrence_count': [0] * len(org_list),  # Será preenchido depois
            
            # Etapa 1: Busca de website
            'website_found': [None] * len(org_list),  # bool: True/False/None
            'website_url': [None] * len(org_list),    # str: URL encontrada ou None
            'website_search_method': [None] * len(org_list),  # str: 'google', 'duckduckgo', 'bing', 'failed'
            'website_search_timestamp': [None] * len(org_list),  # datetime
            'website_search_error': [None] * len(org_list),  # str: mensagem de erro
            
            # Etapa 2: Scraping de conteúdo
            'scraping_success': [None] * len(org_list),  # bool: True/False/None
            'content_length': [None] * len(org_list),    # int: tamanho do conteúdo
            'scraping_timestamp': [None] * len(org_list),  # datetime
            'scraping_error': [None] * len(org_list),    # str: mensagem de erro
            
            # Etapa 3: Classificação por IA
            'classification_success': [None] * len(org_list),  # bool: True/False/None
            'classification_result': [None] * len(org_list),   # str: 'Yes', 'No', ou None
            'classification_timestamp': [None] * len(org_list),  # datetime
            'classification_error': [None] * len(org_list),     # str: mensagem de erro
            
            # Resultado final
            'is_insurance': [None] * len(org_list),      # bool: True/False/None
            'process_status': ['pending'] * len(org_list),  # str: status do processo
            'processing_duration': [None] * len(org_list),  # float: tempo total em segundos
            'last_updated': [datetime.now()] * len(org_list),  # datetime: última atualização
            
            # Metadados
            'retry_count': [0] * len(org_list),          # int: número de tentativas
            'priority': ['normal'] * len(org_list),     # str: 'high', 'normal', 'low'
            'notes': [None] * len(org_list)             # str: observações adicionais
        }
        
        tracking_df = pd.DataFrame(tracking_data)
        
        self.logger.success(f"✨ DataFrame de tracking criado com {len(tracking_df)} organizações")
        self.logger.info(f"📊 Colunas de tracking: {len(tracking_df.columns)}")
        
        return tracking_df
    
    def load_or_create_tracking(self, organizations_file: str = "data/processed/organizations_mapping.csv") -> pd.DataFrame:
        """
        Carrega tracking existente ou cria novo baseado no arquivo de organizações
        
        Args:
            organizations_file: Arquivo com mapeamento de organizações
            
        Returns:
            DataFrame de tracking
        """
        self.logger.info("🔄 Carregando ou criando sistema de tracking...")
        
        # Verificar se já existe tracking
        if self.tracking_file.exists():
            self.logger.info(f"📂 Carregando tracking existente: {self.tracking_file}")
            try:
                tracking_df = pd.read_csv(self.tracking_file)
                
                # Converter colunas de data
                date_columns = ['website_search_timestamp', 'scraping_timestamp', 
                              'classification_timestamp', 'last_updated']
                for col in date_columns:
                    if col in tracking_df.columns:
                        tracking_df[col] = pd.to_datetime(tracking_df[col])
                
                self.logger.success(f"✨ Tracking carregado: {len(tracking_df)} organizações")
                return tracking_df
                
            except Exception as e:
                self.logger.warning(f"⚠️ Erro ao carregar tracking existente: {e}")
                self.logger.info("🔄 Criando novo tracking...")
        
        # Carregar organizações do arquivo de mapeamento
        orgs_path = Path(organizations_file)
        if not orgs_path.exists():
            raise FileNotFoundError(f"Arquivo de organizações não encontrado: {orgs_path}")
        
        self.logger.info(f"📂 Carregando organizações de: {orgs_path}")
        orgs_df = pd.read_csv(orgs_path)
        
        # Usar nomes normalizados únicos
        unique_orgs = orgs_df['normalized_name'].unique().tolist()
        
        # Criar novo tracking
        tracking_df = self.create_tracking_dataframe(unique_orgs)
        
        # Preencher informações das organizações
        org_info = orgs_df.groupby('normalized_name').agg({
            'original_name': 'first',  # Pegar o primeiro nome original
            'occurrence_count': 'sum'  # Somar todas as ocorrências
        }).reset_index()
        
        # Atualizar tracking com informações das organizações
        tracking_df = tracking_df.merge(
            org_info[['normalized_name', 'occurrence_count']], 
            on='normalized_name', 
            how='left',
            suffixes=('', '_new')
        )
        tracking_df['occurrence_count'] = tracking_df['occurrence_count_new'].fillna(0)
        tracking_df.drop('occurrence_count_new', axis=1, inplace=True)
        
        # Salvar tracking inicial
        self.save_tracking(tracking_df)
        
        return tracking_df
    
    def update_organization_status(self, tracking_df: pd.DataFrame, org_name: str, 
                                 status_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Atualiza status de uma organização específica
        
        Args:
            tracking_df: DataFrame de tracking
            org_name: Nome da organização
            status_data: Dicionário com dados de status
            
        Returns:
            DataFrame atualizado
        """
        # Encontrar índice da organização
        mask = tracking_df['normalized_name'] == org_name
        
        if not mask.any():
            self.logger.warning(f"⚠️ Organização não encontrada no tracking: {org_name}")
            return tracking_df
        
        idx = tracking_df[mask].index[0]
        
        # Atualizar campos fornecidos
        for field, value in status_data.items():
            if field in tracking_df.columns:
                tracking_df.at[idx, field] = value
        
        # Sempre atualizar timestamp
        tracking_df.at[idx, 'last_updated'] = datetime.now()
        
        # Atualizar status do processo baseado no progresso
        current_status = self._determine_process_status(tracking_df.iloc[idx])
        tracking_df.at[idx, 'process_status'] = current_status
        
        self.logger.debug(f"📝 Status atualizado para '{org_name}': {current_status}")
        
        return tracking_df
    
    def _determine_process_status(self, row: pd.Series) -> str:
        """
        Determina o status do processo baseado no progresso atual
        
        Args:
            row: Linha do DataFrame de tracking
            
        Returns:
            Status do processo
        """
        # Se classificação foi bem-sucedida
        if row['classification_success'] is True:
            return 'completed'
        
        # Se classificação falhou
        if row['classification_success'] is False:
            return 'classification_failed'
        
        # Se scraping falhou
        if row['scraping_success'] is False:
            return 'scraping_failed'
        
        # Se website não foi encontrado
        if row['website_found'] is False:
            return 'website_not_found'
        
        # Se website foi encontrado mas scraping ainda não foi tentado
        if row['website_found'] is True and row['scraping_success'] is None:
            return 'website_found'
        
        # Se scraping foi bem-sucedido mas classificação ainda não foi tentada
        if row['scraping_success'] is True and row['classification_success'] is None:
            return 'content_extracted'
        
        # Status padrão
        return 'pending'
    
    def get_processing_statistics(self, tracking_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Gera estatísticas de processamento
        
        Args:
            tracking_df: DataFrame de tracking
            
        Returns:
            Dicionário com estatísticas
        """
        total_orgs = len(tracking_df)
        
        # Contar por status
        status_counts = tracking_df['process_status'].value_counts().to_dict()
        
        # Estatísticas por etapa
        website_stats = {
            'found': (tracking_df['website_found'] == True).sum(),
            'not_found': (tracking_df['website_found'] == False).sum(),
            'pending': tracking_df['website_found'].isnull().sum()
        }
        
        scraping_stats = {
            'success': (tracking_df['scraping_success'] == True).sum(),
            'failed': (tracking_df['scraping_success'] == False).sum(),
            'pending': tracking_df['scraping_success'].isnull().sum()
        }
        
        classification_stats = {
            'success': (tracking_df['classification_success'] == True).sum(),
            'failed': (tracking_df['classification_success'] == False).sum(),
            'pending': tracking_df['classification_success'].isnull().sum()
        }
        
        # Resultados finais
        insurance_results = {
            'insurance': (tracking_df['is_insurance'] == True).sum(),
            'not_insurance': (tracking_df['is_insurance'] == False).sum(),
            'unknown': tracking_df['is_insurance'].isnull().sum()
        }
        
        # Calcular progresso geral
        completed = status_counts.get('completed', 0)
        progress_percentage = (completed / total_orgs) * 100 if total_orgs > 0 else 0
        
        stats = {
            'timestamp': datetime.now().isoformat(),
            'total_organizations': total_orgs,
            'progress_percentage': round(progress_percentage, 1),
            'status_distribution': status_counts,
            'website_search': website_stats,
            'content_scraping': scraping_stats,
            'classification': classification_stats,
            'final_results': insurance_results,
            'top_errors': self._get_top_errors(tracking_df)
        }
        
        return stats
    
    def _get_top_errors(self, tracking_df: pd.DataFrame, top_n: int = 5) -> Dict[str, int]:
        """
        Obtém os erros mais comuns
        
        Args:
            tracking_df: DataFrame de tracking
            top_n: Número de erros principais
            
        Returns:
            Dicionário com erros mais comuns
        """
        all_errors = []
        
        # Coletar erros de todas as etapas
        error_columns = ['website_search_error', 'scraping_error', 'classification_error']
        
        for col in error_columns:
            if col in tracking_df.columns:
                errors = tracking_df[col].dropna().tolist()
                all_errors.extend(errors)
        
        if not all_errors:
            return {}
        
        # Contar erros
        error_counts = pd.Series(all_errors).value_counts().head(top_n)
        
        return error_counts.to_dict()
    
    def save_tracking(self, tracking_df: pd.DataFrame) -> None:
        """
        Salva DataFrame de tracking
        
        Args:
            tracking_df: DataFrame de tracking
        """
        try:
            # Criar diretório se não existir
            self.tracking_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Salvar CSV
            tracking_df.to_csv(self.tracking_file, index=False, encoding='utf-8')
            
            self.logger.debug(f"💾 Tracking salvo: {self.tracking_file}")
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao salvar tracking: {e}")
            raise
    
    def save_progress_report(self, tracking_df: pd.DataFrame) -> None:
        """
        Salva relatório de progresso em JSON
        
        Args:
            tracking_df: DataFrame de tracking
        """
        try:
            stats = self.get_processing_statistics(tracking_df)
            
            # Criar diretório se não existir
            self.progress_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Salvar JSON
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"📊 Relatório de progresso salvo: {self.progress_file}")
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao salvar relatório: {e}")
            raise
    
    def export_tracking_data(self, tracking_df: pd.DataFrame, file_path: str) -> None:
        """
        Exporta dados de tracking para arquivo específico
        
        Args:
            tracking_df: DataFrame de tracking
            file_path: Caminho do arquivo de destino
        """
        try:
            export_path = Path(file_path)
            export_path.parent.mkdir(parents=True, exist_ok=True)
            
            if export_path.suffix.lower() == '.json':
                # Exportar como JSON
                tracking_dict = tracking_df.to_dict('records')
                with open(export_path, 'w', encoding='utf-8') as f:
                    json.dump(tracking_dict, f, indent=2, ensure_ascii=False, default=str)
            else:
                # Exportar como CSV
                tracking_df.to_csv(export_path, index=False, encoding='utf-8')
            
            self.logger.info(f"📤 Dados exportados para: {export_path}")
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao exportar dados: {e}")
            raise
    
    def generate_summary_report(self, tracking_df: pd.DataFrame) -> str:
        """
        Gera relatório resumido em texto
        
        Args:
            tracking_df: DataFrame de tracking
            
        Returns:
            Relatório em formato texto
        """
        stats = self.get_processing_statistics(tracking_df)
        
        report = f"""
📊 RELATÓRIO DE PROGRESSO - ORGANIZATION INSURANCE CLASSIFIER
═══════════════════════════════════════════════════════════

🕐 Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}

📈 PROGRESSO GERAL
├─ Total de organizações: {stats['total_organizations']:,}
├─ Progresso: {stats['progress_percentage']:.1f}%
└─ Concluídas: {stats['status_distribution'].get('completed', 0):,}

🔍 BUSCA DE WEBSITES
├─ Encontrados: {stats['website_search']['found']:,}
├─ Não encontrados: {stats['website_search']['not_found']:,}
└─ Pendentes: {stats['website_search']['pending']:,}

🌐 EXTRAÇÃO DE CONTEÚDO
├─ Sucessos: {stats['content_scraping']['success']:,}
├─ Falhas: {stats['content_scraping']['failed']:,}
└─ Pendentes: {stats['content_scraping']['pending']:,}

🤖 CLASSIFICAÇÃO POR IA
├─ Sucessos: {stats['classification']['success']:,}
├─ Falhas: {stats['classification']['failed']:,}
└─ Pendentes: {stats['classification']['pending']:,}

🏢 RESULTADOS FINAIS
├─ Organizações de seguros: {stats['final_results']['insurance']:,}
├─ Não relacionadas a seguros: {stats['final_results']['not_insurance']:,}
└─ Não classificadas: {stats['final_results']['unknown']:,}

📋 STATUS DETALHADO
"""
        
        for status, count in stats['status_distribution'].items():
            report += f"├─ {status.replace('_', ' ').title()}: {count:,}\n"
        
        if stats['top_errors']:
            report += f"\n❌ PRINCIPAIS ERROS\n"
            for error, count in stats['top_errors'].items():
                report += f"├─ {error[:50]}{'...' if len(error) > 50 else ''}: {count}\n"
        
        return report


def main():
    """Função para testar o progress tracker"""
    tracker = ProgressTracker()
    
    try:
        # Carregar ou criar tracking
        tracking_df = tracker.load_or_create_tracking()
        
        print(f"\n📊 Sistema de tracking inicializado:")
        print(f"Organizações: {len(tracking_df)}")
        print(f"Colunas: {len(tracking_df.columns)}")
        
        # Gerar estatísticas
        stats = tracker.get_processing_statistics(tracking_df)
        print(f"\nProgresso atual: {stats['progress_percentage']:.1f}%")
        print(f"Status: {stats['status_distribution']}")
        
        # Salvar relatório
        tracker.save_progress_report(tracking_df)
        
        # Gerar relatório resumido
        report = tracker.generate_summary_report(tracking_df)
        print(report)
        
    except Exception as e:
        print(f"Erro: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()