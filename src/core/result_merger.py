#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Result Merger - Sistema de fusão de resultados de classificação

Este módulo é responsável por:
1. Carregar dataset original de participantes
2. Carregar resultados de classificação de seguros
3. Fazer matching usando nomes normalizados de organizações
4. Adicionar coluna 'is_insurance' ao dataset original
5. Exportar resultado final em múltiplos formatos

É como um "casamenteiro" que conecta os dados originais com os resultados da IA!
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import sys

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger_config import setup_logger
from core.cache_manager import CacheManager


class ResultMerger:
    """
    Sistema de fusão de resultados de classificação com dataset original
    
    Pensa nele como um "casamenteiro" que:
    - Pega a lista original de participantes
    - Pega os resultados da classificação IA
    - Conecta os dois usando nomes de organizações
    - Cria dataset final com informação de seguros
    """
    
    def __init__(self, cache_manager: Optional[CacheManager] = None):
        """
        Inicializa o merger de resultados
        
        Args:
            cache_manager: Gerenciador de cache (opcional)
        """
        self.logger, _ = setup_logger("result_merger", log_to_file=True)
        
        self.cache_manager = cache_manager or CacheManager()
        
        # Estatísticas do merge
        self.merge_stats = {
            'total_participants': 0,
            'total_organizations': 0,
            'organizations_classified': 0,
            'organizations_not_classified': 0,
            'insurance_organizations': 0,
            'non_insurance_organizations': 0,
            'classification_rate': 0.0
        }
        
        self.logger.info("🔗 Result Merger inicializado")
    
    def load_original_dataset(self, file_path: str) -> pd.DataFrame:
        """
        Carrega o dataset original de participantes
        
        Args:
            file_path: Caminho para o arquivo (CSV ou Excel)
            
        Returns:
            DataFrame com dados originais
        """
        self.logger.info(f"📂 Carregando dataset original: {file_path}")
        
        try:
            file_path = Path(file_path)
            
            if not file_path.exists():
                raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
            
            # Detectar formato do arquivo
            if file_path.suffix.lower() == '.csv':
                df = pd.read_csv(file_path)
            elif file_path.suffix.lower() in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            else:
                raise ValueError(f"Formato de arquivo não suportado: {file_path.suffix}")
            
            self.logger.info(f"✅ Dataset carregado: {len(df)} linhas, {len(df.columns)} colunas")
            self.logger.debug(f"Colunas: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao carregar dataset: {str(e)}")
            raise
    
    def load_classification_results(self) -> Dict[str, Dict[str, Any]]:
        """
        Carrega resultados de classificação do cache
        
        Returns:
            Dicionário com resultados por organização
        """
        self.logger.info("📦 Carregando resultados de classificação do cache")
        
        results = {}
        
        try:
            # Listar organizações que têm resultados completos
            cached_orgs = self.cache_manager.list_cached_organizations('full_results')
            
            self.logger.info(f"📋 Encontradas {len(cached_orgs)} organizações com resultados")
            
            for org_name in cached_orgs:
                try:
                    # Carregar resultado completo
                    result_data = self.cache_manager.load_from_cache('full_results', org_name)
                    
                    if result_data and result_data.get('success'):
                        results[org_name] = {
                            'is_insurance': result_data.get('is_insurance'),
                            'website_url': result_data.get('website_url'),
                            'content_source_type': result_data.get('content_source_type'),
                            'search_method': result_data.get('search_method'),
                            'processing_time': result_data.get('total_time_seconds', 0),
                            'success': True
                        }
                        
                        self.logger.debug(f"✅ {org_name}: {'SEGURADORA' if result_data.get('is_insurance') else 'NÃO-SEGURADORA'}")
                    else:
                        # Resultado com falha
                        results[org_name] = {
                            'is_insurance': None,
                            'website_url': None,
                            'content_source_type': None,
                            'search_method': None,
                            'processing_time': 0,
                            'success': False,
                            'error': result_data.get('error_message', 'Unknown error') if result_data else 'No data'
                        }
                        
                        self.logger.debug(f"❌ {org_name}: FALHA")
                
                except Exception as e:
                    self.logger.warning(f"⚠️ Erro ao carregar resultado para {org_name}: {str(e)}")
                    continue
            
            self.logger.info(f"✅ Carregados resultados para {len(results)} organizações")
            
            # Estatísticas dos resultados
            successful = len([r for r in results.values() if r['success']])
            insurance = len([r for r in results.values() if r.get('is_insurance') is True])
            non_insurance = len([r for r in results.values() if r.get('is_insurance') is False])
            
            self.logger.info(f"📊 Resultados: {successful} sucessos, {insurance} seguradoras, {non_insurance} não-seguradoras")
            
            return results
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao carregar resultados de classificação: {str(e)}")
            return {}
    
    def merge_results(self, processed_df: pd.DataFrame, 
                     classification_results: Dict[str, Dict[str, Any]],
                     org_column: str = 'Home organization') -> pd.DataFrame:
        """
        Faz merge dos resultados de classificação com dataset processado e normalizado
        
        Args:
            processed_df: DataFrame processado e normalizado (merged_data_normalized.csv)
            classification_results: Resultados de classificação
            org_column: Nome da coluna com organizações (deve ser 'Home organization')
            
        Returns:
            DataFrame com resultados merged
        """
        self.logger.info("🔗 Fazendo merge dos resultados com dataset processado")
        
        try:
            # Criar cópia do DataFrame processado
            merged_df = processed_df.copy()
            
            # Simplificar colunas de organização se necessário
            if 'Home organization_normalized' in merged_df.columns and 'Home organization' in merged_df.columns:
                # Substituir coluna original pela normalizada
                merged_df['Home organization'] = merged_df['Home organization_normalized']
                merged_df.drop('Home organization_normalized', axis=1, inplace=True)
                self.logger.info("✅ Coluna 'Home organization' atualizada com valores normalizados")
            
            # Verificar se coluna de organização existe
            if org_column not in merged_df.columns:
                available_cols = [col for col in merged_df.columns if 'organization' in col.lower()]
                if available_cols:
                    org_column = available_cols[0]
                    self.logger.warning(f"⚠️ Coluna '{org_column}' não encontrada, usando '{org_column}'")
                else:
                    raise ValueError(f"Coluna de organização não encontrada. Colunas disponíveis: {list(merged_df.columns)}")
            
            # Inicializar novas colunas
            merged_df['is_insurance'] = None
            merged_df['insurance_classification_success'] = False
            merged_df['website_url'] = None
            merged_df['content_source'] = None
            merged_df['search_method'] = None
            merged_df['processing_time_seconds'] = None
            merged_df['classification_error'] = None
            
            # Estatísticas
            total_rows = len(merged_df)
            unique_orgs = merged_df[org_column].nunique()
            matched_orgs = 0
            insurance_count = 0
            non_insurance_count = 0
            
            self.logger.info(f"📊 Dataset processado: {total_rows} participantes, {unique_orgs} organizações únicas")
            
            # Fazer matching linha por linha
            for idx, row in merged_df.iterrows():
                org_name = row[org_column]
                
                if pd.isna(org_name) or not org_name:
                    continue
                
                # Limpar nome da organização
                org_name_clean = str(org_name).strip()
                
                # Procurar resultado de classificação
                if org_name_clean in classification_results:
                    result = classification_results[org_name_clean]
                    
                    merged_df.at[idx, 'insurance_classification_success'] = result['success']
                    
                    if result['success']:
                        merged_df.at[idx, 'is_insurance'] = result['is_insurance']
                        merged_df.at[idx, 'website_url'] = result['website_url']
                        merged_df.at[idx, 'content_source'] = result['content_source_type']
                        merged_df.at[idx, 'search_method'] = result['search_method']
                        merged_df.at[idx, 'processing_time_seconds'] = result['processing_time']
                        
                        if result['is_insurance'] is True:
                            insurance_count += 1
                        elif result['is_insurance'] is False:
                            non_insurance_count += 1
                        
                        matched_orgs += 1
                    else:
                        merged_df.at[idx, 'classification_error'] = result.get('error', 'Unknown error')
            
            # Calcular estatísticas finais
            classification_rate = (matched_orgs / unique_orgs * 100) if unique_orgs > 0 else 0
            
            self.merge_stats = {
                'total_participants': total_rows,
                'total_organizations': unique_orgs,
                'organizations_classified': matched_orgs,
                'organizations_not_classified': unique_orgs - matched_orgs,
                'insurance_organizations': insurance_count,
                'non_insurance_organizations': non_insurance_count,
                'classification_rate': classification_rate
            }
            
            self.logger.info("✅ Merge concluído com sucesso!")
            self.logger.info(f"📊 Estatísticas do merge:")
            self.logger.info(f"   • Total de participantes: {total_rows}")
            self.logger.info(f"   • Organizações únicas: {unique_orgs}")
            self.logger.info(f"   • Organizações classificadas: {matched_orgs} ({classification_rate:.1f}%)")
            self.logger.info(f"   • Seguradoras identificadas: {insurance_count}")
            self.logger.info(f"   • Não-seguradoras identificadas: {non_insurance_count}")
            
            return merged_df
            
        except Exception as e:
            self.logger.error(f"❌ Erro no merge: {str(e)}")
            raise
    
    def export_results(self, merged_df: pd.DataFrame, 
                      output_dir: str = "data/results",
                      base_filename: str = None) -> Dict[str, str]:
        """
        Exporta resultados em múltiplos formatos
        
        Args:
            merged_df: DataFrame com resultados merged
            output_dir: Diretório de saída
            base_filename: Nome base dos arquivos (None = auto-gerar)
            
        Returns:
            Dicionário com caminhos dos arquivos gerados
        """
        self.logger.info("💾 Exportando resultados finais")
        
        try:
            # Criar diretório de saída
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Gerar nome base se não fornecido
            if not base_filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                base_filename = f"cop29_insurance_classification_{timestamp}"
            
            exported_files = {}
            
            # Exportar CSV
            csv_path = output_path / f"{base_filename}.csv"
            merged_df.to_csv(csv_path, index=False, encoding='utf-8')
            exported_files['csv'] = str(csv_path)
            self.logger.info(f"✅ CSV exportado: {csv_path}")
            
            # Exportar Excel
            excel_path = output_path / f"{base_filename}.xlsx"
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # Aba principal com todos os dados
                merged_df.to_excel(writer, sheet_name='All_Participants', index=False)
                
                # Aba apenas com seguradoras
                insurance_df = merged_df[merged_df['is_insurance'] == True]
                if not insurance_df.empty:
                    insurance_df.to_excel(writer, sheet_name='Insurance_Companies', index=False)
                
                # Aba com estatísticas
                stats_df = pd.DataFrame([
                    ['Total Participants', self.merge_stats['total_participants']],
                    ['Unique Organizations', self.merge_stats['total_organizations']],
                    ['Organizations Classified', self.merge_stats['organizations_classified']],
                    ['Classification Rate (%)', f"{self.merge_stats['classification_rate']:.1f}%"],
                    ['Insurance Companies', self.merge_stats['insurance_organizations']],
                    ['Non-Insurance Companies', self.merge_stats['non_insurance_organizations']],
                    ['Export Date', datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
                ], columns=['Metric', 'Value'])
                
                stats_df.to_excel(writer, sheet_name='Statistics', index=False)
            
            exported_files['excel'] = str(excel_path)
            self.logger.info(f"✅ Excel exportado: {excel_path}")
            
            # Exportar resumo em texto
            summary_path = output_path / f"{base_filename}_summary.txt"
            with open(summary_path, 'w', encoding='utf-8') as f:
                f.write("COP29 Insurance Classification Results\n")
                f.write("=" * 40 + "\n\n")
                f.write(f"Export Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write("STATISTICS:\n")
                f.write("-" * 20 + "\n")
                for key, value in self.merge_stats.items():
                    if key == 'classification_rate':
                        f.write(f"{key.replace('_', ' ').title()}: {value:.1f}%\n")
                    else:
                        f.write(f"{key.replace('_', ' ').title()}: {value}\n")
                
                f.write(f"\nFILES GENERATED:\n")
                f.write("-" * 20 + "\n")
                for file_type, file_path in exported_files.items():
                    f.write(f"{file_type.upper()}: {file_path}\n")
            
            exported_files['summary'] = str(summary_path)
            self.logger.info(f"✅ Resumo exportado: {summary_path}")
            
            self.logger.info(f"🎉 Exportação concluída! {len(exported_files)} arquivos gerados")
            
            return exported_files
            
        except Exception as e:
            self.logger.error(f"❌ Erro na exportação: {str(e)}")
            raise
    
    def get_merge_statistics(self) -> Dict[str, Any]:
        """
        Retorna estatísticas do merge
        
        Returns:
            Dicionário com estatísticas
        """
        return self.merge_stats.copy()
    
    def validate_merge_results(self, merged_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida os resultados do merge
        
        Args:
            merged_df: DataFrame com resultados merged
            
        Returns:
            Dicionário com validação
        """
        self.logger.info("🔍 Validando resultados do merge")
        
        validation = {
            'total_rows': len(merged_df),
            'rows_with_classification': len(merged_df[merged_df['insurance_classification_success'] == True]),
            'rows_without_classification': len(merged_df[merged_df['insurance_classification_success'] == False]),
            'insurance_companies': len(merged_df[merged_df['is_insurance'] == True]),
            'non_insurance_companies': len(merged_df[merged_df['is_insurance'] == False]),
            'missing_data': {
                'organization_name': merged_df['Home organization'].isna().sum(),
                'classification_result': merged_df['is_insurance'].isna().sum()
            },

        }
        

        
        self.logger.info(f"📊 Validação concluída:")
        self.logger.info(f"   • Linhas com classificação: {validation['rows_with_classification']}")
        
        return validation


def main():
    """Função para testar o result merger"""
    merger = ResultMerger()
    
    print("🧪 Testando Result Merger")
    print("=" * 40)
    
    # Testar carregamento de resultados de classificação
    print("📦 Carregando resultados de classificação...")
    results = merger.load_classification_results()
    print(f"Resultados carregados: {len(results)}")
    
    if results:
        print("\n📋 Primeiros resultados:")
        for i, (org, result) in enumerate(list(results.items())[:3]):
            status = "SEGURADORA" if result.get('is_insurance') else "NÃO-SEGURADORA"
            print(f"  {i+1}. {org}: {status}")
    
    print("\n✅ Teste concluído!")


if __name__ == "__main__":
    main()