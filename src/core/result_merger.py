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
    
    def create_people_dataset(self, processed_df: pd.DataFrame, 
                             organizations_df: pd.DataFrame,
                             org_column: str = 'Home organization') -> pd.DataFrame:
        """
        Cria dataset final de pessoas com APENAS coluna is_insurance adicionada
        
        Args:
            processed_df: DataFrame processado e normalizado (merged_data_normalized.csv)
            organizations_df: DataFrame de organizações com classificações (results/organizations.csv)
            org_column: Nome da coluna com organizações
            
        Returns:
            DataFrame final para results/people.csv
        """
        self.logger.info("👥 Criando dataset final de pessoas")
        
        try:
            # Criar cópia do DataFrame processado
            people_df = processed_df.copy()
            
            # Simplificar colunas de organização se necessário
            if 'Home organization_normalized' in people_df.columns and 'Home organization' in people_df.columns:
                # Substituir coluna original pela normalizada
                people_df['Home organization'] = people_df['Home organization_normalized']
                people_df.drop('Home organization_normalized', axis=1, inplace=True)
                self.logger.info("✅ Coluna 'Home organization' atualizada com valores normalizados")
            
            # Verificar se coluna de organização existe
            if org_column not in people_df.columns:
                available_cols = [col for col in people_df.columns if 'organization' in col.lower()]
                if available_cols:
                    org_column = available_cols[0]
                    self.logger.warning(f"⚠️ Coluna '{org_column}' não encontrada, usando '{org_column}'")
                else:
                    raise ValueError(f"Coluna de organização não encontrada. Colunas disponíveis: {list(people_df.columns)}")
            
            # Inicializar APENAS coluna is_insurance (conforme processo definido)
            people_df['is_insurance'] = None
            
            # Criar dicionário de lookup das organizações
            org_lookup = {}
            for _, org_row in organizations_df.iterrows():
                org_name = org_row.get('organization_name', '')
                if org_name:
                    org_lookup[org_name] = org_row.get('is_insurance', None)
            
            # Estatísticas
            total_rows = len(people_df)
            unique_orgs = people_df[org_column].nunique()
            matched_count = 0
            insurance_people = 0
            non_insurance_people = 0
            
            self.logger.info(f"📊 Dataset de pessoas: {total_rows} participantes, {unique_orgs} organizações únicas")
            self.logger.info(f"📋 Organizações classificadas disponíveis: {len(org_lookup)}")
            
            # Fazer matching linha por linha - APENAS is_insurance
            for idx, row in people_df.iterrows():
                org_name = row[org_column]
                
                if pd.isna(org_name) or not org_name:
                    continue
                
                # Limpar nome da organização
                org_name_clean = str(org_name).strip()
                
                # Procurar classificação na tabela de organizações
                if org_name_clean in org_lookup:
                    is_insurance = org_lookup[org_name_clean]
                    people_df.at[idx, 'is_insurance'] = is_insurance
                    matched_count += 1
                    
                    if is_insurance is True:
                        insurance_people += 1
                    elif is_insurance is False:
                        non_insurance_people += 1
            
            # Calcular estatísticas finais
            classification_rate = (matched_count / total_rows * 100) if total_rows > 0 else 0
            
            self.merge_stats = {
                'total_participants': total_rows,
                'participants_with_classification': matched_count,
                'participants_without_classification': total_rows - matched_count,
                'insurance_participants': insurance_people,
                'non_insurance_participants': non_insurance_people,
                'classification_rate': classification_rate
            }
            
            self.logger.info("✅ Dataset de pessoas criado com sucesso!")
            self.logger.info(f"📊 Estatísticas:")
            self.logger.info(f"   • Total de participantes: {total_rows}")
            self.logger.info(f"   • Participantes classificados: {matched_count} ({classification_rate:.1f}%)")
            self.logger.info(f"   • Participantes de seguradoras: {insurance_people}")
            self.logger.info(f"   • Participantes de não-seguradoras: {non_insurance_people}")
            
            return people_df
            
        except Exception as e:
            self.logger.error(f"❌ Erro no merge: {str(e)}")
            raise
    
    def create_multi_file_organizations_csv(self, data: pd.DataFrame, 
                                          organizations_df: pd.DataFrame) -> pd.DataFrame:
        """
        Cria organizations.csv V2.0 com contagens de participantes por arquivo
        
        Args:
            data: DataFrame principal com coluna 'File'
            organizations_df: DataFrame com resultados de classificação
            
        Returns:
            DataFrame de organizações com colunas participants_{filename}
        """
        self.logger.info("📊 Criando organizations.csv V2.0 com contagens multi-arquivo...")
        
        try:
            # Determinar coluna de organização
            org_column = 'Organization' if 'Organization' in data.columns else 'Home organization'
            
            # Obter arquivos únicos
            if 'File' not in data.columns:
                self.logger.warning("⚠️ Coluna 'File' não encontrada, usando contagem única")
                unique_files = ['total']
            else:
                unique_files = sorted(data['File'].unique())
            
            self.logger.info(f"📁 Arquivos encontrados: {unique_files}")
            
            # Contar participantes por organização e arquivo
            if 'File' in data.columns:
                counts_by_file = data.groupby([org_column, 'File']).size().unstack(fill_value=0)
            else:
                counts_by_file = data.groupby(org_column).size().to_frame('total')
            
            # Calcular total
            counts_by_file['participants_total'] = counts_by_file.sum(axis=1)
            
            # Renomear colunas para formato participants_{filename}
            column_mapping = {}
            for col in counts_by_file.columns:
                if col != 'participants_total':
                    column_mapping[col] = f'participants_{col}'
            
            counts_by_file = counts_by_file.rename(columns=column_mapping)
            
            # Criar DataFrame final combinando com dados de classificação
            final_orgs_df = organizations_df.copy()
            
            # Fazer merge com contagens
            final_orgs_df = final_orgs_df.merge(
                counts_by_file, 
                left_on='organization_name', 
                right_index=True, 
                how='left'
            )
            
            # Preencher valores ausentes com 0
            participant_cols = [col for col in final_orgs_df.columns if col.startswith('participants_')]
            for col in participant_cols:
                final_orgs_df[col] = final_orgs_df[col].fillna(0).astype(int)
            
            # Reordenar colunas
            base_cols = ['organization_name']
            participant_cols = sorted([col for col in final_orgs_df.columns if col.startswith('participants_')])
            other_cols = [col for col in final_orgs_df.columns if col not in base_cols + participant_cols]
            
            final_cols = base_cols + participant_cols + other_cols
            final_orgs_df = final_orgs_df[final_cols]
            
            self.logger.success(f"✨ Organizations.csv V2.0 criado: {len(final_orgs_df)} organizações")
            self.logger.info(f"📊 Colunas de participantes: {participant_cols}")
            
            return final_orgs_df
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao criar organizations.csv V2.0: {str(e)}")
            raise
    
    def create_simplified_people_csv(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Cria people.csv V2.0 com coluna única de organização normalizada
        
        Args:
            data: DataFrame principal com dados processados
            
        Returns:
            DataFrame de pessoas com estrutura simplificada
        """
        self.logger.info("👥 Criando people.csv V2.0 simplificado...")
        
        try:
            people_df = data.copy()
            
            # Determinar coluna de organização
            org_column = 'Organization' if 'Organization' in people_df.columns else 'Home organization'
            
            # Renomear coluna de organização para nome padrão
            if org_column != 'Home organization':
                people_df = people_df.rename(columns={org_column: 'Home organization'})
            
            # Remover colunas duplicadas de organização se existirem
            cols_to_remove = []
            for col in people_df.columns:
                if 'organization' in col.lower() and col != 'Home organization':
                    cols_to_remove.append(col)
            
            if cols_to_remove:
                people_df = people_df.drop(columns=cols_to_remove)
                self.logger.info(f"🗑️ Removidas colunas duplicadas: {cols_to_remove}")
            
            # Garantir que coluna File existe
            if 'File' not in people_df.columns:
                people_df['File'] = 'unknown'
                self.logger.warning("⚠️ Coluna 'File' não encontrada, adicionada com valor 'unknown'")
            
            # Reordenar colunas: File, Type, Nominated by, Home organization, Name, is_insurance, outras
            base_cols = ['File', 'Type', 'Nominated by', 'Home organization', 'Name']
            if 'is_insurance' in people_df.columns:
                base_cols.append('is_insurance')
            
            other_cols = [col for col in people_df.columns if col not in base_cols]
            final_cols = [col for col in base_cols if col in people_df.columns] + other_cols
            
            people_df = people_df[final_cols]
            
            self.logger.success(f"✨ People.csv V2.0 criado: {len(people_df)} pessoas")
            self.logger.info(f"📊 Colunas: {list(people_df.columns)}")
            
            return people_df
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao criar people.csv V2.0: {str(e)}")
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