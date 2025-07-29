#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Data Processor - Carrega e processa dados do Excel da COP29

Este módulo é responsável por:
1. Carregar o arquivo Excel com múltiplas abas
2. Extrair apenas as colunas relevantes
3. Fazer merge de todas as abas em um DataFrame único
4. Adicionar coluna "Type" com o nome da aba de origem
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple
import sys

# Adicionar src ao path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger_config import setup_logger
from utils.config_manager import config_manager


class DataProcessor:
    """
    Processador de dados do Excel da COP29
    """
    
    def __init__(self):
        self.logger, _ = setup_logger("data_processor", log_to_file=True)
        self.config = config_manager.get_data_config()
        self.cleaning_config = config_manager.get_data_cleaning_config()
        
        self.logger.info("📊 Inicializando Data Processor")
        self.logger.debug(f"Arquivo Excel: {self.config['excel_file']}")
        self.logger.debug(f"Abas excluídas: {self.config['excluded_sheets']}")
        self.logger.debug(f"Colunas necessárias: {self.config['required_columns']}")
    
    def load_excel_data(self, file_path: str = None) -> Dict[str, pd.DataFrame]:
        """
        Carrega todas as abas do arquivo Excel
        
        Args:
            file_path: Caminho para o arquivo Excel (opcional, usa config se não fornecido)
            
        Returns:
            Dict com nome da aba como chave e DataFrame como valor
        """
        if file_path is None:
            file_path = self.config['excel_file']
        
        excel_path = Path(file_path)
        
        if not excel_path.exists():
            raise FileNotFoundError(f"Arquivo Excel não encontrado: {excel_path}")
        
        self.logger.info(f"📂 Carregando arquivo Excel: {excel_path}")
        
        try:
            # Carregar todas as abas do Excel
            all_sheets = pd.read_excel(excel_path, sheet_name=None, engine='openpyxl')
            
            self.logger.info(f"📋 Encontradas {len(all_sheets)} abas no arquivo")
            
            # Filtrar abas excluídas
            filtered_sheets = {}
            excluded_sheets = [sheet.lower() for sheet in self.config['excluded_sheets']]
            
            for sheet_name, df in all_sheets.items():
                if sheet_name.lower() not in excluded_sheets:
                    filtered_sheets[sheet_name] = df
                    self.logger.debug(f"✅ Aba incluída: '{sheet_name}' ({len(df)} linhas)")
                else:
                    self.logger.debug(f"⏭️ Aba excluída: '{sheet_name}'")
            
            self.logger.success(f"✨ {len(filtered_sheets)} abas carregadas com sucesso")
            return filtered_sheets
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao carregar Excel: {str(e)}")
            raise
    
    def extract_relevant_columns(self, df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
        """
        Extrai apenas as colunas relevantes de um DataFrame com mapeamento de sinônimos
        
        Args:
            df: DataFrame da aba
            sheet_name: Nome da aba (para logging)
            
        Returns:
            DataFrame com apenas as colunas relevantes e nomes padronizados
        """
        # Mapeamento de sinônimos de colunas (V2.0 improvement)
        column_synonyms = {
            'Nominated by': ['Nominated by', 'Nominator'],
            'Organization': ['Home organization', 'Organization', 'Home Organization'],
            'Name': ['Name']
        }
        
        # Colunas padronizadas que queremos no output final
        standard_columns = ['Nominated by', 'Name', 'Organization']
        
        available_cols = df.columns.tolist()
        
        self.logger.debug(f"🔍 Processando aba '{sheet_name}':")
        self.logger.debug(f"   Colunas disponíveis: {available_cols}")
        
        # Mapear colunas disponíveis para nomes padronizados
        column_mapping = {}
        missing_cols = []
        
        for standard_col in standard_columns:
            found = False
            synonyms = column_synonyms.get(standard_col, [standard_col])
            
            for synonym in synonyms:
                if synonym in available_cols:
                    column_mapping[synonym] = standard_col
                    found = True
                    self.logger.debug(f"   ✅ '{synonym}' -> '{standard_col}'")
                    break
            
            if not found:
                missing_cols.append(standard_col)
                self.logger.warning(f"   ⚠️ Coluna '{standard_col}' não encontrada (sinônimos: {synonyms})")
        
        if not column_mapping:
            self.logger.error(f"❌ Nenhuma coluna relevante encontrada em '{sheet_name}'")
            return pd.DataFrame()  # DataFrame vazio
        
        # Extrair e renomear colunas
        cols_to_extract = list(column_mapping.keys())
        filtered_df = df[cols_to_extract].copy()
        
        # Renomear para nomes padronizados
        filtered_df = filtered_df.rename(columns=column_mapping)
        
        # Adicionar colunas ausentes como NaN
        for col in missing_cols:
            filtered_df[col] = None
        
        # Reordenar colunas na ordem esperada
        filtered_df = filtered_df[standard_columns]
        
        # 1. PRIMEIRO: Dropar linhas com valores NA em Home organization (se a coluna existir)
        if 'Home organization' in filtered_df.columns:
            na_values_to_drop = [
                "Not applicable", "Not Applicable", "not applicable", 
                "-", ".", "none", "None", "NONE", "N/A", "n/a", "NA", "na"
            ]
            
            initial_count = len(filtered_df)
            # Dropar linhas onde Home organization é exatamente um dos valores NA
            filtered_df = filtered_df[~filtered_df['Home organization'].isin(na_values_to_drop)]
            dropped_count = initial_count - len(filtered_df)
            
            if dropped_count > 0:
                self.logger.debug(f"🗑️ Removidas {dropped_count} linhas com valores NA em '{sheet_name}'")
        
        # 2. DEPOIS: Para Party overflow, adicionar país apenas para organizações governamentais (se colunas existirem)
        if (sheet_name.lower() == "party overflow" and len(filtered_df) > 0 and 
            'Home organization' in filtered_df.columns and 'Nominated by' in filtered_df.columns):
            
            self.logger.debug(f"🔄 Aplicando lógica inteligente para '{sheet_name}': adicionando país apenas a organizações governamentais")
            
            # Palavras-chave que indicam organizações governamentais
            government_keywords = [
                'embassy', 'government', 'parliament', 'ministry', 'department', 
                'secretary', 'ministerio', 'ministre', 'ministère', 'ministério',
                'secretariat', 'secretaria', 'council', 'conselho', 'cabinet',
                'administration', 'administração', 'agency', 'agência', 'bureau',
                'office', 'escritório', 'commission', 'comissão', 'authority',
                'autoridade', 'directorate', 'diretoria', 'institute', 'instituto',
                'service', 'serviço', 'central bank', 'banco central', 'treasury',
                'tesouro', 'customs', 'alfândega', 'immigration', 'imigração'
            ]
            
            combined_count = 0
            for idx, row in filtered_df.iterrows():
                nominated_by = str(row['Nominated by']).strip()
                home_org = str(row['Home organization']).strip().lower()
                
                # Verificar se é organização governamental
                is_government = any(keyword in home_org for keyword in government_keywords)
                
                if is_government:
                    # Verificar se o país já não está no nome (flexível)
                    country_words = nominated_by.lower().split()
                    country_already_present = any(
                        any(country_word in home_org for country_word in country_words if len(country_word) >= 4)
                        for country_word in country_words
                    )
                    
                    if not country_already_present:
                        new_org = f"{nominated_by} {row['Home organization']}"
                        filtered_df.at[idx, 'Home organization'] = new_org
                        combined_count += 1
            
            self.logger.debug(f"   Adicionado país a {combined_count} organizações governamentais")
        
        self.logger.debug(f"✅ Extraídas {len(cols_to_extract)} colunas de '{sheet_name}' ({len(filtered_df)} linhas)")
        
        return filtered_df
    
    def merge_spreadsheets(self, sheets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Faz merge de todas as abas em um DataFrame único
        
        Args:
            sheets: Dict com DataFrames das abas
            
        Returns:
            DataFrame unificado com coluna "Type" indicando a aba de origem
        """
        self.logger.info("🔄 Iniciando merge das abas...")
        
        merged_data = []
        total_rows = 0
        
        for sheet_name, df in sheets.items():
            # Extrair colunas relevantes
            processed_df = self.extract_relevant_columns(df, sheet_name)
            
            if processed_df.empty:
                self.logger.warning(f"⚠️ Aba '{sheet_name}' resultou em DataFrame vazio")
                continue
            
            # Adicionar coluna "Type" com nome da aba
            processed_df['Type'] = sheet_name
            
            # Adicionar ao merge
            merged_data.append(processed_df)
            total_rows += len(processed_df)
            
            self.logger.debug(f"📝 Aba '{sheet_name}': {len(processed_df)} linhas adicionadas")
        
        if not merged_data:
            raise ValueError("Nenhuma aba válida encontrada para merge")
        
        # Concatenar todos os DataFrames
        final_df = pd.concat(merged_data, ignore_index=True)
        
        # Reordenar colunas para colocar "Type" primeiro
        cols = ['Type'] + self.config['required_columns']
        final_df = final_df[cols]
        
        self.logger.success(f"✨ Merge concluído: {len(final_df)} linhas de {len(merged_data)} abas")
        self.logger.info(f"📊 Estatísticas do dataset final:")
        self.logger.info(f"   Total de linhas: {len(final_df)}")
        self.logger.info(f"   Colunas: {list(final_df.columns)}")
        
        # Mostrar distribuição por tipo (aba)
        type_counts = final_df['Type'].value_counts()
        for sheet_type, count in type_counts.items():
            self.logger.info(f"   {sheet_type}: {count} linhas")
        
        return final_df
    
    def clean_null_organizations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove linhas com valores de organização considerados nulos/vazios
        
        Args:
            df: DataFrame para limpar
            
        Returns:
            DataFrame limpo sem valores nulos configurados
        """
        self.logger.info("🧹 Iniciando limpeza de organizações nulas...")
        
        initial_count = len(df)
        null_values = self.cleaning_config['null_organization_values']
        
        self.logger.debug(f"Valores considerados nulos: {null_values}")
        
        # Determinar qual coluna de organização usar (V2.0 compatibility)
        org_column = None
        if 'Home organization' in df.columns:
            org_column = 'Home organization'
        elif 'Organization' in df.columns:
            org_column = 'Organization'
        else:
            self.logger.warning("⚠️ Nenhuma coluna de organização encontrada para limpeza")
            return df
        
        self.logger.debug(f"Usando coluna '{org_column}' para limpeza")
        
        # Remover linhas onde a coluna de organização contém valores nulos
        # Isso captura tanto valores diretos quanto combinados (ex: "Albania Not Applicable")
        mask = pd.Series([True] * len(df), index=df.index)
        
        for null_value in null_values:
            # Verificar valores exatos
            exact_match = df[org_column] == null_value
            
            # Verificar valores que terminam com o valor nulo (para casos como "Albania Not Applicable")
            ends_with_match = df[org_column].str.endswith(f" {null_value}", na=False)
            
            # Combinar as duas condições
            current_mask = exact_match | ends_with_match
            mask = mask & ~current_mask
        
        cleaned_df = df[mask].copy()
        
        removed_count = initial_count - len(cleaned_df)
        
        if removed_count > 0:
            self.logger.info(f"🗑️ Removidas {removed_count} linhas com organizações nulas")
            
            # Mostrar estatísticas dos valores removidos
            removed_df = df[~mask]
            removed_counts = removed_df[org_column].value_counts()
            for value, count in removed_counts.items():
                self.logger.debug(f"   '{value}': {count} linhas removidas")
        else:
            self.logger.info("✅ Nenhuma linha com organização nula encontrada")
        
        self.logger.success(f"✨ Limpeza concluída: {len(cleaned_df)} linhas restantes")
        return cleaned_df
    
    def validate_data_quality(self, df: pd.DataFrame) -> bool:
        """
        Valida a qualidade dos dados carregados
        
        Args:
            df: DataFrame para validar
            
        Returns:
            True se os dados passaram na validação
        """
        self.logger.info("🔍 Validando qualidade dos dados...")
        
        issues = []
        
        # Verificar se DataFrame não está vazio
        if df.empty:
            issues.append("DataFrame está vazio")
        
        # Verificar colunas obrigatórias
        required_cols = ['Type'] + self.config['required_columns']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            issues.append(f"Colunas ausentes: {missing_cols}")
        
        # Verificar se há dados na coluna de organização (V2.0 compatibility)
        org_column = None
        if 'Home organization' in df.columns:
            org_column = 'Home organization'
        elif 'Organization' in df.columns:
            org_column = 'Organization'
        
        if org_column:
            null_orgs = df[org_column].isnull().sum()
            total_rows = len(df)
            null_percentage = (null_orgs / total_rows) * 100
            
            self.logger.info(f"📈 Organizações nulas em '{org_column}': {null_orgs}/{total_rows} ({null_percentage:.1f}%)")
            
            if null_percentage > 50:
                issues.append(f"Muitas organizações nulas: {null_percentage:.1f}%")
        
        # Verificar duplicatas completas
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            self.logger.warning(f"⚠️ Encontradas {duplicates} linhas duplicadas")
        
        # Reportar resultados
        if issues:
            self.logger.error("❌ Problemas de qualidade encontrados:")
            for issue in issues:
                self.logger.error(f"   - {issue}")
            return False
        else:
            self.logger.success("✅ Validação de qualidade passou!")
            return True
    
    def process_multiple_excel_files(self, raw_dir: str = "data/raw") -> pd.DataFrame:
        """
        Método principal V2.0 que processa múltiplos arquivos Excel
        
        Args:
            raw_dir: Diretório com arquivos Excel
            
        Returns:
            DataFrame processado e validado com dados de todos os arquivos
        """
        self.logger.info("🚀 Iniciando processamento V2.0 - múltiplos arquivos Excel")
        
        try:
            # 1. Encontrar todos os arquivos Excel
            raw_path = Path(raw_dir)
            excel_files = list(raw_path.glob("*.xlsx"))
            
            if not excel_files:
                raise FileNotFoundError(f"Nenhum arquivo Excel encontrado em {raw_dir}")
            
            self.logger.info(f"📁 Encontrados {len(excel_files)} arquivos Excel:")
            for file in excel_files:
                self.logger.info(f"   - {file.name}")
            
            # 2. Processar cada arquivo e combinar
            all_data = []
            
            for excel_file in excel_files:
                filename = excel_file.stem  # Nome sem extensão (ex: COP29)
                self.logger.info(f"📊 Processando {excel_file.name}...")
                
                # Carregar e processar arquivo individual
                sheets = self.load_excel_data(str(excel_file))
                merged_df = self.merge_spreadsheets(sheets)
                cleaned_df = self.clean_null_organizations(merged_df)
                
                # Adicionar coluna File ANTES da coluna Type
                cleaned_df = self.add_file_source_column(cleaned_df, filename)
                
                all_data.append(cleaned_df)
                self.logger.info(f"✅ {excel_file.name}: {len(cleaned_df)} linhas processadas")
            
            # 3. Combinar todos os dados
            self.logger.info("🔄 Combinando dados de todos os arquivos...")
            final_df = pd.concat(all_data, ignore_index=True)
            
            # 4. Validar qualidade
            if not self.validate_data_quality(final_df):
                self.logger.warning("⚠️ Dados passaram na validação com avisos")
            
            # 5. Salvar dados processados
            output_path = Path("data/processed/merged_data.csv")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            final_df.to_csv(output_path, index=False, encoding='utf-8')
            self.logger.info(f"💾 Dados combinados salvos em: {output_path}")
            
            # Log estatísticas finais
            self.logger.info(f"📊 Estatísticas finais:")
            self.logger.info(f"   Total de linhas: {len(final_df)}")
            self.logger.info(f"   Arquivos processados: {len(excel_files)}")
            
            # Mostrar distribuição por arquivo
            file_counts = final_df['File'].value_counts()
            for file_name, count in file_counts.items():
                self.logger.info(f"   {file_name}: {count} linhas")
            
            self.logger.success("✨ Processamento V2.0 concluído com sucesso!")
            return final_df
            
        except Exception as e:
            self.logger.error(f"❌ Erro no processamento V2.0: {str(e)}")
            raise
    
    def add_file_source_column(self, df: pd.DataFrame, filename: str) -> pd.DataFrame:
        """
        Adiciona coluna File antes da coluna Type
        
        Args:
            df: DataFrame para modificar
            filename: Nome do arquivo fonte (sem extensão)
            
        Returns:
            DataFrame com coluna File adicionada
        """
        # Criar nova coluna File
        df['File'] = filename
        
        # Reordenar colunas para colocar File antes de Type
        cols = df.columns.tolist()
        
        # Remover File da posição atual
        cols.remove('File')
        
        # Encontrar posição de Type e inserir File antes
        if 'Type' in cols:
            type_index = cols.index('Type')
            cols.insert(type_index, 'File')
        else:
            # Se não tem Type, colocar File no início
            cols.insert(0, 'File')
        
        # Reordenar DataFrame
        df = df[cols]
        
        return df
    
    def process_multiple_excel_files(self, raw_dir: str = "data/raw") -> pd.DataFrame:
        """
        Método principal V2.0 que processa múltiplos arquivos Excel
        
        Args:
            raw_dir: Diretório com arquivos Excel
            
        Returns:
            DataFrame processado e validado com dados de todos os arquivos
        """
        self.logger.info("🚀 Iniciando processamento V2.0 - Múltiplos arquivos Excel")
        
        try:
            # 1. Encontrar todos os arquivos Excel
            raw_path = Path(raw_dir)
            excel_files = list(raw_path.glob("*.xlsx"))
            
            if not excel_files:
                raise FileNotFoundError(f"Nenhum arquivo Excel encontrado em {raw_dir}")
            
            self.logger.info(f"📁 Encontrados {len(excel_files)} arquivos Excel:")
            for file in excel_files:
                self.logger.info(f"   - {file.name}")
            
            # 2. Processar cada arquivo e combinar
            all_dataframes = []
            
            for excel_file in excel_files:
                filename_without_ext = excel_file.stem  # Ex: "COP29"
                self.logger.info(f"📂 Processando {excel_file.name}...")
                
                # Carregar e processar arquivo individual
                sheets = self.load_excel_data(str(excel_file))
                merged_df = self.merge_spreadsheets(sheets)
                cleaned_df = self.clean_null_organizations(merged_df)
                
                # Adicionar coluna File ANTES da coluna Type
                cleaned_df = self.add_file_source_column(cleaned_df, filename_without_ext)
                
                all_dataframes.append(cleaned_df)
                self.logger.info(f"✅ {excel_file.name}: {len(cleaned_df)} linhas processadas")
            
            # 3. Combinar todos os DataFrames
            self.logger.info("🔄 Combinando dados de todos os arquivos...")
            final_df = pd.concat(all_dataframes, ignore_index=True)
            
            # 4. Limpeza final pós-combinação
            self.logger.info("🧹 Limpeza final pós-combinação...")
            initial_count = len(final_df)
            
            # Remover duplicatas completas
            final_df = final_df.drop_duplicates()
            duplicates_removed = initial_count - len(final_df)
            if duplicates_removed > 0:
                self.logger.info(f"🗑️ Removidas {duplicates_removed} linhas duplicadas")
            
            # Remover linhas com organizações nulas (caso tenham passado)
            org_column = 'Organization' if 'Organization' in final_df.columns else 'Home organization'
            if org_column in final_df.columns:
                null_orgs_before = final_df[org_column].isnull().sum()
                final_df = final_df.dropna(subset=[org_column])
                null_orgs_removed = null_orgs_before - final_df[org_column].isnull().sum()
                if null_orgs_removed > 0:
                    self.logger.info(f"🗑️ Removidas {null_orgs_removed} linhas com organizações nulas")
            
            final_count = len(final_df)
            total_removed = initial_count - final_count
            if total_removed > 0:
                self.logger.info(f"✨ Limpeza final: {total_removed} linhas removidas, {final_count} linhas restantes")
            
            # 5. Validar qualidade final
            if not self.validate_data_quality(final_df):
                self.logger.warning("⚠️ Dados passaram na validação com avisos")
            
            # 5. Salvar dados processados
            output_path = Path("data/processed/merged_data.csv")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            final_df.to_csv(output_path, index=False, encoding='utf-8')
            self.logger.info(f"💾 Dados combinados salvos em: {output_path}")
            
            # Estatísticas finais
            self.logger.success("✨ Processamento V2.0 concluído com sucesso!")
            self.logger.info(f"📊 Estatísticas finais:")
            self.logger.info(f"   Total de arquivos: {len(excel_files)}")
            self.logger.info(f"   Total de linhas: {len(final_df)}")
            
            # Mostrar distribuição por arquivo
            file_counts = final_df['File'].value_counts()
            for file_name, count in file_counts.items():
                self.logger.info(f"   {file_name}: {count} linhas")
            
            return final_df
            
        except Exception as e:
            self.logger.error(f"❌ Erro no processamento V2.0: {str(e)}")
            raise
    
    def add_file_source_column(self, df: pd.DataFrame, filename: str) -> pd.DataFrame:
        """
        Adiciona coluna File antes da coluna Type para rastrear origem dos dados
        
        Args:
            df: DataFrame processado
            filename: Nome do arquivo sem extensão (ex: "COP29")
            
        Returns:
            DataFrame com coluna File adicionada
        """
        # Criar nova coluna File
        df_with_file = df.copy()
        df_with_file['File'] = filename
        
        # Reordenar colunas para colocar File antes de Type
        cols = df_with_file.columns.tolist()
        
        # Remover File da posição atual e inserir antes de Type
        cols.remove('File')
        type_index = cols.index('Type')
        cols.insert(type_index, 'File')
        
        df_with_file = df_with_file[cols]
        
        self.logger.debug(f"✅ Coluna 'File' adicionada com valor '{filename}'")
        return df_with_file
    
    def process_excel_file(self, file_path: str = None) -> pd.DataFrame:
        """
        Método de compatibilidade V1.0 - processa arquivo único
        
        Args:
            file_path: Caminho para o arquivo Excel (opcional)
            
        Returns:
            DataFrame processado e validado
        """
        self.logger.info("🚀 Iniciando processamento V1.0 - arquivo único")
        
        try:
            # 1. Carregar dados
            sheets = self.load_excel_data(file_path)
            
            # 2. Fazer merge
            merged_df = self.merge_spreadsheets(sheets)
            
            # 3. Limpar organizações nulas
            cleaned_df = self.clean_null_organizations(merged_df)
            
            # 4. Validar qualidade
            if not self.validate_data_quality(cleaned_df):
                self.logger.warning("⚠️ Dados passaram na validação com avisos")
            
            # 5. Salvar dados processados
            output_path = Path("data/processed/merged_data.csv")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            cleaned_df.to_csv(output_path, index=False, encoding='utf-8')
            self.logger.info(f"💾 Dados salvos em: {output_path}")
            
            self.logger.success("✨ Processamento V1.0 concluído com sucesso!")
            return cleaned_df
            
        except Exception as e:
            self.logger.error(f"❌ Erro no processamento V1.0: {str(e)}")
            raise


def main():
    """Função para testar o processador de dados"""
    processor = DataProcessor()
    
    try:
        df = processor.process_excel_file()
        print(f"\n📊 Resumo dos dados processados:")
        print(f"Shape: {df.shape}")
        print(f"Colunas: {list(df.columns)}")
        print(f"\nPrimeiras 5 linhas:")
        print(df.head())
        
        print(f"\nDistribuição por Type:")
        print(df['Type'].value_counts())
        
    except Exception as e:
        print(f"Erro: {e}")


if __name__ == "__main__":
    main()