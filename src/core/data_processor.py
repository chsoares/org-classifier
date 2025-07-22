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
        Extrai apenas as colunas relevantes de um DataFrame
        
        Args:
            df: DataFrame da aba
            sheet_name: Nome da aba (para logging)
            
        Returns:
            DataFrame com apenas as colunas relevantes
        """
        required_cols = self.config['required_columns']
        available_cols = df.columns.tolist()
        
        self.logger.debug(f"🔍 Processando aba '{sheet_name}':")
        self.logger.debug(f"   Colunas disponíveis: {available_cols}")
        
        # Verificar quais colunas existem
        missing_cols = []
        existing_cols = []
        
        for col in required_cols:
            if col in available_cols:
                existing_cols.append(col)
            else:
                missing_cols.append(col)
        
        if missing_cols:
            self.logger.warning(f"⚠️ Colunas ausentes em '{sheet_name}': {missing_cols}")
        
        if not existing_cols:
            self.logger.error(f"❌ Nenhuma coluna relevante encontrada em '{sheet_name}'")
            return pd.DataFrame()  # DataFrame vazio
        
        # Extrair apenas as colunas existentes
        filtered_df = df[existing_cols].copy()
        
        # Adicionar colunas ausentes como NaN
        for col in missing_cols:
            filtered_df[col] = None
        
        # Reordenar colunas na ordem esperada
        filtered_df = filtered_df[required_cols]
        
        # 1. PRIMEIRO: Dropar linhas com valores NA em Home organization
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
        
        # 2. DEPOIS: Para Party overflow, adicionar Nominated by ao Home organization
        if sheet_name.lower() == "party overflow" and len(filtered_df) > 0:
            self.logger.debug(f"🔄 Aplicando lógica para '{sheet_name}': adicionando país às organizações")
            
            # SIMPLES: Nominated by + Home organization para todas as linhas
            filtered_df['Home organization'] = filtered_df['Nominated by'] + " " + filtered_df['Home organization']
            
            self.logger.debug(f"   Adicionado país a {len(filtered_df)} organizações")
        
        self.logger.debug(f"✅ Extraídas {len(existing_cols)} colunas de '{sheet_name}' ({len(filtered_df)} linhas)")
        
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
        
        # Remover linhas onde 'Home organization' contém valores nulos
        # Isso captura tanto valores diretos quanto combinados (ex: "Albania Not Applicable")
        mask = pd.Series([True] * len(df), index=df.index)
        
        for null_value in null_values:
            # Verificar valores exatos
            exact_match = df['Home organization'] == null_value
            
            # Verificar valores que terminam com o valor nulo (para casos como "Albania Not Applicable")
            ends_with_match = df['Home organization'].str.endswith(f" {null_value}", na=False)
            
            # Combinar as duas condições
            current_mask = exact_match | ends_with_match
            mask = mask & ~current_mask
        
        cleaned_df = df[mask].copy()
        
        removed_count = initial_count - len(cleaned_df)
        
        if removed_count > 0:
            self.logger.info(f"🗑️ Removidas {removed_count} linhas com organizações nulas")
            
            # Mostrar estatísticas dos valores removidos
            removed_df = df[~mask]
            removed_counts = removed_df['Home organization'].value_counts()
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
        
        # Verificar se há dados em Home organization
        if 'Home organization' in df.columns:
            null_orgs = df['Home organization'].isnull().sum()
            total_rows = len(df)
            null_percentage = (null_orgs / total_rows) * 100
            
            self.logger.info(f"📈 Organizações nulas: {null_orgs}/{total_rows} ({null_percentage:.1f}%)")
            
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
    
    def process_excel_file(self, file_path: str = None) -> pd.DataFrame:
        """
        Método principal que executa todo o pipeline de processamento
        
        Args:
            file_path: Caminho para o arquivo Excel (opcional)
            
        Returns:
            DataFrame processado e validado
        """
        self.logger.info("🚀 Iniciando processamento completo do Excel")
        
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
            
            self.logger.success("✨ Processamento do Excel concluído com sucesso!")
            return cleaned_df
            
        except Exception as e:
            self.logger.error(f"❌ Erro no processamento: {str(e)}")
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