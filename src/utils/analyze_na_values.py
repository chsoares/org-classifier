#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Analyze NA Values - Ferramenta para identificar valores nulos em organizações

Esta ferramenta analisa o dataset processado e identifica valores que representam
organizações nulas/vazias, ajudando na configuração da limpeza de dados.
"""

import pandas as pd
import sys
from pathlib import Path
from collections import Counter

# Adicionar src ao path para imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.logger_config import setup_logger
from utils.config_manager import config_manager


class NAValueAnalyzer:
    """
    Analisador de valores nulos em organizações
    """
    
    def __init__(self):
        self.logger, _ = setup_logger("na_analyzer", log_to_file=False)
        self.cleaning_config = config_manager.get_data_cleaning_config()
    
    def analyze_na_values(self, csv_path: str = "data/processed/merged_data.csv") -> dict:
        """
        Analisa valores que representam organizações nulas/vazias
        
        Args:
            csv_path: Caminho para o arquivo CSV processado
            
        Returns:
            Dict com estatísticas dos valores NA encontrados
        """
        self.logger.info("🔍 Analisando valores que representam NA...")
        
        if not Path(csv_path).exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {csv_path}")
        
        # Carregar dados processados
        df = pd.read_csv(csv_path)
        
        # Analisar valores únicos de Home organization
        home_orgs = df['Home organization'].value_counts()
        
        self.logger.info(f"📊 Total de organizações únicas: {len(home_orgs)}")
        self.logger.info(f"📊 Total de linhas: {len(df)}")
        
        # Procurar por valores que parecem ser NA
        potential_na_values = []
        
        for org, count in home_orgs.items():
            org_str = str(org).strip()
            org_lower = org_str.lower()
            
            # Critérios para identificar valores NA
            if self._is_na_value(org_str, org_lower):
                potential_na_values.append((org_str, count))
        
        # Ordenar por frequência
        potential_na_values.sort(key=lambda x: x[1], reverse=True)
        
        total_na_lines = sum(count for _, count in potential_na_values)
        
        self.logger.info(f"🚨 Valores identificados como NA ({len(potential_na_values)} tipos):")
        for org, count in potential_na_values:
            self.logger.info(f"   '{org}': {count} ocorrências")
        
        self.logger.info(f"📈 Total de linhas com valores NA: {total_na_lines}")
        
        # Mostrar exemplos
        self._show_examples(df, potential_na_values[:5])
        
        # Comparar com configuração atual
        self._compare_with_config(potential_na_values)
        
        return {
            'total_organizations': len(home_orgs),
            'total_lines': len(df),
            'na_values': potential_na_values,
            'total_na_lines': total_na_lines
        }
    
    def _is_na_value(self, org_str: str, org_lower: str) -> bool:
        """
        Determina se um valor representa uma organização nula
        
        Args:
            org_str: Valor original da organização
            org_lower: Valor em minúsculas
            
        Returns:
            True se o valor representa uma organização nula
        """
        # Lista de valores explicitamente nulos
        explicit_na_values = [
            'not applicable', 'not available', 'n/a', 'na', '-', '--', '---', 
            'none', 'null', 'nan', 'not specified', 'not provided', 'tbd', 
            'to be determined', 'unknown', '?', 'not known', 'not given',
            'not mentioned', 'not stated', 'not indicated', 'not disclosed', '.'
        ]
        
        # Verificações
        if org_lower in explicit_na_values:
            return True
        
        if org_lower.startswith(('not applicable', 'not available', 'n/a')):
            return True
        
        # Símbolos únicos
        if len(org_lower) <= 1 and org_lower in ['-', '.', '?', '']:
            return True
        
        # Valores muito curtos que parecem códigos vazios
        if len(org_str.strip()) <= 2 and org_str.strip() in ['-', '--', '..', '??', 'na', 'NA']:
            return True
        
        return False
    
    def _show_examples(self, df: pd.DataFrame, na_values: list):
        """
        Mostra exemplos de linhas com valores NA
        
        Args:
            df: DataFrame com os dados
            na_values: Lista de valores NA encontrados
        """
        self.logger.info("🔍 Exemplos de linhas com valores NA:")
        
        for org, count in na_values:
            examples = df[df['Home organization'] == org][
                ['Type', 'Nominated by', 'Name', 'Home organization']
            ].head(3)
            
            self.logger.info(f"\n   Valor: '{org}' ({count} ocorrências)")
            for _, row in examples.iterrows():
                self.logger.info(f"      {row['Type']} | {row['Nominated by']} | {row['Name']} | {row['Home organization']}")
    
    def _compare_with_config(self, potential_na_values: list):
        """
        Compara valores encontrados com a configuração atual
        
        Args:
            potential_na_values: Lista de valores NA encontrados
        """
        configured_values = set(self.cleaning_config['null_organization_values'])
        found_values = set(org for org, _ in potential_na_values)
        
        # Valores configurados mas não encontrados
        configured_not_found = configured_values - found_values
        if configured_not_found:
            self.logger.info(f"\n⚙️ Valores configurados mas não encontrados nos dados:")
            for value in sorted(configured_not_found):
                self.logger.info(f"   '{value}'")
        
        # Valores encontrados mas não configurados
        found_not_configured = found_values - configured_values
        if found_not_configured:
            self.logger.info(f"\n🆕 Valores encontrados mas não configurados:")
            for value in sorted(found_not_configured):
                count = next(count for org, count in potential_na_values if org == value)
                self.logger.info(f"   '{value}': {count} ocorrências")
            
            self.logger.info(f"\n💡 Sugestão: Adicione estes valores ao config.yaml em 'data_cleaning.null_organization_values'")
    
    def generate_config_suggestion(self, potential_na_values: list) -> list:
        """
        Gera sugestão de configuração baseada nos valores encontrados
        
        Args:
            potential_na_values: Lista de valores NA encontrados
            
        Returns:
            Lista de valores sugeridos para configuração
        """
        # Pegar apenas valores com mais de 1 ocorrência
        suggested_values = [org for org, count in potential_na_values if count > 0]
        
        # Adicionar valores padrão comuns
        default_values = [
            "Not applicable", "Not Applicable", "not applicable",
            "N/A", "n/a", "NA", "na", 
            "-", ".", "none", "None", "NONE"
        ]
        
        # Combinar e remover duplicatas mantendo ordem
        all_values = []
        seen = set()
        
        for value in suggested_values + default_values:
            if value not in seen:
                all_values.append(value)
                seen.add(value)
        
        return all_values


def main():
    """Função principal para executar a análise"""
    analyzer = NAValueAnalyzer()
    
    try:
        results = analyzer.analyze_na_values()
        
        print(f"\n📋 Resumo da Análise:")
        print(f"Total de organizações únicas: {results['total_organizations']}")
        print(f"Total de linhas: {results['total_lines']}")
        print(f"Valores NA encontrados: {len(results['na_values'])} tipos")
        print(f"Linhas com valores NA: {results['total_na_lines']}")
        
        if results['na_values']:
            print(f"\n🔧 Sugestão de configuração:")
            suggested_config = analyzer.generate_config_suggestion(results['na_values'])
            print("null_organization_values:")
            for value in suggested_config:
                print(f'  - "{value}"')
        
    except Exception as e:
        print(f"❌ Erro na análise: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()