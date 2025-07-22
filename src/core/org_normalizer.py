#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Organization Normalizer - Normaliza nomes de organizações usando fuzzy matching

Este módulo é responsável por:
1. Extrair organizações únicas do dataset
2. Encontrar organizações similares usando fuzzy matching
3. Criar mapeamento para o nome mais frequente
4. Atualizar o dataset principal com nomes normalizados
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple
import sys
from collections import Counter
from rapidfuzz import fuzz, process

# Adicionar src ao path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger_config import setup_logger
from utils.config_manager import config_manager


class OrganizationNormalizer:
    """
    Normalizador de nomes de organizações usando fuzzy matching
    """
    
    def __init__(self):
        self.logger, _ = setup_logger("org_normalizer", log_to_file=True)
        self.fuzzy_config = config_manager.get_fuzzy_config()
        
        self.logger.info("🔧 Inicializando Organization Normalizer")
        self.logger.debug(f"Threshold de similaridade: {self.fuzzy_config['threshold']}%")
        self.logger.debug(f"Máximo de sugestões: {self.fuzzy_config['max_suggestions']}")
    
    def extract_unique_organizations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extrai organizações únicas do dataset com contagem de ocorrências
        
        Args:
            df: DataFrame principal com dados processados
            
        Returns:
            DataFrame com organizações únicas e suas contagens
        """
        self.logger.info("📊 Extraindo organizações únicas...")
        
        # Contar ocorrências de cada organização
        org_counts = df['Home organization'].value_counts()
        
        # Criar DataFrame com organizações únicas
        unique_orgs_df = pd.DataFrame({
            'original_name': org_counts.index,
            'occurrence_count': org_counts.values,
            'normalized_name': org_counts.index,  # Inicialmente igual ao original
            'is_normalized': False  # Flag para indicar se foi normalizado
        }).reset_index(drop=True)
        
        self.logger.success(f"✨ Encontradas {len(unique_orgs_df)} organizações únicas")
        self.logger.info(f"📈 Total de ocorrências: {unique_orgs_df['occurrence_count'].sum()}")
        
        # Mostrar estatísticas
        self.logger.info("📊 Estatísticas de frequência:")
        self.logger.info(f"   Organizações com 1 ocorrência: {(unique_orgs_df['occurrence_count'] == 1).sum()}")
        self.logger.info(f"   Organizações com 2-5 ocorrências: {((unique_orgs_df['occurrence_count'] >= 2) & (unique_orgs_df['occurrence_count'] <= 5)).sum()}")
        self.logger.info(f"   Organizações com 6+ ocorrências: {(unique_orgs_df['occurrence_count'] >= 6).sum()}")
        
        return unique_orgs_df
    
    def _clean_organization_name(self, name: str) -> str:
        """
        Limpa nome da organização para melhor matching
        
        Args:
            name: Nome original da organização
            
        Returns:
            Nome limpo para comparação
        """
        if pd.isna(name):
            return ""
        
        # Converter para string e limpar
        cleaned = str(name).strip()
        
        # Remover caracteres especiais comuns
        cleaned = cleaned.replace(',', '').replace('.', '').replace('&', 'and')
        
        # Normalizar espaços
        cleaned = ' '.join(cleaned.split())
        
        return cleaned
    
    def _validate_similarity(self, org1: str, org2: str) -> bool:
        """
        Valida se duas organizações são realmente similares (não apenas por fuzzy score)
        
        Args:
            org1: Primeira organização
            org2: Segunda organização
            
        Returns:
            True se são realmente similares
        """
        clean1 = self._clean_organization_name(org1).lower()
        clean2 = self._clean_organization_name(org2).lower()
        
        # Palavras-chave que não devem ser confundidas
        conflicting_keywords = [
            ('african', 'asian'), ('africa', 'asia'),
            ('american', 'european'), ('america', 'europe'),
            ('north', 'south'), ('east', 'west'),
            ('development', 'investment'), ('bank', 'fund'),
            ('international', 'national'), ('global', 'local')
        ]
        
        # Verificar se há conflitos de palavras-chave
        for word1, word2 in conflicting_keywords:
            if (word1 in clean1 and word2 in clean2) or (word2 in clean1 and word1 in clean2):
                return False
        
        # Verificar se compartilham pelo menos 50% das palavras principais
        words1 = set(clean1.split())
        words2 = set(clean2.split())
        
        # Remover palavras muito comuns que não são distintivas
        common_words = {'the', 'of', 'and', 'for', 'in', 'on', 'at', 'to', 'a', 'an'}
        words1 = words1 - common_words
        words2 = words2 - common_words
        
        if not words1 or not words2:
            return False
        
        # Calcular sobreposição de palavras
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        
        word_overlap = len(intersection) / len(union) if union else 0
        
        # Exigir pelo menos 30% de sobreposição de palavras
        return word_overlap >= 0.3

    def find_similar_organizations(self, org_list: List[str]) -> Dict[str, str]:
        """
        Encontra organizações similares usando fuzzy matching melhorado
        
        Args:
            org_list: Lista de nomes de organizações
            
        Returns:
            Dicionário mapeando nome original -> nome normalizado
        """
        self.logger.info("🔍 Iniciando busca por organizações similares...")
        
        threshold = self.fuzzy_config['threshold']
        mapping = {}
        processed = set()
        groups_found = 0
        
        for i, org1 in enumerate(org_list):
            if org1 in processed:
                continue
            
            # Limpar nome para comparação
            clean_org1 = self._clean_organization_name(org1)
            if not clean_org1:
                continue
            
            # Encontrar organizações similares
            similar_orgs = [(org1, org_list.index(org1))]  # (nome, índice_frequência)
            
            for j, org2 in enumerate(org_list[i+1:], i+1):
                if org2 in processed:
                    continue
                
                clean_org2 = self._clean_organization_name(org2)
                if not clean_org2:
                    continue
                
                # Calcular similaridade
                similarity = fuzz.ratio(clean_org1.lower(), clean_org2.lower())
                
                # Aplicar validações adicionais
                if similarity >= threshold and self._validate_similarity(org1, org2):
                    similar_orgs.append((org2, j))
                    processed.add(org2)
            
            # Se encontrou organizações similares, criar mapeamento
            if len(similar_orgs) > 1:
                groups_found += 1
                
                # Escolher o nome mais frequente (menor índice = mais frequente)
                similar_orgs.sort(key=lambda x: x[1])  # Ordenar por frequência
                normalized_name = similar_orgs[0][0]
                
                for org_name, _ in similar_orgs:
                    mapping[org_name] = normalized_name
                
                self.logger.debug(f"Grupo {groups_found}: {len(similar_orgs)} organizações similares -> '{normalized_name}'")
                for org_name, _ in similar_orgs[1:]:  # Não mostrar o primeiro (é o normalizado)
                    self.logger.debug(f"   '{org_name}' -> '{normalized_name}'")
            
            processed.add(org1)
        
        self.logger.success(f"✨ Encontrados {groups_found} grupos de organizações similares")
        self.logger.info(f"📊 Total de mapeamentos criados: {len(mapping)}")
        
        return mapping
    
    def normalize_organization_names(self, unique_orgs_df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza nomes de organizações usando fuzzy matching
        
        Args:
            unique_orgs_df: DataFrame com organizações únicas
            
        Returns:
            DataFrame atualizado com nomes normalizados
        """
        self.logger.info("🔄 Iniciando normalização de nomes...")
        
        # Ordenar por frequência (mais frequente primeiro)
        sorted_orgs = unique_orgs_df.sort_values('occurrence_count', ascending=False)
        org_list = sorted_orgs['original_name'].tolist()
        
        # Encontrar organizações similares
        mapping = self.find_similar_organizations(org_list)
        
        # Aplicar mapeamento
        normalized_count = 0
        for idx, row in unique_orgs_df.iterrows():
            original_name = row['original_name']
            
            if original_name in mapping:
                unique_orgs_df.at[idx, 'normalized_name'] = mapping[original_name]
                unique_orgs_df.at[idx, 'is_normalized'] = True
                normalized_count += 1
        
        self.logger.success(f"✨ Normalizadas {normalized_count} organizações")
        
        # Calcular estatísticas finais
        final_unique_count = unique_orgs_df['normalized_name'].nunique()
        original_unique_count = len(unique_orgs_df)
        reduction = original_unique_count - final_unique_count
        reduction_pct = (reduction / original_unique_count) * 100
        
        self.logger.info(f"📊 Resultado da normalização:")
        self.logger.info(f"   Organizações originais: {original_unique_count}")
        self.logger.info(f"   Organizações após normalização: {final_unique_count}")
        self.logger.info(f"   Redução: {reduction} organizações ({reduction_pct:.1f}%)")
        
        return unique_orgs_df
    
    def update_main_dataset(self, main_df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Atualiza o dataset principal com nomes normalizados
        
        Args:
            main_df: DataFrame principal
            mapping: Dicionário de mapeamento original -> normalizado
            
        Returns:
            DataFrame atualizado
        """
        self.logger.info("🔄 Atualizando dataset principal com nomes normalizados...")
        
        # Criar nova coluna com nomes normalizados
        main_df['Home organization_normalized'] = main_df['Home organization'].map(mapping).fillna(main_df['Home organization'])
        
        # Contar quantas linhas foram atualizadas
        updated_count = (main_df['Home organization'] != main_df['Home organization_normalized']).sum()
        
        self.logger.success(f"✨ Atualizadas {updated_count} linhas no dataset principal")
        
        return main_df
    
    def process_normalization(self, input_file: str = "data/processed/merged_data.csv") -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Método principal que executa todo o pipeline de normalização
        
        Args:
            input_file: Caminho para o arquivo de dados processados
            
        Returns:
            Tuple com (dataset_principal_atualizado, dataframe_organizacoes_unicas)
        """
        self.logger.info("🚀 Iniciando processo completo de normalização")
        
        try:
            # 1. Carregar dados processados
            input_path = Path(input_file)
            if not input_path.exists():
                raise FileNotFoundError(f"Arquivo não encontrado: {input_path}")
            
            self.logger.info(f"📂 Carregando dados de: {input_path}")
            main_df = pd.read_csv(input_path)
            
            # 2. Extrair organizações únicas
            unique_orgs_df = self.extract_unique_organizations(main_df)
            
            # 3. Normalizar nomes
            normalized_orgs_df = self.normalize_organization_names(unique_orgs_df)
            
            # 4. Criar mapeamento para o dataset principal
            mapping = dict(zip(normalized_orgs_df['original_name'], normalized_orgs_df['normalized_name']))
            
            # 5. Atualizar dataset principal
            updated_main_df = self.update_main_dataset(main_df, mapping)
            
            # 6. Salvar resultados
            # Salvar dataset principal atualizado
            main_output_path = Path("data/processed/merged_data_normalized.csv")
            updated_main_df.to_csv(main_output_path, index=False, encoding='utf-8')
            self.logger.info(f"💾 Dataset principal salvo em: {main_output_path}")
            
            # Salvar mapeamento de organizações
            orgs_output_path = Path("data/processed/organizations_mapping.csv")
            normalized_orgs_df.to_csv(orgs_output_path, index=False, encoding='utf-8')
            self.logger.info(f"💾 Mapeamento de organizações salvo em: {orgs_output_path}")
            
            self.logger.success("✨ Processo de normalização concluído com sucesso!")
            
            return updated_main_df, normalized_orgs_df
            
        except Exception as e:
            self.logger.error(f"❌ Erro no processo de normalização: {str(e)}")
            raise


def main():
    """Função para testar o normalizador"""
    normalizer = OrganizationNormalizer()
    
    try:
        main_df, orgs_df = normalizer.process_normalization()
        
        print(f"\n📊 Resumo da normalização:")
        print(f"Dataset principal: {main_df.shape}")
        print(f"Organizações únicas: {len(orgs_df)}")
        print(f"Organizações normalizadas: {orgs_df['is_normalized'].sum()}")
        
        print(f"\nPrimeiros 5 mapeamentos:")
        normalized_orgs = orgs_df[orgs_df['is_normalized']].head()
        for _, row in normalized_orgs.iterrows():
            print(f"  '{row['original_name']}' -> '{row['normalized_name']}'")
        
    except Exception as e:
        print(f"Erro: {e}")


if __name__ == "__main__":
    main()