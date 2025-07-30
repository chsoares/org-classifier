#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Cache Manager - Sistema de cache para resultados de processamento

Este módulo é responsável por:
1. Salvar resultados de processamento para evitar reprocessamento
2. Verificar se organizações já foram processadas
3. Permitir limpeza manual do cache
4. Incluir timestamps para controle de validade
5. Organizar cache por tipo (busca, extração, classificação)

É como ter uma "memória" do sistema que lembra o que já foi feito!
"""

import json
import os
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import sys

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger_config import setup_logger


class CacheManager:
    """
    Gerenciador de cache para resultados de processamento
    
    Organiza o cache em diferentes tipos:
    - web_search: URLs encontradas para organizações
    - content_extraction: Conteúdo extraído dos websites
    - classification: Resultados de classificação IA
    - full_results: Resultados completos do pipeline
    """
    
    def __init__(self, cache_dir: str = "data/cache"):
        """
        Inicializa o gerenciador de cache
        
        Args:
            cache_dir: Diretório base para armazenar cache
        """
        self.logger, _ = setup_logger("cache_manager", log_to_file=True)
        
        self.cache_dir = Path(cache_dir)
        
        # Criar estrutura de diretórios
        self.cache_types = {
            'web_search': self.cache_dir / 'web_search',
            'content_extraction': self.cache_dir / 'content_extraction', 
            'classification': self.cache_dir / 'classification',
            'full_results': self.cache_dir / 'full_results'
        }
        
        # Criar diretórios se não existirem
        for cache_type, path in self.cache_types.items():
            path.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"💾 Cache Manager inicializado: {self.cache_dir}")
        self.logger.debug(f"Tipos de cache: {list(self.cache_types.keys())}")
    
    def _generate_cache_key(self, org_name: str) -> str:
        """
        Gera uma chave única para a organização
        
        Usa hash MD5 do nome normalizado para evitar problemas com:
        - Caracteres especiais
        - Nomes muito longos
        - Case sensitivity
        
        Args:
            org_name: Nome da organização
            
        Returns:
            Chave única para cache
        """
        # Normalizar nome (lowercase, sem espaços extras)
        normalized_name = org_name.lower().strip()
        
        # Gerar hash MD5
        hash_object = hashlib.md5(normalized_name.encode())
        cache_key = hash_object.hexdigest()
        
        self.logger.debug(f"Cache key para '{org_name}': {cache_key}")
        return cache_key
    
    def _get_cache_file_path(self, cache_type: str, org_name: str) -> Path:
        """
        Gera o caminho completo do arquivo de cache
        
        Args:
            cache_type: Tipo de cache (web_search, content_extraction, etc.)
            org_name: Nome da organização
            
        Returns:
            Caminho para o arquivo de cache
        """
        if cache_type not in self.cache_types:
            raise ValueError(f"Tipo de cache inválido: {cache_type}")
        
        cache_key = self._generate_cache_key(org_name)
        return self.cache_types[cache_type] / f"{cache_key}.json"
    
    def save_to_cache(self, cache_type: str, org_name: str, data: Dict[str, Any]) -> bool:
        """
        Salva dados no cache
        
        Args:
            cache_type: Tipo de cache
            org_name: Nome da organização
            data: Dados para salvar
            
        Returns:
            True se salvou com sucesso
        """
        try:
            cache_file = self._get_cache_file_path(cache_type, org_name)
            
            # Adicionar metadados e converter datetime para string
            cache_data = {
                'organization_name': org_name,
                'cache_type': cache_type,
                'timestamp': datetime.now().isoformat(),
                'data': self._serialize_data(data)
            }
            
            # Salvar arquivo JSON
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
            self.logger.debug(f"💾 Cache salvo: {cache_type} para {org_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao salvar cache {cache_type} para {org_name}: {str(e)}")
            return False
    
    def load_from_cache(self, cache_type: str, org_name: str) -> Optional[Dict[str, Any]]:
        """
        Carrega dados do cache
        
        Args:
            cache_type: Tipo de cache
            org_name: Nome da organização
            
        Returns:
            Dados do cache ou None se não encontrado
        """
        try:
            cache_file = self._get_cache_file_path(cache_type, org_name)
            
            if not cache_file.exists():
                self.logger.debug(f"📭 Cache não encontrado: {cache_type} para {org_name}")
                return None
            
            # Carregar arquivo JSON
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            self.logger.debug(f"📦 Cache carregado: {cache_type} para {org_name}")
            return cache_data['data']
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao carregar cache {cache_type} para {org_name}: {str(e)}")
            return None
    
    def is_cached(self, cache_type: str, org_name: str) -> bool:
        """
        Verifica se uma organização está no cache
        
        Args:
            cache_type: Tipo de cache
            org_name: Nome da organização
            
        Returns:
            True se está no cache
        """
        cache_file = self._get_cache_file_path(cache_type, org_name)
        return cache_file.exists()
    
    def get_cache_info(self, cache_type: str, org_name: str) -> Optional[Dict[str, Any]]:
        """
        Obtém informações sobre um item do cache (sem carregar os dados)
        
        Args:
            cache_type: Tipo de cache
            org_name: Nome da organização
            
        Returns:
            Metadados do cache ou None
        """
        try:
            cache_file = self._get_cache_file_path(cache_type, org_name)
            
            if not cache_file.exists():
                return None
            
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            return {
                'organization_name': cache_data.get('organization_name'),
                'cache_type': cache_data.get('cache_type'),
                'timestamp': cache_data.get('timestamp'),
                'file_size': cache_file.stat().st_size
            }
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao obter info do cache: {str(e)}")
            return None
    
    def clear_cache(self, cache_type: Optional[str] = None, org_name: Optional[str] = None) -> int:
        """
        Limpa cache
        
        Args:
            cache_type: Tipo específico para limpar (None = todos)
            org_name: Organização específica (None = todas)
            
        Returns:
            Número de arquivos removidos
        """
        removed_count = 0
        
        try:
            if cache_type is not None and org_name is not None:
                # Limpar cache específico de uma organização
                cache_file = self._get_cache_file_path(cache_type, org_name)
                if cache_file.exists():
                    cache_file.unlink()
                    removed_count = 1
                    self.logger.info(f"🗑️ Cache removido: {cache_type} para {org_name}")
            
            elif cache_type is not None:
                # Limpar todos os caches de um tipo
                cache_dir = self.cache_types[cache_type]
                for cache_file in cache_dir.glob("*.json"):
                    cache_file.unlink()
                    removed_count += 1
                self.logger.info(f"🗑️ Cache tipo {cache_type} limpo: {removed_count} arquivos")
            
            elif org_name is not None:
                # Limpar todos os tipos de cache para uma organização específica
                for cache_type_name, cache_dir in self.cache_types.items():
                    cache_file = self._get_cache_file_path(cache_type_name, org_name)
                    if cache_file.exists():
                        cache_file.unlink()
                        removed_count += 1
                        self.logger.debug(f"🗑️ Cache removido: {cache_type_name} para {org_name}")
                self.logger.info(f"🗑️ Todos os caches removidos para {org_name}: {removed_count} arquivos")
            
            else:
                # Limpar todo o cache
                for cache_dir in self.cache_types.values():
                    for cache_file in cache_dir.glob("*.json"):
                        cache_file.unlink()
                        removed_count += 1
                self.logger.info(f"🗑️ Todo cache limpo: {removed_count} arquivos")
        
        except Exception as e:
            self.logger.error(f"❌ Erro ao limpar cache: {str(e)}")
        
        return removed_count
    
    def get_cache_statistics(self) -> Dict[str, Any]:
        """
        Obtém estatísticas do cache
        
        Returns:
            Dicionário com estatísticas
        """
        stats = {
            'total_files': 0,
            'total_size_bytes': 0,
            'by_type': {}
        }
        
        try:
            for cache_type, cache_dir in self.cache_types.items():
                type_files = list(cache_dir.glob("*.json"))
                type_count = len(type_files)
                type_size = sum(f.stat().st_size for f in type_files)
                
                stats['by_type'][cache_type] = {
                    'files': type_count,
                    'size_bytes': type_size
                }
                
                stats['total_files'] += type_count
                stats['total_size_bytes'] += type_size
            
            # Converter bytes para MB
            stats['total_size_mb'] = stats['total_size_bytes'] / (1024 * 1024)
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao calcular estatísticas: {str(e)}")
        
        return stats
    
    def list_cached_organizations(self, cache_type: Optional[str] = None) -> List[str]:
        """
        Lista organizações que estão no cache
        
        Args:
            cache_type: Tipo específico (None = todos os tipos)
            
        Returns:
            Lista de nomes de organizações
        """
        organizations = set()
        
        try:
            cache_types_to_check = [cache_type] if cache_type else list(self.cache_types.keys())
            
            for ct in cache_types_to_check:
                cache_dir = self.cache_types[ct]
                
                for cache_file in cache_dir.glob("*.json"):
                    try:
                        with open(cache_file, 'r', encoding='utf-8') as f:
                            cache_data = json.load(f)
                            org_name = cache_data.get('organization_name')
                            if org_name:
                                organizations.add(org_name)
                    except Exception:
                        continue  # Pular arquivos corrompidos
        
        except Exception as e:
            self.logger.error(f"❌ Erro ao listar organizações: {str(e)}")
        
        return sorted(list(organizations))
    
    def _serialize_data(self, data: Any) -> Any:
        """
        Converte objetos datetime para strings para serialização JSON
        
        Args:
            data: Dados para serializar
            
        Returns:
            Dados serializáveis
        """
        if isinstance(data, dict):
            return {key: self._serialize_data(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self._serialize_data(item) for item in data]
        elif isinstance(data, datetime):
            return data.isoformat()
        else:
            return data


def main():
    """Função para testar o cache manager"""
    cache_manager = CacheManager()
    
    print("🧪 Testando Cache Manager")
    print("=" * 40)
    
    # Testar salvamento
    test_data = {
        'website_url': 'https://www.allianz.com',
        'search_method': 'wikipedia',
        'timestamp': datetime.now().isoformat()
    }
    
    print("💾 Salvando dados de teste...")
    success = cache_manager.save_to_cache('web_search', 'Allianz SE', test_data)
    print(f"Resultado: {'✅ Sucesso' if success else '❌ Falha'}")
    
    # Testar carregamento
    print("\n📦 Carregando dados do cache...")
    loaded_data = cache_manager.load_from_cache('web_search', 'Allianz SE')
    if loaded_data:
        print("✅ Dados carregados:")
        print(f"  URL: {loaded_data.get('website_url')}")
        print(f"  Método: {loaded_data.get('search_method')}")
    else:
        print("❌ Dados não encontrados")
    
    # Testar estatísticas
    print("\n📊 Estatísticas do cache:")
    stats = cache_manager.get_cache_statistics()
    print(f"  Total de arquivos: {stats['total_files']}")
    print(f"  Tamanho total: {stats['total_size_mb']:.2f} MB")
    
    # Listar organizações
    print("\n📋 Organizações no cache:")
    orgs = cache_manager.list_cached_organizations()
    for org in orgs:
        print(f"  • {org}")


if __name__ == "__main__":
    main()