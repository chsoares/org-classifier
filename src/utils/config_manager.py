#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import yaml
from pathlib import Path
from dotenv import load_dotenv


class ConfigManager:
    """
    Gerenciador de configuração que combina arquivo YAML e variáveis de ambiente
    """
    
    def __init__(self, config_file="config.yaml"):
        self.project_root = self._find_project_root()
        self.config_file = self.project_root / config_file
        
        # Carregar variáveis de ambiente
        env_file = self.project_root / ".env"
        if env_file.exists():
            load_dotenv(env_file)
        
        # Carregar configuração YAML
        self.config = self._load_yaml_config()
    
    def _find_project_root(self):
        """Encontra o diretório raiz do projeto"""
        current_dir = Path.cwd()
        
        while current_dir != current_dir.parent:
            if (current_dir / "config.yaml").exists():
                return current_dir
            current_dir = current_dir.parent
        
        # Se não encontrar, usar diretório atual
        return Path.cwd()
    
    def _load_yaml_config(self):
        """Carrega configuração do arquivo YAML"""
        if not self.config_file.exists():
            raise FileNotFoundError(f"Arquivo de configuração não encontrado: {self.config_file}")
        
        with open(self.config_file, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    
    def get(self, key, default=None):
        """
        Obtém valor de configuração, priorizando variáveis de ambiente
        
        Args:
            key: Chave da configuração (pode usar notação de ponto: 'section.key')
            default: Valor padrão se não encontrar
        """
        # Primeiro, tentar variável de ambiente
        env_key = key.upper().replace(".", "_")
        env_value = os.getenv(env_key)
        if env_value is not None:
            return env_value
        
        # Depois, tentar configuração YAML
        keys = key.split(".")
        value = self.config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_openrouter_config(self):
        """Retorna configuração específica do OpenRouter"""
        return {
            "api_key": self.get("OPENROUTER_API_KEY"),
            "base_url": self.get("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"),
            "model": self.get("classification.model", "google/gemini-2.0-flash-001"),
            "max_tokens": self.get("classification.max_tokens", 100),
            "temperature": self.get("classification.temperature", 0.1)
        }
    
    def get_scraping_config(self):
        """Retorna configuração específica do web scraping"""
        return {
            "timeout": self.get("scraping.timeout", 10),
            "max_retries": self.get("scraping.max_retries", 3),
            "retry_delay": self.get("scraping.retry_delay", 2),
            "max_content_length": self.get("scraping.max_content_length", 2000),
            "user_agent": self.get("scraping.user_agent", "Mozilla/5.0 (compatible; OrgClassifier/1.0)")
        }
    
    def get_data_config(self):
        """Retorna configuração específica dos dados"""
        return {
            "excel_file": self.get("data.excel_file", "COP29_FLOP_On-site.xlsx"),
            "excluded_sheets": self.get("data.excluded_sheets", ["temporary passes", "media"]),
            "required_columns": self.get("data.required_columns", ["Nominated by", "Name", "Home Organization"])
        }
    
    def get_cache_config(self):
        """Retorna configuração específica do cache"""
        return {
            "enabled": self.get("cache.enabled", True),
            "max_age_days": self.get("cache.max_age_days", 30),
            "cache_directory": self.project_root / self.get("cache.cache_directory", "data/cache")
        }
    
    def get_fuzzy_config(self):
        """Retorna configuração específica do fuzzy matching"""
        return {
            "threshold": self.get("fuzzy_matching.threshold", 85),
            "max_suggestions": self.get("fuzzy_matching.max_suggestions", 5)
        }


# Instância global do gerenciador de configuração
config_manager = ConfigManager()