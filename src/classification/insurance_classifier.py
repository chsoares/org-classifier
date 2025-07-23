#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Insurance Classifier - Sistema de classificação AI para indústria de seguros

Este módulo é responsável por:
1. Classificar organizações como relacionadas ou não à indústria de seguros
2. Usar prompts específicos para identificação de seguros
3. Garantir respostas apenas "Yes" ou "No"
4. Implementar retry logic e rate limiting
5. Integrar com OpenRouter API
"""

import json
import os
import time
from datetime import datetime
from typing import Optional, Dict, List
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

# Adicionar src ao path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger_config import setup_logger
from utils.config_manager import config_manager

# Carregar variáveis de ambiente
load_dotenv()


class InsuranceClassificationError(Exception):
    """Erro específico da classificação de seguros"""
    pass


class SystemicClassifierError(Exception):
    """Erro sistêmico que impede o funcionamento do classificador"""
    pass


class OpenRouterClient:
    """
    Cliente para comunicação com OpenRouter API
    Adaptado especificamente para classificação de seguros
    """
    
    def __init__(self):
        self.logger, _ = setup_logger("openrouter_client", log_to_file=True)
        
        # Configurações da API
        self.api_key = os.getenv("OPENROUTER_API_KEY")
        self.base_url = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
        self.model = os.getenv("OPENROUTER_MODEL", "google/gemini-2.0-flash-001")
        
        # Configurações de retry e rate limiting
        self.max_retries = int(os.getenv("MAX_RETRIES", 3))
        self.retry_delay = int(os.getenv("RETRY_DELAY", 2))
        self.rate_limit_delay = float(os.getenv("RATE_LIMIT_DELAY", 1.0))
        
        # Métricas
        self.cost_per_request = 0.0001  # Custo estimado por request
        self.total_requests = 0
        self.total_cost = 0.0
        self.last_request_time = 0
        
        if not self.api_key:
            raise SystemicClassifierError("OPENROUTER_API_KEY não encontrada nas variáveis de ambiente")
        
        self.logger.info("🤖 OpenRouter Client inicializado")
        self.logger.debug(f"Modelo: {self.model}, Max retries: {self.max_retries}")
    
    def _apply_rate_limiting(self):
        """
        Aplica rate limiting entre requisições
        """
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last
            self.logger.debug(f"Rate limiting: aguardando {sleep_time:.2f}s")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def call_api(self, prompt: str, company_name: str = "") -> Optional[str]:
        """
        Faz chamada à API com retry logic e tratamento de erros
        
        Args:
            prompt: Prompt para classificação
            company_name: Nome da empresa (para logs)
            
        Returns:
            Resposta da API ou None em caso de erro
        """
        self._apply_rate_limiting()
        
        self.total_requests += 1
        self.total_cost += self.cost_per_request
        
        start_time = datetime.now()
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/org-insurance-classifier",
            "X-Title": "Organization Insurance Classifier"
        }
        
        data = {
            "model": self.model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                }
            ],
            "temperature": 0.1,  # Baixa temperatura para respostas consistentes
            "max_tokens": 10     # Limite baixo para forçar respostas concisas
        }
        
        for attempt in range(self.max_retries):
            try:
                self.logger.debug(f"Tentativa {attempt + 1} para {company_name or 'organização'}")
                
                response = requests.post(
                    f"{self.base_url}/chat/completions",
                    headers=headers,
                    json=data,
                    timeout=30
                )
                
                # Tratamento de erros específicos
                if response.status_code == 402:
                    raise SystemicClassifierError("Créditos da API OpenRouter esgotados")
                elif response.status_code == 401:
                    raise SystemicClassifierError("Chave da API OpenRouter inválida")
                elif response.status_code == 429:
                    self.logger.warning("Rate limit atingido, aguardando...")
                    time.sleep(self.retry_delay * (2 ** attempt))
                    continue
                
                response.raise_for_status()
                
                # Extrair resposta
                response_data = response.json()
                content = response_data["choices"][0]["message"]["content"]
                
                # Log de métricas
                elapsed_time = (datetime.now() - start_time).total_seconds()
                self.logger.debug(
                    f"API call sucesso: {company_name or 'org'} - "
                    f"tempo={elapsed_time:.2f}s, custo=${self.cost_per_request:.4f}"
                )
                
                return content.strip()
                
            except SystemicClassifierError:
                raise  # Re-lança erros sistêmicos
            except requests.exceptions.RequestException as e:
                self.logger.warning(
                    f"Tentativa {attempt + 1} falhou para {company_name or 'organização'}: {str(e)}"
                )
                if attempt < self.max_retries - 1:
                    sleep_time = self.retry_delay * (2 ** attempt)
                    self.logger.debug(f"Aguardando {sleep_time}s antes da próxima tentativa")
                    time.sleep(sleep_time)
                continue
            except Exception as e:
                self.logger.error(f"Erro inesperado na API: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                continue
        
        self.logger.error(f"Todas as tentativas falharam para {company_name or 'organização'}")
        return None
    
    def get_usage_stats(self) -> Dict[str, float]:
        """
        Retorna estatísticas de uso da API
        
        Returns:
            Dict com estatísticas
        """
        return {
            'total_requests': self.total_requests,
            'total_cost': self.total_cost,
            'avg_cost_per_request': self.cost_per_request
        }


class InsuranceClassifier:
    """
    Classificador específico para indústria de seguros
    Determina se uma organização está relacionada ao setor de seguros
    """
    
    def __init__(self):
        self.logger, _ = setup_logger("insurance_classifier", log_to_file=True)
        self.api_client = OpenRouterClient()
        
        self.logger.info("🏢 Insurance Classifier inicializado")
        
        # Palavras-chave relacionadas a seguros para validação
        self.insurance_keywords = [
            # Inglês
            'insurance', 'insurer', 'reinsurance', 'reinsurer', 'underwriting',
            'actuarial', 'claims', 'policy', 'premium', 'coverage', 'liability',
            'life insurance', 'health insurance', 'auto insurance', 'property insurance',
            'casualty', 'annuity', 'pension', 'risk management', 'broker', 'brokerage',
            # Português
            'seguro', 'seguradora', 'resseguro', 'resseguradora', 'subscrição',
            'atuarial', 'sinistro', 'apólice', 'prêmio', 'cobertura', 'responsabilidade',
            'seguro de vida', 'seguro saúde', 'seguro auto', 'seguro propriedade',
            'corretora', 'corretagem', 'gestão de risco',
            # Francês
            'assurance', 'assureur', 'réassurance', 'courtier', 'prime',
            # Alemão
            'versicherung', 'versicherer', 'rückversicherung', 'makler',
            # Espanhol
            'seguro', 'aseguradora', 'reaseguro', 'corredor', 'prima'
        ]
    
    def create_classification_prompt(self, content: str, org_name: str) -> str:
        """
        Cria prompt específico para classificação de seguros
        
        Args:
            content: Conteúdo da organização
            org_name: Nome da organização
            
        Returns:
            Prompt formatado
        """
        prompt = f"""You are an expert insurance industry analyst. Your task is to determine if an organization is related to the insurance industry.

INSURANCE INDUSTRY includes:
- Insurance companies (life, health, auto, property, casualty, etc.)
- Reinsurance companies
- Insurance brokers and agents
- Insurance technology companies (InsurTech)
- Actuarial consulting firms
- Claims management companies
- Risk management firms focused on insurance
- Insurance regulatory bodies
- Insurance associations and organizations

NOT INSURANCE INDUSTRY:
- Banks and financial services (unless specifically insurance-focused)
- Investment firms
- General consulting companies
- Technology companies (unless specifically InsurTech)
- Healthcare providers
- Government agencies (unless insurance regulatory)
- Educational institutions
- Any other non-insurance business

Organization: {org_name}
Content: {content}

Based on the organization name and content provided, is this organization part of the insurance industry?

Respond with ONLY "Yes" or "No". No explanations, no additional text."""

        return prompt
    
    def _clean_response(self, response: str) -> str:
        """
        Limpa resposta da API para garantir apenas "Yes" ou "No"
        
        Args:
            response: Resposta bruta da API
            
        Returns:
            "Yes", "No" ou string vazia se inválida
        """
        if not response:
            return ""
        
        # Limpar e normalizar
        cleaned = response.strip().lower()
        
        # Remover pontuação
        cleaned = cleaned.replace('.', '').replace(',', '').replace('!', '').replace('?', '')
        
        # Verificar respostas válidas
        if cleaned in ['yes', 'sim', 'sí', 'oui', 'ja']:
            return "Yes"
        elif cleaned in ['no', 'não', 'non', 'nein']:
            return "No"
        
        # Verificar se contém yes/no no início
        if cleaned.startswith('yes'):
            return "Yes"
        elif cleaned.startswith('no'):
            return "No"
        
        # Log resposta inválida
        self.logger.warning(f"Resposta inválida da API: '{response}' -> '{cleaned}'")
        return ""
    
    def classify_organization(self, content: str, org_name: str) -> Optional[bool]:
        """
        Classifica uma organização como relacionada ou não ao setor de seguros
        
        Args:
            content: Conteúdo extraído da organização
            org_name: Nome da organização
            
        Returns:
            True se é relacionada a seguros, False se não, None se erro
        """
        self.logger.info(f"🏢 Classificando: {org_name}")
        
        # Validação de entrada
        if not content or len(content.strip()) < 20:
            self.logger.warning(f"Conteúdo insuficiente para {org_name}")
            return None
        
        # Criar prompt
        prompt = self.create_classification_prompt(content, org_name)
        
        # Chamar API
        response = self.api_client.call_api(prompt, org_name)
        
        if not response:
            self.logger.error(f"❌ Falha na API para {org_name}")
            return None
        
        # Limpar resposta
        cleaned_response = self._clean_response(response)
        
        if cleaned_response == "Yes":
            self.logger.success(f"✅ {org_name} -> INSURANCE")
            return True
        elif cleaned_response == "No":
            self.logger.info(f"❌ {org_name} -> NOT INSURANCE")
            return False
        else:
            self.logger.error(f"⚠️ Resposta inválida para {org_name}: '{response}'")
            return None
    
    def classify_batch(self, organizations: List[Dict[str, str]]) -> List[Dict[str, any]]:
        """
        Classifica múltiplas organizações em lote
        
        Args:
            organizations: Lista de dicts com 'name' e 'content'
            
        Returns:
            Lista de resultados com classificações
        """
        self.logger.info(f"🏢 Iniciando classificação em lote: {len(organizations)} organizações")
        
        start_time = datetime.now()
        initial_stats = self.api_client.get_usage_stats()
        
        results = []
        
        for i, org in enumerate(organizations, 1):
            org_name = org.get('name', f'Organização {i}')
            content = org.get('content', '')
            
            self.logger.debug(f"Processando {i}/{len(organizations)}: {org_name}")
            
            classification = self.classify_organization(content, org_name)
            
            result = {
                'name': org_name,
                'content': content,
                'is_insurance': classification,
                'classification_status': 'success' if classification is not None else 'failed',
                'timestamp': datetime.now().isoformat()
            }
            
            results.append(result)
            
            # Pequena pausa entre classificações
            if i < len(organizations):
                time.sleep(0.5)
        
        # Estatísticas finais
        end_time = datetime.now()
        final_stats = self.api_client.get_usage_stats()
        
        elapsed_time = (end_time - start_time).total_seconds()
        batch_cost = final_stats['total_cost'] - initial_stats['total_cost']
        
        success_count = sum(1 for r in results if r['classification_status'] == 'success')
        insurance_count = sum(1 for r in results if r['is_insurance'] is True)
        
        self.logger.info(
            f"📊 Classificação em lote concluída:\n"
            f"  - Total processado: {len(organizations)}\n"
            f"  - Sucessos: {success_count}\n"
            f"  - Falhas: {len(organizations) - success_count}\n"
            f"  - Organizações de seguros: {insurance_count}\n"
            f"  - Tempo total: {elapsed_time:.2f}s\n"
            f"  - Custo total: ${batch_cost:.4f}\n"
            f"  - Custo médio: ${(batch_cost/len(organizations)):.4f}"
        )
        
        return results
    
    def validate_with_keywords(self, content: str, org_name: str) -> bool:
        """
        Validação adicional usando palavras-chave
        Pode ser usada como fallback ou validação cruzada
        
        Args:
            content: Conteúdo da organização
            org_name: Nome da organização
            
        Returns:
            True se encontrou palavras-chave de seguros
        """
        text_to_check = f"{org_name} {content}".lower()
        
        keyword_matches = [
            keyword for keyword in self.insurance_keywords
            if keyword in text_to_check
        ]
        
        if keyword_matches:
            self.logger.debug(f"Palavras-chave encontradas em {org_name}: {keyword_matches[:3]}")
            return True
        
        return False
    
    def get_classification_stats(self) -> Dict[str, any]:
        """
        Retorna estatísticas do classificador
        
        Returns:
            Dict com estatísticas
        """
        api_stats = self.api_client.get_usage_stats()
        
        return {
            'api_usage': api_stats,
            'keywords_count': len(self.insurance_keywords),
            'model_used': self.api_client.model
        }


def main():
    """Função para testar o classificador"""
    classifier = InsuranceClassifier()
    
    # Casos de teste
    test_cases = [
        {
            'name': 'Allianz SE',
            'content': 'Allianz SE is a German multinational financial services company headquartered in Munich, Germany. Its core businesses are insurance and asset management. Allianz is the world\'s largest insurance company.'
        },
        {
            'name': 'Microsoft Corporation',
            'content': 'Microsoft Corporation is an American multinational technology corporation headquartered in Redmond, Washington. Microsoft\'s best-known software products are the Windows line of operating systems.'
        },
        {
            'name': 'Swiss Re',
            'content': 'Swiss Re Ltd is a Swiss multinational reinsurance company founded in 1863 and headquartered in Zurich, Switzerland. It is one of the world\'s largest reinsurers.'
        }
    ]
    
    print("🧪 TESTANDO INSURANCE CLASSIFIER")
    print("=" * 50)
    
    for case in test_cases:
        print(f"\n🏢 Testando: {case['name']}")
        result = classifier.classify_organization(case['content'], case['name'])
        
        if result is True:
            print(f"   ✅ INSURANCE")
        elif result is False:
            print(f"   ❌ NOT INSURANCE")
        else:
            print(f"   ⚠️ CLASSIFICATION FAILED")
    
    # Estatísticas
    stats = classifier.get_classification_stats()
    print(f"\n📊 Estatísticas:")
    print(f"   - Requests: {stats['api_usage']['total_requests']}")
    print(f"   - Custo: ${stats['api_usage']['total_cost']:.4f}")


if __name__ == "__main__":
    main()