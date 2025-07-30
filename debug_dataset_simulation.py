#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para simular exatamente o que o dataset completo faz
"""

import sys
import pandas as pd
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from main import MasterOrchestrator


def simulate_dataset_processing():
    """Simula o processamento do dataset completo com as primeiras organizações"""
    
    print("🎯 SIMULANDO PROCESSAMENTO DO DATASET COMPLETO")
    print("=" * 60)
    
    # Criar lista das primeiras organizações que você mostrou no log
    test_organizations = [
        "Presidential Court",
        "World Bank Group", 
        "University Climate Network",
        "Riyadh",
        "Ministry of Foreign Affairs",
        "U.S. Department of State",
        "Jordan",
        "COP28",
        "UNICEF",
        "Asian Development Bank"
    ]
    
    print(f"📋 Testando com {len(test_organizations)} organizações")
    
    # Criar DataFrame simulando o que o MasterOrchestrator faz
    organizations_df = pd.DataFrame({
        'organization_name': test_organizations,
        'is_insurance': None,
        'website_url': None,
        'search_method': None,
        'content_source': None,
        'processing_status': 'pending',
        'error_message': None,
        'processed_at': None
    })
    
    print(f"✅ DataFrame criado com {len(organizations_df)} organizações")
    
    # Inicializar MasterOrchestrator
    orchestrator = MasterOrchestrator()
    
    print(f"\n🔄 Iniciando processamento simulado...")
    print("-" * 60)
    
    # Simular exatamente o que _run_classification_pipeline faz
    total_orgs = len(organizations_df)
    processed_count = 0
    
    for idx, row in organizations_df.iterrows():
        org_name = row['organization_name']
        
        print(f"\n[{idx+1:3d}/{total_orgs}] Processando: {org_name}")
        
        try:
            # Esta é a mesma chamada que o MasterOrchestrator faz
            result = orchestrator.main_processor.process_single_organization(org_name)
            
            # Mostrar se usou cache ou não
            processing_time = result.get('total_time_seconds', 0)
            
            if processing_time < 1.0:
                print(f"  ✅ Cache usado (tempo: {processing_time:.3f}s)")
            else:
                print(f"  ❌ Cache NÃO usado (tempo: {processing_time:.3f}s)")
            
            # Mostrar resultado
            if result['success']:
                classification = "SEGURADORA" if result['is_insurance'] else "NÃO-SEGURADORA"
                print(f"  📊 Resultado: {classification}")
                processed_count += 1
            else:
                print(f"  ❌ FALHA: {result['error_stage']}")
                
        except Exception as e:
            print(f"  💥 ERRO: {str(e)}")
            continue
    
    print(f"\n📊 Processamento simulado concluído")
    print(f"✅ Sucessos: {processed_count}/{total_orgs}")


def main():
    """Função principal"""
    simulate_dataset_processing()


if __name__ == "__main__":
    main()