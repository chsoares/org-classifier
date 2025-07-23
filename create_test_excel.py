#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para criar arquivo Excel de teste com 20 linhas
"""

import pandas as pd
from pathlib import Path

def create_test_excel():
    """Cria arquivo Excel de teste com dados representativos"""
    
    print("📊 Criando arquivo Excel de teste...")
    
    # Dados de teste representativos com organizações conhecidas
    test_data = {
        'Type': ['Party overflow'] * 20,
        'Nominated by': [
            'Germany', 'Switzerland', 'USA', 'UK', 'France',
            'Germany', 'USA', 'Switzerland', 'UK', 'Netherlands',
            'USA', 'Germany', 'UK', 'Switzerland', 'France',
            'USA', 'Germany', 'UK', 'Switzerland', 'Netherlands'
        ],
        'Name': [
            'Hans Mueller', 'Anna Schmidt', 'John Smith', 'Mary Johnson', 'Pierre Dubois',
            'Klaus Weber', 'Sarah Wilson', 'Peter Meier', 'Emma Brown', 'Jan de Vries',
            'Michael Davis', 'Thomas Fischer', 'Sophie Taylor', 'Marco Rossi', 'Jean Martin',
            'David Miller', 'Andreas Bauer', 'Helen Clark', 'Stefan Huber', 'Lisa van Berg'
        ],
        'Home organization': [
            # Mix de seguradoras conhecidas e não-seguradoras
            'Allianz SE',                    # Seguradora alemã
            'Swiss Re',                      # Resseguradora suíça  
            'Microsoft Corporation',         # Tecnologia
            'Lloyd\'s of London',           # Seguradora britânica
            'AXA Group',                    # Seguradora francesa
            'Munich Re',                    # Resseguradora alemã
            'Harvard University',           # Universidade
            'Zurich Insurance Group',       # Seguradora suíça
            'University of Oxford',         # Universidade
            'ING Group',                    # Banco/Seguros holandês
            'Google LLC',                   # Tecnologia
            'Hannover Re',                  # Resseguradora alemã
            'Cambridge University',         # Universidade
            'Chubb Limited',               # Seguradora suíça
            'Sorbonne University',         # Universidade francesa
            'Apple Inc',                   # Tecnologia
            'ERGO Group',                  # Seguradora alemã
            'Imperial College London',     # Universidade
            'Baloise Group',              # Seguradora suíça
            'Radboud University'          # Universidade holandesa
        ]
    }
    
    # Criar DataFrame
    df = pd.DataFrame(test_data)
    
    # Criar diretório se não existir
    output_dir = Path('data/raw')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Salvar como Excel
    output_path = output_dir / 'cop29_test.xlsx'
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Criar sheet principal (simulando o formato original)
        df.to_excel(writer, sheet_name='Party overflow', index=False)
        
        # Criar sheets vazias que serão ignoradas (como no original)
        pd.DataFrame().to_excel(writer, sheet_name='temporary passes', index=False)
        pd.DataFrame().to_excel(writer, sheet_name='media', index=False)
        pd.DataFrame().to_excel(writer, sheet_name='parties', index=False)
    
    print(f"✅ Arquivo criado: {output_path}")
    print(f"📊 Dados: {len(df)} linhas")
    
    # Mostrar amostra dos dados
    print(f"\n📋 Organizações no teste:")
    unique_orgs = df['Home organization'].unique()
    for i, org in enumerate(unique_orgs, 1):
        print(f"  {i:2d}. {org}")
    
    print(f"\n💡 Para usar este arquivo, atualize config.yaml:")
    print(f"   excel_file: \"data/raw/cop29_test.xlsx\"")
    
    return str(output_path)

if __name__ == "__main__":
    create_test_excel()