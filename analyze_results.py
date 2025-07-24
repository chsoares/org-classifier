#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Análise rápida dos resultados do processamento completo
"""

import pandas as pd
from pathlib import Path

def analyze_results():
    """Analisa os resultados do processamento"""
    
    print("📊 ANÁLISE DOS RESULTADOS")
    print("=" * 70)
    
    # Carregar dados
    orgs_path = Path("data/results/organizations.csv")
    people_path = Path("data/results/people.csv")
    
    if not orgs_path.exists():
        print("❌ Arquivo organizations.csv não encontrado!")
        return
    
    if not people_path.exists():
        print("❌ Arquivo people.csv não encontrado!")
        return
    
    orgs_df = pd.read_csv(orgs_path)
    people_df = pd.read_csv(people_path)
    
    print(f"\n🏢 ORGANIZAÇÕES:")
    print("-" * 40)
    
    total_orgs = len(orgs_df)
    completed = len(orgs_df[orgs_df['processing_status'] == 'completed'])
    failed = len(orgs_df[orgs_df['processing_status'] == 'failed'])
    error = len(orgs_df[orgs_df['processing_status'] == 'error'])
    pending = len(orgs_df[orgs_df['processing_status'] == 'pending'])
    
    insurance_orgs = len(orgs_df[orgs_df['is_insurance'] == True])
    non_insurance_orgs = len(orgs_df[orgs_df['is_insurance'] == False])
    
    print(f"📋 Total de organizações: {total_orgs:,}")
    print(f"✅ Processadas com sucesso: {completed:,} ({completed/total_orgs*100:.1f}%)")
    print(f"❌ Falharam: {failed:,} ({failed/total_orgs*100:.1f}%)")
    print(f"💥 Erros: {error:,} ({error/total_orgs*100:.1f}%)")
    print(f"⏳ Pendentes: {pending:,} ({pending/total_orgs*100:.1f}%)")
    print()
    print(f"🏢 Seguradoras identificadas: {insurance_orgs:,} ({insurance_orgs/total_orgs*100:.1f}% do total, {insurance_orgs/completed*100:.1f}% das processadas)")
    print(f"🏭 Não-seguradoras: {non_insurance_orgs:,} ({non_insurance_orgs/total_orgs*100:.1f}% do total)")
    
    # Análise de erros por tipo
    print(f"\n❌ DETALHAMENTO DOS ERROS:")
    print("-" * 30)
    
    failed_orgs = orgs_df[orgs_df['processing_status'] == 'failed']
    if len(failed_orgs) > 0:
        error_counts = failed_orgs['error_message'].value_counts()
        for error_type, count in error_counts.head(5).items():
            if pd.notna(error_type):
                print(f"   • {error_type[:60]}{'...' if len(str(error_type)) > 60 else ''}: {count:,}")
    
    # Análise de pessoas
    print(f"\n👥 PESSOAS:")
    print("-" * 20)
    
    total_people = len(people_df)
    classified_people = len(people_df[people_df['is_insurance'].notna()])
    insurance_people = len(people_df[people_df['is_insurance'] == True])
    non_insurance_people = len(people_df[people_df['is_insurance'] == False])
    
    print(f"📋 Total de participantes: {total_people:,}")
    print(f"✅ Classificados: {classified_people:,} ({classified_people/total_people*100:.1f}%)")
    print(f"🏢 De seguradoras: {insurance_people:,} ({insurance_people/total_people*100:.1f}% do total)")
    print(f"🏭 De não-seguradoras: {non_insurance_people:,} ({non_insurance_people/total_people*100:.1f}% do total)")
    
    # Lista de seguradoras
    print(f"\n🏢 SEGURADORAS IDENTIFICADAS ({insurance_orgs}):")
    print("-" * 50)
    
    insurance_list = orgs_df[orgs_df['is_insurance'] == True].sort_values('participant_count', ascending=False)
    
    for idx, row in insurance_list.iterrows():
        org_name = row['organization_name']
        website = row['website_url'] if pd.notna(row['website_url']) else 'N/A'
        participants = row['participant_count']
        search_method = row['search_method'] if pd.notna(row['search_method']) else 'N/A'
        
        print(f"{idx+1:3d}. {org_name}")
        print(f"     👥 Participantes: {participants}")
        print(f"     🌐 Website: {website}")
        print(f"     🔍 Método: {search_method}")
        print()
    
    # Top organizações por número de participantes
    print(f"\n👥 TOP 10 ORGANIZAÇÕES POR PARTICIPANTES:")
    print("-" * 45)
    
    top_orgs = orgs_df.sort_values('participant_count', ascending=False).head(10)
    
    for idx, row in top_orgs.iterrows():
        org_name = row['organization_name']
        participants = row['participant_count']
        is_insurance = row['is_insurance']
        
        if pd.isna(is_insurance):
            status = "❓ Não classificada"
        elif is_insurance:
            status = "🏢 SEGURADORA"
        else:
            status = "🏭 Não-seguradora"
        
        print(f"{idx+1:2d}. {org_name} ({participants:,} participantes) - {status}")
    
    print(f"\n" + "=" * 70)
    print(f"✅ Análise concluída!")
    print(f"📁 Dados completos disponíveis em:")
    print(f"   • data/results/organizations.csv")
    print(f"   • data/results/people.csv")

if __name__ == "__main__":
    analyze_results()