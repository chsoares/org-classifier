#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Teste do Insurance Classifier
"""

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from classification.insurance_classifier import InsuranceClassifier

def test_classifier():
    """Teste completo do classificador de seguros"""
    
    print("🧪 TESTANDO INSURANCE CLASSIFIER")
    print("=" * 60)
    
    try:
        classifier = InsuranceClassifier()
        
        # Dataset de teste com organizações conhecidas
        test_dataset = [
            # INSURANCE COMPANIES (should return True)
            {
                'name': 'Allianz SE',
                'content': 'Allianz SE is a German multinational financial services company headquartered in Munich, Germany. Its core businesses are insurance and asset management. Allianz is the world\'s largest insurance company and the largest financial services company.',
                'expected': True,
                'category': 'Insurance'
            },
            {
                'name': 'Swiss Re',
                'content': 'Swiss Re Ltd is a Swiss multinational reinsurance company founded in 1863 and headquartered in Zurich, Switzerland. It is one of the world\'s largest reinsurers, providing reinsurance, insurance and other forms of insurance-based risk transfer.',
                'expected': True,
                'category': 'Reinsurance'
            },
            {
                'name': 'Lloyd\'s of London',
                'content': 'Lloyd\'s of London is an insurance and reinsurance market located in London, United Kingdom. Unlike most of its competitors in the industry, it is not an insurance company; rather, Lloyd\'s is a corporate body governed by the Lloyd\'s Act 1871 and subsequent Acts of Parliament.',
                'expected': True,
                'category': 'Insurance Market'
            },
            
            # NON-INSURANCE COMPANIES (should return False)
            {
                'name': 'Microsoft Corporation',
                'content': 'Microsoft Corporation is an American multinational technology corporation headquartered in Redmond, Washington. Microsoft\'s best-known software products are the Windows line of operating systems, the Microsoft Office suite, and the Internet Explorer and Edge web browsers.',
                'expected': False,
                'category': 'Technology'
            },
            {
                'name': 'Harvard University',
                'content': 'Harvard University is a private Ivy League research university in Cambridge, Massachusetts. Established in 1636, Harvard is the oldest institution of higher education in the United States and among the most prestigious in the world.',
                'expected': False,
                'category': 'Education'
            },
            {
                'name': 'Red Cross',
                'content': 'The International Red Cross and Red Crescent Movement is an international humanitarian movement with approximately 97 million volunteers, members and staff worldwide, which was founded to protect human life and health.',
                'expected': False,
                'category': 'Non-Profit'
            },
            {
                'name': 'JPMorgan Chase',
                'content': 'JPMorgan Chase & Co. is an American multinational investment bank and financial services holding company headquartered in New York City. It is the largest bank in the United States and the world\'s largest bank by market capitalization.',
                'expected': False,
                'category': 'Banking'
            }
        ]
        
        print(f"📋 Testando {len(test_dataset)} organizações:")
        print()
        
        results = []
        correct_predictions = 0
        
        for i, case in enumerate(test_dataset, 1):
            print(f"{i}. {case['name']} ({case['category']})")
            print(f"   Esperado: {'✅ Insurance' if case['expected'] else '❌ Not Insurance'}")
            
            # Classificar
            result = classifier.classify_organization(case['content'], case['name'])
            
            if result is not None:
                is_correct = result == case['expected']
                if is_correct:
                    correct_predictions += 1
                
                status_icon = "✅" if result else "❌"
                correct_icon = "✓" if is_correct else "✗"
                
                print(f"   Resultado: {status_icon} {'Insurance' if result else 'Not Insurance'} {correct_icon}")
                
                results.append({
                    'name': case['name'],
                    'expected': case['expected'],
                    'predicted': result,
                    'correct': is_correct,
                    'category': case['category']
                })
            else:
                print(f"   Resultado: ⚠️ CLASSIFICATION FAILED")
                results.append({
                    'name': case['name'],
                    'expected': case['expected'],
                    'predicted': None,
                    'correct': False,
                    'category': case['category']
                })
            
            print()
        
        # Estatísticas finais
        print("=" * 60)
        print("📊 RESULTADOS FINAIS:")
        print("=" * 60)
        
        total_tests = len(test_dataset)
        successful_classifications = len([r for r in results if r['predicted'] is not None])
        accuracy = (correct_predictions / successful_classifications * 100) if successful_classifications > 0 else 0
        
        print(f"Total de testes: {total_tests}")
        print(f"Classificações bem-sucedidas: {successful_classifications}")
        print(f"Classificações corretas: {correct_predictions}")
        print(f"Precisão: {accuracy:.1f}%")
        print()
        
        # Breakdown por categoria
        insurance_results = [r for r in results if r['expected'] == True]
        non_insurance_results = [r for r in results if r['expected'] == False]
        
        insurance_correct = len([r for r in insurance_results if r['correct']])
        non_insurance_correct = len([r for r in non_insurance_results if r['correct']])
        
        print("📈 BREAKDOWN POR CATEGORIA:")
        print(f"Insurance companies: {insurance_correct}/{len(insurance_results)} corretas")
        print(f"Non-insurance companies: {non_insurance_correct}/{len(non_insurance_results)} corretas")
        print()
        
        # Estatísticas da API
        stats = classifier.get_classification_stats()
        print("💰 ESTATÍSTICAS DA API:")
        print(f"Total de requests: {stats['api_usage']['total_requests']}")
        print(f"Custo total: ${stats['api_usage']['total_cost']:.4f}")
        print(f"Modelo usado: {stats['model_used']}")
        print()
        
        # Teste de validação por palavras-chave
        print("🔍 TESTE DE VALIDAÇÃO POR PALAVRAS-CHAVE:")
        for case in test_dataset[:3]:  # Apenas primeiros 3
            keyword_result = classifier.validate_with_keywords(case['content'], case['name'])
            keyword_icon = "✅" if keyword_result else "❌"
            print(f"{case['name']}: {keyword_icon} {'Keywords found' if keyword_result else 'No keywords'}")
        
        print()
        print("✅ Teste concluído!")
        
        # Verificar se o teste passou
        if accuracy >= 80:  # 80% de precisão mínima
            print("🎉 TESTE PASSOU - Precisão aceitável!")
        else:
            print("⚠️ TESTE FALHOU - Precisão baixa!")
        
    except Exception as e:
        print(f"❌ ERRO NO TESTE: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_classifier()