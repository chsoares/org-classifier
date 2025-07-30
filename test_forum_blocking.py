#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Teste para verificar se URLs de fóruns, Baidu, Quora, etc. estão sendo bloqueadas
"""

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from scraping.web_searcher import WebSearcher

def test_forum_blocking():
    """Testa se URLs de fóruns e sites similares são bloqueadas"""
    
    searcher = WebSearcher()
    
    # URLs que DEVEM ser bloqueadas
    blocked_urls = [
        # Fóruns
        "https://forum.example.com/topic/123",
        "https://community.microsoft.com/discussion/456",
        "https://discuss.python.org/thread/789",
        
        # Q&A Sites
        "https://stackoverflow.com/questions/123456/how-to-code",
        "https://quora.com/What-is-the-best-way-to-learn-python",
        "https://answers.yahoo.com/question/index?qid=20230101000000AA12345",
        "https://superuser.com/questions/654321/windows-problem",
        
        # Baidu e sites chineses
        "https://baidu.com/search?q=test",
        "https://zhidao.baidu.com/question/123456.html",
        "https://tieba.baidu.com/p/7654321",
        
        # Reddit
        "https://reddit.com/r/programming/comments/abc123/test",
        "https://www.reddit.com/r/AskReddit/comments/def456/question",
        
        # Medium e blogs
        "https://medium.com/@user/article-title-123",
        "https://user.medium.com/story-title-456",
        
        # Sites de tradução
        "https://translate.google.com/translate?sl=en&tl=pt&u=example.com",
        "https://webcache.googleusercontent.com/search?q=cache:example.com",
        
        # Diretórios
        "https://yellowpages.com/business/example-company",
        "https://yelp.com/biz/example-restaurant",
        "https://zoominfo.com/c/example-corp/123456"
    ]
    
    # URLs que NÃO devem ser bloqueadas (sites legítimos)
    # Usar organizações reais para teste mais preciso
    allowed_test_cases = [
        ("https://microsoft.com", "Microsoft Corporation"),
        ("https://apple.com", "Apple Inc"),
        ("https://google.com", "Google LLC"),
        ("https://coldiretti.it", "Coldiretti"),
        ("https://allianz.com", "Allianz SE")
    ]
    
    print("🧪 Testando bloqueio de URLs de fóruns e sites similares\n")
    
    print("❌ URLs que DEVEM ser bloqueadas:")
    blocked_count = 0
    for url in blocked_urls:
        is_valid = searcher._is_valid_result(url, "Test Organization")
        status = "✅ BLOQUEADA" if not is_valid else "❌ PERMITIDA (ERRO!)"
        print(f"  {status}: {url}")
        if not is_valid:
            blocked_count += 1
    
    print(f"\n✅ URLs que NÃO devem ser bloqueadas:")
    allowed_count = 0
    for url, org_name in allowed_test_cases:
        is_valid = searcher._is_valid_result(url, org_name)
        status = "✅ PERMITIDA" if is_valid else "❌ BLOQUEADA (ERRO!)"
        print(f"  {status}: {url} (para {org_name})")
        if is_valid:
            allowed_count += 1
    
    print(f"\n📊 Resultados:")
    print(f"  URLs bloqueadas corretamente: {blocked_count}/{len(blocked_urls)}")
    print(f"  URLs permitidas corretamente: {allowed_count}/{len(allowed_test_cases)}")
    
    total_correct = blocked_count + allowed_count
    total_tests = len(blocked_urls) + len(allowed_test_cases)
    accuracy = (total_correct / total_tests) * 100
    
    print(f"  Precisão geral: {accuracy:.1f}% ({total_correct}/{total_tests})")
    
    if accuracy >= 90:
        print("🎉 Teste PASSOU! Filtros funcionando corretamente.")
    else:
        print("⚠️ Teste FALHOU! Alguns filtros precisam de ajuste.")

def test_domain_relevance():
    """Testa cálculo de relevância de domínios"""
    
    searcher = WebSearcher()
    
    test_cases = [
        # (domain, org_name, expected_high_relevance)
        ("coldiretti.it", "Coldiretti", True),
        ("microsoft.com", "Microsoft Corporation", True),
        ("forum.coldiretti.it", "Coldiretti", False),  # Fórum deve ter baixa relevância
        ("stackoverflow.com", "Stack Overflow", False),  # Site de fórum
        ("baidu.com", "Baidu Inc", False),  # Site de busca
        ("quora.com", "Quora Inc", False),  # Site de Q&A
    ]
    
    print("\n🧪 Testando cálculo de relevância de domínios\n")
    
    for domain, org_name, expected_high in test_cases:
        relevance = searcher._calculate_domain_relevance(domain, org_name)
        
        if expected_high:
            status = "✅ ALTA" if relevance >= 0.5 else "❌ BAIXA (ERRO!)"
        else:
            status = "✅ BAIXA" if relevance < 0.5 else "❌ ALTA (ERRO!)"
        
        print(f"  {status}: {domain} para '{org_name}' = {relevance:.2f}")

if __name__ == "__main__":
    test_forum_blocking()
    test_domain_relevance()