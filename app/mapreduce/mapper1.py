#!/usr/bin/env python3
import sys
import re

def tokenize(text):
    return re.findall(r'\b\w+\b', text.lower())

for line in sys.stdin:
    try:
        doc_id, doc_title, doc_text = line.strip().split("\t", 2)
        tokens = tokenize(doc_text)
        doc_len = len(tokens)
        term_freq = {}
        for token in tokens:
            term_freq[token] = term_freq.get(token, 0) + 1
        for term, tf in term_freq.items():
            print(f"{term}\t{doc_id}\t{tf}\t{doc_len}")
    except Exception as e:
        continue
