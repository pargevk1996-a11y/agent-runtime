"""LLM provider interface and metered access.

A thin, provider-agnostic abstraction over chat-completion models (Anthropic,
OpenAI, local vLLM) so no single vendor is hardcoded. The gateway layer meters
every call — tokens, latency, dollar cost — into a ledger and enforces budgets.
"""
