# Meta Reversa para Planejamento de Aberturas em Operações de Importação

Pipeline de dados em Python que converte metas mensais de faturamento em um plano operacional: quantos processos de importação devem ser abertos a cada mês, por modal de transporte e tipo de serviço, para atingir os objetivos de receita.

**Trabalho de Conclusão de Curso** - MBA em Data Science e Analytics (USP/Esalq)  
**Autor:** Pedro Henrique Scholles

---

## Visão Geral

O modelo de **meta reversa** parte das metas de receita e trabalha de trás para frente:

1. Distribui a meta por modal (Aéreo, Marítimo, Rodoviário) com base no histórico
2. Calcula o lead time mediano de cada modal para determinar **quando** abrir os processos
3. Divide a meta pelo ticket médio para determinar **quantos** processos abrir
4. Valida o modelo com backtesting out-of-sample (treino 2023-2024, teste 2025)
5. Executa análise de sensibilidade sobre ticket médio e lead time

## Arquitetura do Pipeline

```
data/db_base_processos.csv ──┐
                              ├──▶ [0] Data Wrangling
data/db_metas.csv ───────────┘          │
                                        ▼
                                [1] Análise Histórica
                                   (lead times, tickets, shares)
                                        │
                                        ▼
                                [2] Distribuição Temporal
                                   (meta → mês de abertura)
                                        │
                                        ▼
                                [3] Meta Reversa
                                   (meta → nº de processos)
                                        │
                                        ▼
                                [4] Backtesting
                                   (validação out-of-sample)
                                        │
                                        ▼
                                [5] Análise de Sensibilidade
                                   (variações de ticket e lead time)
                                        │
                                        ▼
                                  outputs/ (9 CSVs)
```

## Estrutura do Projeto

```
projeto_meta_reversa/
├── main.py                     # Orquestração do pipeline
├── requirements.txt            # Dependências
├── src/
│   ├── __init__.py
│   ├── data_wrangling.py       # ETL e limpeza de dados
│   ├── analise_historica.py    # Cálculo de métricas históricas
│   ├── distribuicao_temporal.py# Planejamento temporal de aberturas
│   ├── meta_reversa.py         # Conversão de meta em processos
│   ├── backtesting.py          # Validação do modelo (2025)
│   └── analise_sensibilidade.py# Sensibilidade a variações
├── data/
│   ├── db_base_processos.csv   # Histórico de processos
│   └── db_metas.csv            # Metas mensais 2026
└── outputs/                    # Resultados gerados
```

## Como Executar

**Pré-requisitos:** Python 3.9+

```bash
# Instalar dependências
pip install -r requirements.txt

# Executar o pipeline
python main.py
```

O pipeline imprime logs detalhados no console e salva os resultados em `outputs/`.

## Principais Saídas

| Arquivo | Descrição |
|---------|-----------|
| `tabela_aberturas.csv` | **Entregável principal:** processos a abrir por mês/modal/serviço |
| `recomendacoes_detalhadas.csv` | Detalhamento com ticket e valor esperado |
| `backtesting_mensal.csv` | Comparação previsto vs. real (2025) |
| `performance_backtesting.csv` | Métricas de validação (MAPE, BIAS, Hit Rate) |
| `sensibilidade_ticket.csv` | Impacto de variações no ticket médio |
| `sensibilidade_lead_time.csv` | Impacto de variações no lead time |
| `lead_times.csv` | Lead times históricos por modal |
| `ticket_medio_modal_servico.csv` | Ticket médio por modal e serviço |
| `share_modal_por_servico.csv` | Distribuição de modais por serviço |

## Metodologia

- **Tratamento de outliers:** Método IQR (1.5 × amplitude interquartil) por grupo modal+serviço
- **Lead time:** Mediana por modal, filtrado a ≤ 365 dias
- **Backtesting:** Treino em 2023-2024, teste em 2025 (sem data leakage)
- **Sensibilidade:** Variações de -20% a +20% no ticket e -50% a +50% no lead time

## Dependências

- `pandas` 3.0.2
- `numpy` 2.4.4
