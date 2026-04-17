# Meta Reversa para Planejamento de Aberturas em Operações de Importação

Pipeline em Python que converte metas mensais de faturamento em um plano operacional: quantos processos de importação devem ser abertos a cada mês, por modal de transporte e tipo de serviço, para atingir os objetivos de receita.

> Invertendo a lógica tradicional de *forecasting*, o modelo parte da meta financeira futura e calcula retrospectivamente a ação comercial necessária no presente.

**Trabalho de Conclusão de Curso** — MBA em Data Science e Analytics (USP/Esalq)
**Autor:** Pedro Henrique Scholles

---

## Funcionalidades

- **Cálculo reverso de metas operacionais** a partir de metas financeiras, considerando lead times e tickets históricos por combinação de modal e serviço
- **Distribuição temporal de aberturas** com base no lead time mediano de cada modal (Aéreo, Marítimo, Rodoviário)
- **Validação out-of-sample** com dois splits temporais (simulação reversa com metas ex-post)
- **Análise de sensibilidade paramétrica** sobre ticket médio e lead time
- **Tratamento robusto de outliers** via IQR por combinação modal×serviço

## Como Executar

**Pré-requisitos:** Python 3.10+ e `pandas >= 2.2, < 3.0`.

```bash
# Clonar o repositório
git clone https://github.com/phscholles/projeto-meta-reversa.git
cd projeto-meta-reversa

# Criar ambiente virtual e instalar dependências
python -m venv venv
source venv/bin/activate  # Linux/Mac
.\venv\Scripts\Activate.ps1  # Windows PowerShell
pip install -r requirements.txt

# Executar o pipeline
python main.py
```

O pipeline imprime logs detalhados no console e salva os resultados em `outputs/`.

## Arquitetura

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
                                     outputs/
```

## Estrutura do Projeto

```
projeto_meta_reversa/
├── main.py                          # Orquestração do pipeline
├── requirements.txt                 # Dependências
├── src/
│   ├── data_wrangling.py            # ETL e limpeza de dados
│   ├── analise_historica.py         # Métricas históricas
│   ├── distribuicao_temporal.py     # Planejamento temporal de aberturas
│   ├── meta_reversa.py              # Conversão de meta em processos
│   ├── backtesting.py               # Validação out-of-sample
│   └── analise_sensibilidade.py     # Análise de cenários
├── data/                            # Histórico de processos e metas
└── outputs/                         # Resultados gerados
```

## Entregáveis

O pipeline gera um conjunto de arquivos CSV em `outputs/`:

| Arquivo | Descrição |
|---|---|
| `tabela_aberturas.csv` | **Entregável principal** — processos a abrir por mês, modal e serviço |
| `processos_por_mes_meta.csv` | Processos por mês-meta (perspectiva de faturamento) |
| `recomendacoes_detalhadas.csv` | Detalhamento linha a linha com rastreabilidade |
| `backtesting_mensal.csv` | Comparação mensal processos reais vs. recomendados |
| `performance_backtesting.csv` | Métricas de validação (MAPE, BIAS, HIT RATE) |
| `sensibilidade_ticket.csv` | Impacto de variações no ticket médio |
| `sensibilidade_lead_time.csv` | Impacto de variações no lead time |
| `lead_times.csv` | Lead times históricos por modal |
| `ticket_medio_modal_servico.csv` | Ticket médio por combinação |
| `share_modal_por_servico.csv` | Distribuição de modais por serviço |

## Metodologia

- **Tratamento de outliers:** Método IQR (1.5 × amplitude interquartil) aplicado por combinação modal+serviço, preservando a representatividade de segmentos com comportamentos distintos.
- **Ticket médio:** Média aritmética por (modal, serviço) após remoção de outliers, com amostra mínima de 10 observações por combinação; combinações raras usam o ticket médio do modal como fallback.
- **Lead time:** Mediana por modal, filtrada a 0 ≤ dias ≤ 365.
- **Share de modal por serviço:** Proporção calculada pelo faturamento histórico, para distribuir metas financeiras conforme o peso econômico de cada modal.
- **Backtesting out-of-sample (simulação reversa):** Dois splits temporais com métricas calculadas estritamente sobre o conjunto de treino. O faturamento real do ano de teste é tratado como "metas ex-post" e o modelo reverso completo é aplicado retrospectivamente para comparação com processos efetivamente abertos.

## Dependências

- Python ≥ 3.10
- pandas ≥ 2.2, < 3.0

## Licença

Projeto acadêmico. Uso restrito a fins educacionais e de pesquisa.
