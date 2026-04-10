"""
Etapa 4: Backtesting
Valida o modelo comparando previsões com faturamento real de 2025.
Metodologia out-of-sample: métricas calculadas exclusivamente com 2023-2024 (treino)
e aplicadas sobre 2025 (teste), evitando vazamento de dados.
"""

import pandas as pd
import numpy as np
from src import analise_historica


def executar(df, metricas_completas):
    print("\n" + "="*80)
    print("ETAPA 4: BACKTESTING")
    print("="*80)

    # Separar treino (2023-2024) e teste (2025)
    df_treino = df[df['ano_faturamento'] < 2025].copy()
    df_teste  = df[df['ano_faturamento'] == 2025].copy()

    print(f"Treino: {len(df_treino):,} processos (2023-2024)")
    print(f"Teste : {len(df_teste):,} processos (2025)")

    # Recalcular métricas exclusivamente com dados de treino (out-of-sample correto)
    print("\nRecalculando métricas somente com dados de treino (2023-2024)...")
    metricas_treino = analise_historica.executar(df_treino, verbose=False)

    # Faturamento real mensal 2025
    fat_real_mensal = df_teste.groupby('mes_faturamento')['vlr_faturamento'].sum()

    print("\nFaturamento Real 2025 (por mês):")
    for mes, valor in fat_real_mensal.items():
        print(f"  Mes {mes:02d}: R$ {valor:,.2f}")
    print(f"\nTotal 2025: R$ {fat_real_mensal.sum():,.2f}")

    # Processos reais que geraram faturamento em 2025, agrupados por mês/modal/serviço
    processos_reais = df_teste.groupby(['mes_faturamento', 'modal', 'servico']).agg(
        processos_reais=('processo', 'count'),
        fat_real=('vlr_faturamento', 'sum')
    ).reset_index()

    # Faturamento previsto: processos reais × ticket do modelo (apenas métricas de treino)
    def get_ticket(row):
        ticket = metricas_treino['ticket_modal_servico'].get((row['modal'], row['servico']))
        return ticket if ticket else metricas_treino['ticket_modal'].get(row['modal'], 0)

    processos_reais['ticket_modelo'] = processos_reais.apply(get_ticket, axis=1)
    processos_reais['fat_previsto']  = processos_reais['processos_reais'] * processos_reais['ticket_modelo']

    # Agregar previsão por mês
    fat_previsto_mensal = processos_reais.groupby('mes_faturamento')['fat_previsto'].sum()

    # Métricas de performance
    print("\n" + "="*80)
    print("METRICAS DE PERFORMANCE")
    print("="*80)

    meses_comuns = fat_real_mensal.index.intersection(fat_previsto_mensal.index)
    real     = fat_real_mensal[meses_comuns]
    previsto = fat_previsto_mensal[meses_comuns]

    erros_pct = ((previsto - real) / real * 100)
    mape      = erros_pct.abs().mean()
    bias      = erros_pct.mean()
    hit_rate  = (erros_pct.abs() <= 20).sum() / len(erros_pct) * 100

    print(f"\nMAE  (erro absoluto medio): R$ {(previsto - real).abs().mean():,.2f}")
    print(f"MAPE (erro percentual medio): {mape:.1f}%  [meta: < 20%]")
    print(f"BIAS (tendencia sistematica): {bias:+.1f}%  [positivo = superestima]")
    print(f"HIT RATE (erro <= 20%): {hit_rate:.1f}%  [meta: >= 80% dos meses]")

    print("\nDetalhamento mensal:")
    print(f"{'Mes':>5} {'Real':>16} {'Previsto':>16} {'Erro %':>8}")
    print("-" * 50)
    for mes in sorted(meses_comuns):
        r = real[mes]
        p = previsto[mes]
        e = (p - r) / r * 100
        print(f"  {mes:02d}   R$ {r:>12,.2f}  R$ {p:>12,.2f}  {e:>+7.1f}%")

    # Retornar DataFrame detalhado além dos agregados
    df_backtesting = pd.DataFrame({
        'mes': sorted(meses_comuns),
        'faturamento_real':     [real[m] for m in sorted(meses_comuns)],
        'faturamento_previsto': [previsto[m] for m in sorted(meses_comuns)],
        'erro_pct':             [erros_pct[m] for m in sorted(meses_comuns)]
    })

    return fat_real_mensal, fat_previsto_mensal, {
        'mape':          mape,
        'bias':          bias,
        'hit_rate':      hit_rate,
        'df_detalhado':  df_backtesting,
        'metricas_treino': metricas_treino
    }
