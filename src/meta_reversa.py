"""
Etapa 3: Cálculo de Meta Reversa
Calcula quantos processos abrir por (modal, serviço) e mês para atingir cada meta.
"""

import pandas as pd
import numpy as np


def executar(df_dist, metricas):
    print("\n" + "="*80)
    print("ETAPA 3: CÁLCULO DE META REVERSA")
    print("="*80)

    recomendacoes = []

    for _, row in df_dist.iterrows():
        modal              = row['modal']
        servico            = row['servico']
        meta_modal_servico = row['meta_modal_servico']

        # Buscar ticket específico; fallback para ticket médio do modal
        ticket = metricas['ticket_modal_servico'].get((modal, servico))
        if ticket is None or ticket == 0:
            ticket = metricas['ticket_modal'][modal]

        processos = int(np.ceil(meta_modal_servico / ticket))

        recomendacoes.append({
            'mes_meta':              row['mes_meta'],
            'ano_meta':              row['ano_meta'],
            'servico':               servico,
            'modal':                 modal,
            'meta_modal_servico':    meta_modal_servico,
            'ticket':                ticket,
            'processos_necessarios': processos,
            'valor_esperado':        processos * ticket,
            'mes_abertura':          row['mes_abertura'],
            'ano_abertura':          row['ano_abertura'],
            'data_limite':           row['data_limite']
        })

    df_rec = pd.DataFrame(recomendacoes)
    print(f"\nRecomendações geradas: {len(df_rec)} linhas")

    # --- Output 1: Meta de Aberturas (quando abrir) ---
    print("\n" + "="*80)
    print("META DE ABERTURAS")
    print("="*80)

    df_ab = df_rec.copy()
    df_ab['ano_mes_sort'] = df_ab['ano_abertura'] * 100 + df_ab['mes_abertura']
    df_ab['mes_ano'] = df_ab.apply(
        lambda r: f"{int(r['mes_abertura']):02d}/{int(r['ano_abertura'])}", axis=1
    )

    tabela_aberturas = df_ab.pivot_table(
        values='processos_necessarios',
        index=['ano_mes_sort', 'mes_ano'],
        columns=['servico', 'modal'],
        aggfunc='sum',
        fill_value=0
    ).astype(int)
    tabela_aberturas = tabela_aberturas.reset_index(level=0, drop=True)
    tabela_aberturas.loc['TOTAL'] = tabela_aberturas.sum()

    print("\nProcessos a abrir por mês:")
    print(tabela_aberturas)

    # --- Output 2: Processos por mês-meta (quando faturar) ---
    print("\n" + "="*80)
    print("PROCESSOS NECESSÁRIOS (por mês-meta)")
    print("="*80)

    tabela_faturamento = df_rec.pivot_table(
        values='processos_necessarios',
        index=['ano_meta', 'mes_meta'],
        columns=['servico', 'modal'],
        aggfunc='sum',
        fill_value=0
    ).astype(int)
    tabela_faturamento.index = [
        f"{int(m):02d}/{int(a)}" for a, m in tabela_faturamento.index
    ]
    tabela_faturamento.loc['TOTAL'] = tabela_faturamento.sum()

    print("\nProcessos por mês-meta:")
    print(tabela_faturamento)

    # --- Resumo consolidado ---
    print("\n" + "="*80)
    print("RESUMO CONSOLIDADO")
    print("="*80)

    tab_calc = tabela_aberturas.drop('TOTAL')
    total_geral = tab_calc.sum().sum()

    print("\nTotal por Serviço:")
    for servico in tab_calc.columns.get_level_values(0).unique():
        total = tab_calc[servico].sum().sum()
        pct   = total / total_geral * 100
        print(f"  {servico}: {total:,} processos ({pct:.1f}%)")

    print("\nTotal por Modal:")
    for modal in ['Marítimo', 'Aéreo', 'Rodoviário']:
        if modal in tab_calc.columns.get_level_values(1):
            total = tab_calc.xs(modal, level=1, axis=1).sum().sum()
            pct   = total / total_geral * 100
            print(f"  {modal}: {total:,} processos ({pct:.1f}%)")

    print(f"\nTotal Geral: {total_geral:,} processos")

    return df_rec, tabela_aberturas
