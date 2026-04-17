"""
Etapa 5: Análise de Sensibilidade
Avalia o impacto de variações nos parâmetros do modelo (lead time e ticket médio)
sobre as recomendações de abertura de processos.
"""

import pandas as pd
from math import ceil


# Cenários de variação aplicados aos parâmetros
VARIACOES_TICKET    = [-0.20, -0.10, 0.00, +0.10, +0.20]
VARIACOES_LEAD_TIME = [-0.50, -0.20, 0.00, +0.20, +0.50]
NOMES_TICKET    = ['-20%', '-10%', 'Base', '+10%', '+20%']
NOMES_LEAD_TIME = ['-50%', '-20%', 'Base', '+20%', '+50%']


def _calcular_processos_totais(df_metas, metricas, fator_ticket=1.0):
    """Recalcula o total de processos necessários dado um fator de ajuste no ticket."""
    total = 0
    for _, meta_row in df_metas.iterrows():
        servico      = meta_row['servico']
        meta_servico = meta_row['vlr_meta']
        share_servico = metricas['share_modal_por_servico'].get(servico, {})

        for modal in ['Marítimo', 'Aéreo', 'Rodoviário']:
            share_modal = share_servico.get(modal, 0) / 100
            if share_modal == 0:
                continue

            meta_modal = meta_servico * share_modal

            ticket_base = metricas['ticket_modal_servico'].get((modal, servico))
            if ticket_base is None or ticket_base == 0:
                ticket_base = metricas['ticket_modal'].get(modal, 0)
            if ticket_base == 0:
                continue

            ticket_ajustado = ticket_base * fator_ticket
            if ticket_ajustado <= 0:
                continue
            processos = ceil(meta_modal / ticket_ajustado)
            total += processos

    return total


def executar(df_metas, metricas):
    print("\n" + "="*80)
    print("ETAPA 5: ANALISE DE SENSIBILIDADE")
    print("="*80)

    processos_base = _calcular_processos_totais(df_metas, metricas)

    if processos_base == 0:
        print("\nAVISO: base de processos eh zero - analise de sensibilidade de ticket ignorada")
        df_ticket = pd.DataFrame(columns=[
            'parametro', 'cenario', 'fator', 'processos_necessarios',
            'delta_absoluto', 'delta_percentual'
        ])
        df_lead = pd.DataFrame(columns=[
            'parametro', 'cenario', 'modal', 'fator',
            'lead_time_base_dias', 'lead_time_ajustado_dias', 'delta_dias'
        ])
        return df_ticket, df_lead

    # --- Sensibilidade ao Ticket Médio ---
    print("\nImpacto de variacoes no Ticket Medio (total de processos necessarios):")
    print(f"  {'Cenario':<10} {'Processos':>12} {'Variacao':>10} {'Delta':>8}")
    print("  " + "-"*44)

    rows_ticket = []
    for nome, fator in zip(NOMES_TICKET, VARIACOES_TICKET):
        total = _calcular_processos_totais(df_metas, metricas, fator_ticket=1 + fator)
        delta = total - processos_base
        variacao_pct = (total / processos_base - 1) * 100
        sinal = '+' if delta >= 0 else ''
        print(f"  {nome:<10} {total:>12,} {sinal}{variacao_pct:>8.1f}%  {sinal}{delta:>6,}")
        rows_ticket.append({
            'parametro': 'Ticket Medio',
            'cenario': nome,
            'fator': round(1 + fator, 2),
            'processos_necessarios': total,
            'delta_absoluto': delta,
            'delta_percentual': round(variacao_pct, 1)
        })

    # --- Sensibilidade ao Lead Time ---
    print("\nImpacto de variacoes no Lead Time (alteracao no mes de abertura):")
    print("  Lead time afeta QUANDO abrir, nao a quantidade de processos.")
    print("  A seguir, o impacto em dias de antecipacao/postergacao por modal:\n")

    rows_lead = []
    for nome, fator in zip(NOMES_LEAD_TIME, VARIACOES_LEAD_TIME):
        print(f"  Cenario {nome}:")
        for modal in ['Marítimo', 'Aéreo', 'Rodoviário']:
            lt_info = metricas['lead_times'].get(modal)
            if lt_info is None or 'mediano' not in lt_info:
                continue
            lt_base    = lt_info['mediano']
            lt_ajust   = round(lt_base * (1 + fator))
            delta_dias = lt_ajust - lt_base
            sinal = '+' if delta_dias >= 0 else ''
            print(f"    {modal:<12}: {lt_ajust:>3} dias ({sinal}{delta_dias} vs. base de {lt_base:.0f} dias)")
            rows_lead.append({
                'parametro': 'Lead Time',
                'cenario': nome,
                'modal': modal,
                'fator': round(1 + fator, 2),
                'lead_time_base_dias': int(lt_base),
                'lead_time_ajustado_dias': lt_ajust,
                'delta_dias': delta_dias
            })

    df_ticket = pd.DataFrame(rows_ticket)
    df_lead   = pd.DataFrame(rows_lead)

    print("\nResumo: variacoes no ticket medio impactam diretamente a quantidade de processos.")
    print("Variacoes no lead time deslocam o mes de abertura, nao a quantidade.")

    return df_ticket, df_lead
