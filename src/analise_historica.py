"""
Etapa 1: Análise Histórica
Calcula métricas históricas: lead time, ticket médio e share por modal dentro de cada serviço.
"""

import pandas as pd


def executar(df, verbose=True):
    if verbose:
        print("\n" + "="*80)
        print("ETAPA 1: ANÁLISE HISTÓRICA")
        print("="*80)

    # Lead times por modal
    lead_times = df.groupby('modal')['lead_time'].agg(
        mediano='median',
        media='mean',
        desvio='std',
        count='count'
    ).round(2)

    if verbose:
        print("\nLead Times (dias):")
        print(lead_times)

    # Share geral por modal
    fat_modal = df.groupby('modal')['vlr_faturamento'].sum()
    share_modal_geral = (fat_modal / fat_modal.sum() * 100).round(2)

    if verbose:
        print("\nShare Geral por Modal (%):")
        for modal, pct in share_modal_geral.items():
            print(f"  {modal}: {pct:.1f}%")

    # Share de modal dentro de cada serviço
    if verbose:
        print("\nShare de Modal DENTRO de Cada Serviço (%):")
    share_modal_por_servico = {}

    for servico in df['servico'].unique():
        df_servico = df[df['servico'] == servico]
        fat_por_modal = df_servico.groupby('modal')['vlr_faturamento'].sum()
        share = (fat_por_modal / fat_por_modal.sum() * 100).round(2)
        share_modal_por_servico[servico] = share.to_dict()

        if verbose:
            print(f"\n  {servico}:")
            for modal, pct in share.items():
                print(f"    {modal}: {pct:.1f}%")

    # Ticket médio por modal
    ticket_modal = df.groupby('modal')['vlr_faturamento'].mean().round(2)

    if verbose:
        print("\nTicket Médio por Modal (R$):")
        for modal, valor in ticket_modal.items():
            print(f"  {modal}: R$ {valor:,.2f}")

    # Ticket médio por modal + serviço (mínimo 10 registros)
    ticket_modal_servico = df.groupby(['modal', 'servico'])['vlr_faturamento'].agg(
        ticket_medio='mean',
        count='count'
    ).round(2)

    if verbose:
        print(f"\nTicket Médio por Modal + Serviço: {len(ticket_modal_servico)} combinações")
        display = ticket_modal_servico.reset_index()
        print(display.sort_values('ticket_medio', ascending=False).to_string(index=False))

    return {
        'lead_times': lead_times.to_dict('index'),
        'share_modal_geral': share_modal_geral.to_dict(),
        'share_modal_por_servico': share_modal_por_servico,
        'ticket_modal': ticket_modal.to_dict(),
        'ticket_modal_servico': ticket_modal_servico['ticket_medio'].to_dict()
    }
