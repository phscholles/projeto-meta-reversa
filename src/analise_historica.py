"""
Etapa 1: Análise Histórica
Calcula métricas históricas: lead time, ticket médio e share por modal dentro de cada serviço.
"""

MIN_AMOSTRA_TICKET = 10


def executar(df, verbose=True):
    if verbose:
        print("\n" + "="*80)
        print("ETAPA 1: ANÁLISE HISTÓRICA")
        print("="*80)

    # Lead times por modal (dias inteiros)
    lead_times = df.groupby('modal')['lead_time'].agg(
        mediano='median',
        media='mean',
        desvio='std',
        count='count'
    ).round({'mediano': 0, 'media': 1, 'desvio': 1, 'count': 0})

    if verbose:
        print("\nLead Times por Modal (dias):")
        print(lead_times)

    # Share geral por modal (por faturamento)
    fat_por_modal_total = df.groupby('modal')['vlr_faturamento'].sum()
    share_geral_modal = (fat_por_modal_total / fat_por_modal_total.sum() * 100).round(2)

    if verbose:
        print("\nShare Geral por Modal (%) — por faturamento:")
        for modal, pct in share_geral_modal.items():
            print(f"  {modal}: {pct:.1f}%")

    # Share de modal dentro de cada serviço (por faturamento)
    if verbose:
        print("\nShare de Modal DENTRO de Cada Serviço (%):")
    share_modal_por_servico = {}

    for servico in df['servico'].unique():
        df_servico = df[df['servico'] == servico]
        fat_por_modal = df_servico.groupby('modal')['vlr_faturamento'].sum()
        total_fat = fat_por_modal.sum()
        if total_fat <= 0:
            share_modal_por_servico[servico] = {}
            continue
        share = (fat_por_modal / total_fat * 100).round(2)
        share_modal_por_servico[servico] = share.to_dict()

        soma = float(share.sum())
        if abs(soma - 100.0) > 0.1:
            print(f"  AVISO: shares de '{servico}' somam {soma:.2f}% (esperado ~100%)")

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

    # Ticket médio por modal + serviço (apenas combinações com amostra mínima)
    ticket_grupo = df.groupby(['modal', 'servico'])['vlr_faturamento'].agg(
        ticket_medio='mean',
        count='count'
    ).round(2)
    ticket_modal_servico = ticket_grupo[ticket_grupo['count'] >= MIN_AMOSTRA_TICKET]

    if verbose:
        descartados = len(ticket_grupo) - len(ticket_modal_servico)
        print(f"\nTicket Médio por Modal + Serviço: {len(ticket_modal_servico)} combinações "
              f"(>= {MIN_AMOSTRA_TICKET} obs; {descartados} descartadas vão para fallback por modal)")
        display = ticket_modal_servico.reset_index()
        print(display.sort_values('ticket_medio', ascending=False).to_string(index=False))

    return {
        'lead_times': lead_times.to_dict('index'),
        'share_geral_modal': share_geral_modal.to_dict(),
        'share_modal_por_servico': share_modal_por_servico,
        'ticket_modal': ticket_modal.to_dict(),
        'ticket_modal_servico': ticket_modal_servico['ticket_medio'].to_dict()
    }