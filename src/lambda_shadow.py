"""Isolated expansion experiment. Never supplies production scoring rates."""
from statistics_first import market_anchored_rates


def expanded_rates(base, prices, factor=.15):
    anchored, details = market_anchored_rates(base, prices, factor)
    if not details or not (details['fair_favorite_probability'] >= .60 or prices[details['favorite']] <= 1.60):
        return anchored, dict(applied=False, reason='no_strong_divergent_favorite')
    fav, other = ('home','away') if details['favorite']=='1' else ('away','home')
    total = base['home_ft'] + base['away_ft']
    increment = anchored[fav+'_ft'] - base[fav+'_ft']
    if increment <= 0:
        return anchored, dict(applied=False, reason='favorite_requires_downward_correction')
    # Half compensation implies +0.5*increment total; cap expansion at 10%.
    increment = min(increment, .20*total, 2*(base[other+'_ft']-.01))
    output = dict(base)
    for team, value in ((fav, base[fav+'_ft']+increment), (other, base[other+'_ft']-.5*increment)):
        output[team+'_ft'] = value
        output[team+'_ht'] = min(value, base[team+'_ht']*value/base[team+'_ft'])
    return output, dict(applied=True, favorite=details['favorite'], base_total=total,
                        expanded_total=output['home_ft']+output['away_ft'],
                        compensation_fraction=.5, maximum_expansion_fraction=.10)
