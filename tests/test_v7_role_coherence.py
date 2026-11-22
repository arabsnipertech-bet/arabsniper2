"""Role priority and cross-view coherence; no probability or outcome fitting."""
from copy import deepcopy
from pathlib import Path
import json
import sys
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from lateral_matrix import market_id, specification, label
from v7_decision import (GG, GOAL, TRIAD_MARKETS, compatible,
                         confirmed_triad_coherent, route, stored_offers)
from v7_probability import VERSION
from test_gold_explorer import AT, KEY, fixture, resign
import gold_explorer

FLAGS=dict(conversion=False,rebound_gg=False)
U25=market_id(specification('total','ft','under',2.5))
U05=market_id(specification('total','ht','under',.5))
NG_ST=market_id(specification('btts','sh','no'))


def candidate(key,p,odd,*,low=-.15,tier=None):
    spec=json.loads(key)
    return dict(source_market_id=key,label=label(spec),spec_kind=spec['kind'],calibrated=p,
        odd=odd,ev=p*odd-1,ev_low=low,tier=tier,niche_id=None,supported=True,
        horizon={'ht':'PT','sh':'ST','ft':'FT'}[spec['period']],model_fair_odd=1/p,
        quote_state='stable',study_supported=True,features={},fair=1/(odd*1.05),
        quote=dict(quote_status='recent',bookmaker_id='1'))


def snapshot_with_alternative(other):
    match=fixture();matrix=match['multimarket']['matrix'];snap=matrix['v7']
    snap['status']='published'
    matrix['rows'][0].update(probability_model=VERSION,spec=json.loads(KEY))
    snap['triad']={'most_probable':dict(market_id=KEY,source_market_id=KEY,label='Over 2,5 FT',
        odd=2.,probability=.6,tier='GOLD',study_only=False)}
    source=deepcopy(matrix['rows'][0]);source.update(id=other,odd=2.7,spec=json.loads(other))
    matrix['rows'].append(source)
    market=deepcopy(snap['markets'][KEY]);market.update(source_market_id=other,calibrated=.4,
        label=label(json.loads(other)),odd=2.7,ev=.08,ev_low=.01,quote=source)
    snap['markets'][other]=market;resign(match)
    return match


class StudyRolePriority(unittest.TestCase):
    def test_standard_roles_beat_higher_probability_out_of_role_fillers(self):
        # Some values reproduce the reported screen; alternative prices are
        # synthetic. This is a selector regression, not a replay of the live board.
        quotes=[(NG_ST,.665,1.5),(U25,.413,2.55),(U05,.269,4.),
                (GOAL['over25_ft'],.587,1.8),(GOAL['over15_ht'],.38,2.6),(GG,.234,4.)]
        records={key:candidate(key,p,odd) for key,p,odd in quotes};before=deepcopy(records)
        triad,_=route(records,FLAGS,study=True)
        self.assertEqual([p['market_id'] for p in triad.values()],
                         [GOAL['over25_ft'],GOAL['over15_ht'],GG])
        self.assertEqual(records,before)
        for role,pick in triad.items():
            self.assertIn(pick['market_id'],TRIAD_MARKETS[role])
            self.assertTrue(pick['study_only'])
            self.assertEqual(pick['probability'],records[pick['market_id']]['calibrated'])

    def test_three_relaxed_fillers_cannot_displace_a_standard_choice(self):
        quotes=[(NG_ST,.665,1.5),(U25,.413,2.55),(U05,.269,4.),(GOAL['over25_ft'],.587,1.8)]
        records={key:candidate(key,p,odd) for key,p,odd in quotes}
        triad,_=route(records,FLAGS,study=True)
        self.assertEqual(triad['most_probable']['market_id'],GOAL['over25_ft'])
        self.assertIsNone(triad['recommended_alternative'])
        self.assertTrue(compatible(*[p['market_id'] for p in triad.values() if p]))

    def test_outside_role_observations_do_not_force_a_triad(self):
        records={key:candidate(key,p,odd) for key,p,odd in
                 [(NG_ST,.665,1.5),(U25,.413,2.55),(U05,.269,4.)]}
        before=deepcopy(records)
        triad,signals=route(records,FLAGS,study=True)
        self.assertFalse(any(triad.values()))
        self.assertFalse(any(signals[role] for role in ('primary','consigliata','colpaccio')))
        self.assertEqual(records,before,'The alternative analyses are retained')

    def test_a_qualified_compatible_under_is_not_generically_banned(self):
        under=market_id(specification('total','ft','under',3.5))
        records={GOAL['over25_ft']:candidate(GOAL['over25_ft'],.6,1.8,low=.03,tier='BLU'),
                 under:candidate(under,.42,2.6,low=.02,tier='BLU')}
        triad,_=route(records,FLAGS,study=True,qualified_alternatives=('recommended_alternative',))
        self.assertEqual(triad['recommended_alternative']['market_id'],under)
        self.assertFalse(triad['recommended_alternative'].get('study_only',False))

    def test_different_lines_or_periods_are_not_automatically_opposites(self):
        self.assertFalse(compatible(GOAL['over25_ft'],U25))
        self.assertFalse(compatible(GG,market_id(specification('btts','ft','no'))))
        self.assertTrue(compatible(GOAL['over15_ht'],market_id(specification('total','ft','under',3.5))))


class CrossViewCoherence(unittest.TestCase):
    def test_opposite_value_remains_available_but_leaves_recommended_rankings(self):
        match=snapshot_with_alternative(U25);before=deepcopy(match)
        rows=gold_explorer.project_match(match,AT)
        opposite=next(row for row in rows if row['market']==U25)
        self.assertTrue(opposite['validated'],'Value and coherence are separate checks')
        self.assertEqual(opposite['coherence_status'],'opposed')
        self.assertEqual(opposite['coherence_opposed_to'],['Over 2,5 FT'])
        self.assertNotIn(opposite['id'],gold_explorer.rank(rows))
        self.assertEqual(match,before)

    def test_compatible_under_remains_in_rankings(self):
        other=market_id(specification('total','ft','under',3.5))
        rows=gold_explorer.project_match(snapshot_with_alternative(other),AT)
        row=next(r for r in rows if r['market']==other)
        self.assertEqual(row['coherence_status'],'compatible')
        self.assertIn(row['id'],gold_explorer.rank(rows,diversify=False))

    def test_an_observation_cannot_veto_a_qualified_alternative(self):
        match=snapshot_with_alternative(U25)
        match['multimarket']['matrix']['v7']['triad']['most_probable']['study_only']=True
        resign(match)
        rows=gold_explorer.project_match(match,AT)
        row=next(r for r in rows if r['market']==U25)
        self.assertEqual(row['coherence_status'],'no_confirmed_triad')
        self.assertIn(row['id'],gold_explorer.rank(rows,diversify=False))

    def test_inconsistent_confirmed_snapshot_is_not_published_or_promoted(self):
        match=snapshot_with_alternative(U25);snap=match['multimarket']['matrix']['v7']
        snap['triad']['recommended_alternative']=dict(market_id=U25,source_market_id=U25,
            label='Under 2,5 FT',odd=2.7,probability=.4,tier='GOLD',study_only=False)
        resign(match);before=deepcopy(match)
        self.assertFalse(confirmed_triad_coherent(snap['triad']))
        self.assertEqual(list(stored_offers(match['multimarket'],match['fixture'],AT.isoformat())),[])
        rows=gold_explorer.project_match(match,AT)
        self.assertTrue(all(r['coherence_status']=='inconsistent_gold_triad' for r in rows))
        self.assertEqual(gold_explorer.rank(rows),[])
        self.assertEqual(match,before)


if __name__=='__main__':unittest.main()
