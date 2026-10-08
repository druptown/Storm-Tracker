import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location('target_replay',Path(__file__).parents[1]/'scripts/oldenburg_target_replay.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_causal_fields_exclude_future_receipt_and_future_product():
    def field(name, receipt, product):
        return {'field_id':name,'available_at':receipt,'metadata':{'observed_at':product}}
    fields = [field('later-receipt',101,90),field('future-product',90,101),field('past',90,80)]
    assert [f['field_id'] for f in module.causally_available(fields,100)] == ['past']


def test_unknown_is_not_dry_and_weak_thresholds_remain_distinct():
    assert module.classify(None,.2,.1) == 'unknown_on_wet'
    assert module.classify(.01,.01,0.) == 'hit'
    assert module.classify(.01,.01,.1) == 'correct_dry'
    assert module.classify(.2,0.,.1) == 'false_wet'
