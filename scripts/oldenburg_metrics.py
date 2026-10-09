"""Fixed, transparent radar reference and frozen forecast comparison."""
import json,hashlib
from pathlib import Path
from datetime import datetime
from collections import Counter
DEST=Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008')
def ep(s):return datetime.fromisoformat(s).timestamp()
def local(t):return datetime.fromtimestamp(t,datetime.fromisoformat('2026-10-08T00:00:00+02:00').tzinfo).isoformat()
def refs(frames,threshold):
    events=[];active=None;previous=None
    for f in frames:
        t=f['metadata']['observed_at'];v=f['target_rate_mm_h']
        if v is None:
            if active:active['coverage_gaps']+=1
            previous=None;continue
        wet=v>0 if threshold==0 else v>=threshold
        if wet and active is None:
            active={'entry_lower':None if previous is None else previous,'entry_upper':t,'last_wet':t,'peak_mm_h':v,'coverage_gaps':0}
        if wet and active:
            active['last_wet']=t;active['peak_mm_h']=max(active['peak_mm_h'],v)
        if not wet and active:
            active['exit_lower']=active['last_wet'];active['exit_upper']=t;events.append(active);active=None
        previous=t
    if active:active['exit_lower']=active['last_wet'];active['exit_upper']=None;events.append(active)
    return events
def main():
    a=json.loads((DEST/'event-audit.json').read_text());m=json.loads((DEST/'export-manifest.json').read_text())
    frames=m['frames'];events=refs(frames,0);threshold_events=refs(frames,.1)
    issues={i['forecast_issue_id']:i for i in a['issues']}
    comparison=[]
    for e in events:
        if e['entry_lower'] is None:continue
        forecasts=[]
        for p in a['passages']:
            i=issues[p['forecast_issue_id']];issued=ep(i['issued_at']);entry=ep(p['entry_at'])
            if issued<e['entry_lower'] and e['entry_lower']-3600<=entry<=e['entry_upper']+3600:
                forecasts.append({'issued_at':i['issued_at'],'predicted_entry':p['entry_at'],'predicted_exit':p['exit_at'],
                    'entry_error_minutes_vs_bracket':[(entry-e['entry_upper'])/60,(entry-e['entry_lower'])/60],
                    'lead_minutes_to_actual_bracket':[(e['entry_lower']-issued)/60,(e['entry_upper']-issued)/60]})
        comparison.append({'radar_reference':e,'pre_event_passage_candidates':forecasts})
    manifest={}
    for p in sorted(DEST.rglob('*')):
        if p.is_file() and p.name not in {'metrics.json','files-sha256.json','replay-comparison.json'}:
            manifest[str(p.relative_to(DEST))]={'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size}
    (DEST/'files-sha256.json').write_text(json.dumps(manifest,separators=(',',':')))
    out={'reference':'Local fused quantitative radar pixel; NOT exact ground truth. Weak >0 and existing .1mm/h passage threshold kept distinct.',
         'weak_positive_events':events,'policy_threshold_events':threshold_events,'frozen_forecast_comparison':comparison,
         'issues':len(a['issues']),'passages':len(a['passages']),'used_exact_fields':len(frames),
         'complete_cycles':sum(c['complete'] for c in a['cycles']),'cycles':len(a['cycles']),
         'issue_reason_counts':dict(Counter(i['forecast_metadata']['forecast']['reason_code'] for i in a['issues'])),
         'max_product_gap_minutes':max((b['metadata']['observed_at']-x['metadata']['observed_at'])/60 for x,b in zip(frames,frames[1:])),
         'gaps':m['gap_counts'],'ground_onset_at_target':None,'production_modified':False}
    (DEST/'metrics.json').write_text(json.dumps(out,separators=(',',':')))
    print(json.dumps({'events':[{'entry':[None if e['entry_lower'] is None else local(e['entry_lower']),local(e['entry_upper'])],'exit':[local(e['exit_lower']),None if e['exit_upper'] is None else local(e['exit_upper'])],'peak':e['peak_mm_h']} for e in events],
                      'complete_cycles':out['complete_cycles'],'cycles':out['cycles'],'max_product_gap_minutes':out['max_product_gap_minutes'],'files_hashed':len(manifest)},separators=(',',':')))
if __name__=='__main__':main()
