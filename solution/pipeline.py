import argparse, csv, hashlib, json, os, re, sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict

IST = timezone(timedelta(hours=5, minutes=30))
SALT = os.environ.get("PII_SALT", "agentic-universe-dev-salt")
DB = Path("data.db")

def phone_hash(c): return hashlib.sha256((SALT + c).encode()).hexdigest()
def hash_pii(v): return hashlib.sha256((SALT + str(v)).encode()).hexdigest()[:32]
def norm_header(h): return re.sub(r'[_\s]+', ' ', h.strip().lower())

def norm_phone(raw):
    if raw is None: return None
    d = re.sub(r'\D', '', str(raw))
    if len(d)==12 and d.startswith('91'): d=d[2:]
    elif len(d)==11 and d.startswith('0'): d=d[1:]
    return '+91'+d if len(d)==10 and d[0] in '6789' else None

def parse_date(raw):
    s=str(raw).strip()
    for f in ["%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%d-%b-%Y","%d-%B-%Y"]:
        try: return datetime.strptime(s, f).date().isoformat()
        except:
            try: return datetime.strptime(s.title(), f).date().isoformat()
            except: pass
    return None

def parse_decimal(raw):
    s=re.sub(r'^(₹|Rs\.?|INR)\s*','',str(raw).strip(),flags=re.I).replace(',','')
    try:
        v=float(s); return round(v,2) if v>=0 else None
    except: return None

def parse_ts(raw):
    s=str(raw).strip()
    if not s: return None
    try:
        if s.endswith('Z'): dt=datetime.fromisoformat(s.replace('Z','+00:00'))
        elif 'T' in s: dt=datetime.fromisoformat(s)
        else: dt=datetime.fromisoformat(s).replace(tzinfo=IST)
        if dt.tzinfo is None: dt=dt.replace(tzinfo=IST)
        return dt.astimezone(timezone.utc)
    except: return None

def get_db():
    con=sqlite3.connect(DB)
    con.executescript(Path(__file__).parent.joinpath("schema.sql").read_text())
    return con

def counter(con,k,delta=0):
    con.execute("INSERT OR IGNORE INTO counters VALUES(?,0)",(k,))
    if delta: con.execute("UPDATE counters SET value=value+? WHERE key=?",(delta,k))
    con.commit()

def load_schemas(base):
    schemas={}
    for f in Path(base).glob("schemas/*.json"):
        schemas[f.stem]=json.loads(f.read_text())
    disp={}
    with open(Path(base)/"schemas/dispositions.csv",encoding='utf-8-sig') as fh:
        for r in csv.DictReader(fh):
            disp[(r['workflow_id'],r['code'])]=(r.get('is_terminal','false').lower()=='true',r.get('is_success','false').lower()=='true')
    return schemas,disp

def ingest(drop,data_dir):
    drop=Path(drop); con=get_db()
    if con.execute("SELECT 1 FROM ingested_drops WHERE drop_path=?",(str(drop),)).fetchone():
        print(f"already ingested {drop}, skipping"); return
    schemas,disp=load_schemas(data_dir)
    for lead_file in sorted((drop/"leads").rglob("*.csv")):
        wf=lead_file.parent.name; schema=schemas.get(wf)
        if not schema: continue
        batch_id=lead_file.stem
        with open(lead_file,newline='',encoding='utf-8-sig') as fh:
            reader=csv.reader(fh)
            try: header=next(reader)
            except StopIteration: continue
            norm_to_field={}
            for f in schema['fields']:
                norm_to_field[norm_header(f['name'])]=f['name']
                for a in f.get('aliases',[]): norm_to_field[norm_header(a)]=f['name']
            colmap={}
            for idx,htxt in enumerate(header):
                fn=norm_to_field.get(norm_header(htxt))
                if fn: colmap[fn]=idx
            row_no=0
            for row in reader:
                if all(not (c or '').strip() for c in row): continue
                row_no+=1; counter(con,"lead_rows_total",1)
                vals={}
                for f in schema['fields']:
                    idx=colmap.get(f['name']); v=row[idx].strip() if idx is not None and idx<len(row) else ''
                    vals[f['name']]=v if v!='' else None
                reason=None; parsed={}
                for f in schema['fields']:
                    name,typ=f['name'],f['type']; raw=vals.get(name)
                    if raw is None:
                        if f.get('required'): reason=f"missing_required:{name}"; break
                        parsed[name]=None; continue
                    if typ=='string': parsed[name]=raw
                    elif typ=='phone':
                        cp=norm_phone(raw)
                        if not cp: reason="invalid_phone"; break
                        parsed[name]=cp
                    elif typ=='date':
                        dv=parse_date(raw)
                        if dv is None:
                            if f.get('required'): reason=f"invalid:{name}"; break
                            parsed[name]=None; counter(con,"field_warnings",1)
                        else: parsed[name]=dv
                    elif typ=='decimal':
                        dv=parse_decimal(raw)
                        if dv is None:
                            if f.get('required'): reason=f"invalid:{name}"; break
                            parsed[name]=None; counter(con,"field_warnings",1)
                        else: parsed[name]=dv
                    elif typ=='enum':
                        low=raw.lower()
                        if low not in [x.lower() for x in f['values']]:
                            if f.get('required'): reason=f"invalid:{name}"; break
                            parsed[name]=None; counter(con,"field_warnings",1)
                        else: parsed[name]=low
                    else: parsed[name]=raw
                if reason:
                    counter(con,"lead_rows_rejected",1)
                    con.execute("INSERT OR REPLACE INTO rejections VALUES(?,?,?,?)",(wf,batch_id,row_no,reason)); con.commit(); continue
                phone_field=next((f['name'] for f in schema['fields'] if f['type']=='phone'),None)
                cp=parsed.get(phone_field)
                if not cp: continue
                ph=phone_hash(cp)
                ref_field=next((f['name'] for f in schema['fields'] if f.get('reference')),None)
                ext_ref=str(parsed.get(ref_field,'')) if ref_field else ''
                store={}
                for f in schema['fields']:
                    v=parsed.get(f['name'])
                    if v is None: continue
                    store[f['name']]=hash_pii(v) if f.get('pii') else v
                existing=con.execute("SELECT fields_json FROM leads WHERE workflow_id=? AND phone_hash=?",(wf,ph)).fetchone()
                if existing is None:
                    for f in schema['fields']:
                        if f['name'] not in store and 'default' in f: store[f['name']]=f['default']
                    con.execute("INSERT INTO leads VALUES(?,?,?,?,?,?)",(wf,ph,cp[-4:],batch_id,ext_ref,json.dumps(store)))
                else:
                    counter(con,"duplicate_lead_rows_merged",1)
                    old=json.loads(existing[0])
                    for k,v in store.items():
                        if v is not None: old[k]=v
                    for f in schema['fields']:
                        if f['name'] not in old and 'default' in f: old[f['name']]=f['default']
                    con.execute("UPDATE leads SET fields_json=? WHERE workflow_id=? AND phone_hash=?",(json.dumps(old),wf,ph))
                con.commit()
    for ev_file in sorted((drop/"events").glob("*.jsonl")):
        for line in open(ev_file,encoding='utf-8'):
            if not line.strip(): continue
            counter(con,"events_total",1)
            try: ev=json.loads(line)
            except: counter(con,"malformed_events",1); continue
            if not ev.get('event_id'): counter(con,"malformed_events",1); continue
            if con.execute("SELECT 1 FROM events WHERE event_id=?",(ev['event_id'],)).fetchone():
                counter(con,"duplicate_events_dropped",1); continue
            if ev.get('event_type','').startswith('disposition.'):
                code=ev.get('payload',{}).get('disposition_code')
                if (ev.get('workflow_id'),code) not in disp:
                    counter(con,"unknown_disposition_events",1); continue
            dt=parse_ts(ev.get('occurred_at','')); ph=None
            if ev.get('phone'):
                cp=norm_phone(ev['phone'])
                if cp: ph=phone_hash(cp)
            con.execute("INSERT INTO events VALUES(?,?,?,?,?,?,?,0)",(ev['event_id'],ev.get('event_type'),ev.get('workflow_id'),ev.get('call_id'),ph,dt.isoformat() if dt else None,json.dumps(ev.get('payload',{}))))
            con.commit()
    for (eid,wf,call_id,ph) in con.execute("SELECT event_id,workflow_id,call_id,phone_hash FROM events WHERE lead_linked=0").fetchall():
        lead_ph=ph
        if lead_ph is None and call_id:
            r=con.execute("SELECT phone_hash FROM events WHERE call_id=? AND phone_hash IS NOT NULL LIMIT 1",(call_id,)).fetchone()
            if r: lead_ph=r[0]
        if lead_ph and con.execute("SELECT 1 FROM leads WHERE workflow_id=? AND phone_hash=?",(wf,lead_ph)).fetchone():
            con.execute("UPDATE events SET phone_hash=?,lead_linked=1 WHERE event_id=?",(lead_ph,eid))
        con.commit()
    con.execute("INSERT OR IGNORE INTO ingested_drops VALUES(?,?)",(str(drop),'x')); con.commit(); con.close()
    print(f"ingested {drop}")

def report(out):
    out=Path(out); out.mkdir(parents=True,exist_ok=True); con=get_db()
    _,disp=load_schemas("assignment-a-data")
    leads=con.execute("SELECT workflow_id,phone_hash,phone_last4,batch_id,external_ref FROM leads").fetchall()
    events=con.execute("SELECT event_id,event_type,workflow_id,call_id,phone_hash,occurred_at,payload_json FROM events WHERE lead_linked=1").fetchall()
    calls={}
    for eid,et,wf,cid,ph,occ,pay in events:
        if not cid: continue
        c=calls.setdefault((wf,cid),{'wf':wf,'call_id':cid,'ph':ph,'initiated':None,'connected':False,'failed':False,'sets':[],'overrides':[]})
        if et=='call.initiated' and occ:
            if not c['initiated'] or occ<c['initiated']: c['initiated']=occ
            c['ph']=ph or c['ph']
        if et=='call.connected': c['connected']=True
        if et=='call.telephony_failed': c['failed']=True
        if et=='disposition.set': c['sets'].append((occ,json.loads(pay).get('disposition_code')))
        if et=='disposition.override': c['overrides'].append((occ,json.loads(pay).get('disposition_code')))
    for c in calls.values():
        if c['overrides']: c['eff']=sorted(c['overrides'])[-1][1]
        elif c['sets']: c['eff']=sorted(c['sets'])[-1][1]
        else: c['eff']=None
    lead_calls=defaultdict(list)
    for c in calls.values():
        if c['initiated']: lead_calls[(c['wf'],c['ph'])].append(c)
    batches=defaultdict(list)
    for wf,ph,last4,batch,ext in leads: batches[(wf,batch)].append((ph,ext))
    with open(out/"batch_summary.csv","w",newline='') as fh:
        w=csv.writer(fh); w.writerow(["workflow_id","batch_id","total_leads","yet_to_reach_out","in_progress_telephony_failed","in_progress_dialed","completed"])
        for (wf,batch) in sorted(batches):
            cnt={"yet_to_reach_out":0,"completed":0,"in_progress_telephony_failed":0,"in_progress_dialed":0}
            for ph,ext in batches[(wf,batch)]:
                cs=lead_calls.get((wf,ph),[])
                if not cs: cnt["yet_to_reach_out"]+=1
                elif any(disp.get((wf,c['eff']),(False,False))[0] for c in cs if c['eff']): cnt["completed"]+=1
                elif all(c['failed'] for c in cs): cnt["in_progress_telephony_failed"]+=1
                else: cnt["in_progress_dialed"]+=1
            w.writerow([wf,batch,len(batches[(wf,batch)]),cnt["yet_to_reach_out"],cnt["in_progress_telephony_failed"],cnt["in_progress_dialed"],cnt["completed"]])
    daily=defaultdict(list)
    for c in calls.values():
        if c['initiated']:
            dt=datetime.fromisoformat(c['initiated']).astimezone(IST); daily[(c['wf'],dt.date().isoformat())].append(c)
    with open(out/"daily_overview.csv","w",newline='') as fh:
        w=csv.writer(fh); w.writerow(["workflow_id","date","total_calls","connected_calls","connectivity_rate","success_leads"])
        for (wf,d) in sorted(daily):
            cs=daily[(wf,d)]; conn=sum(1 for c in cs if c['connected'])
            succ=len({c['ph'] for c in cs if c['eff'] and disp.get((wf,c['eff']),(False,False))[1]})
            w.writerow([wf,d,len(cs),conn,f"{conn/len(cs):.4f}" if cs else "0.0000",succ])
    with open(out/"lead_report.csv","w",newline='') as fh:
        w=csv.writer(fh); w.writerow(["workflow_id","external_ref","batch_id","phone_masked","bucket","total_calls","connected_calls","last_call_at","current_disposition","ever_success"])
        for wf,ph,last4,batch,ext in sorted(leads,key=lambda x:(x[0],x[4])):
            cs=sorted(lead_calls.get((wf,ph),[]),key=lambda x:x['initiated'] or '')
            if not cs: bucket="yet_to_reach_out"
            elif any(disp.get((wf,c['eff']),(False,False))[0] for c in cs if c['eff']): bucket="completed"
            elif all(c['failed'] for c in cs): bucket="in_progress_telephony_failed"
            else: bucket="in_progress_dialed"
            last=cs[-1]['initiated'] if cs else ''; last_ist=datetime.fromisoformat(last).astimezone(IST).isoformat() if last else ''
            cur=''
            for c in reversed(cs):
                if c['eff']: cur=c['eff']; break
            ever=any(disp.get((wf,c['eff']),(False,False))[1] for c in cs if c['eff'])
            w.writerow([wf,ext,batch,f"+91******{last4}",bucket,len(cs),sum(1 for c in cs if c['connected']),last_ist,cur,str(ever).lower()])
    with open(out/"rejections.csv","w",newline='') as fh:
        w=csv.writer(fh); w.writerow(["workflow_id","batch_id","row_number","reason"])
        for wf,batch,rn,reason in con.execute("SELECT * FROM rejections ORDER BY workflow_id,batch_id,row_number"): w.writerow([wf,batch,rn,reason])
    def get(k):
        r=con.execute("SELECT value FROM counters WHERE key=?",(k,)).fetchone(); return r[0] if r else 0
    orphan=con.execute("SELECT COUNT(*) FROM events WHERE lead_linked=0").fetchone()[0]
    by_reason={}
    for reason,cnt in con.execute("SELECT reason,COUNT(*) FROM rejections GROUP BY reason"): by_reason[reason]=cnt
    dq={"lead_rows_total":get("lead_rows_total"),"lead_rows_rejected":get("lead_rows_rejected"),"rejections_by_reason":by_reason,"duplicate_lead_rows_merged":get("duplicate_lead_rows_merged"),"field_warnings":get("field_warnings"),"events_total":get("events_total"),"malformed_events":get("malformed_events"),"duplicate_events_dropped":get("duplicate_events_dropped"),"orphan_events":orphan,"unknown_disposition_events":get("unknown_disposition_events")}
    (out/"data_quality.json").write_text(json.dumps(dq,indent=2)); print(f"reports in {out}")

if __name__=="__main__":
    ap=argparse.ArgumentParser(); sub=ap.add_subparsers(dest="cmd",required=True)
    p1=sub.add_parser("ingest"); p1.add_argument("drop"); p1.add_argument("--data-dir",default="assignment-a-data")
    p2=sub.add_parser("report"); p2.add_argument("--out",default="reports")
    a=ap.parse_args()
    if a.cmd=="ingest": ingest(a.drop,a.data_dir)
    else: report(a.out)