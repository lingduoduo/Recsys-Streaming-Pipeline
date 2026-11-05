"""Regenerate whole-schema Scala measurement goldens using retained Python references.

Run from the repository root with a Python environment containing pandas/numpy:
python recsys-pipeline/services/spark-streaming-job/src/test/resources/report/generate-measurement-reference.py
This is test-fixture generation only; Scala report jobs do not execute Python.
"""
import sys,json
sys.path.insert(0,'recsys-pipeline/services/python-modeling')
import pandas as pd
from analysis_dashboard_report import build_measurement_dashboard
samples=[]
for i in range(32):
 samples.append(dict(user_id='u'+str(i%4),item_id='m'+str(i%8),clicked=int(i%3==0),ordered=int(i%7==0),reward=(i%4)/3,rating= None if i%5==0 else (i%10+1)/2,dwell_millis=i*123,completion_rate=(i%8)/8,negative_feedback_reason=None if i%4 else 'hide',published_at=1700000000-(i%8)*864000,impression_ts=1700100000+i*10,request_id='r'+str(i//4),position=i%4,label=i%3,filter_reason=None if i%3 else ('expired' if i%2 else 'other'),unsafe_label=bool(i%9==0),user_features={'gender':'a' if i%2 else 'b','device':'web'}))
slates=[dict(request_id='s'+str(i),user_id='u'+str(i%3),request_ts=1700100000+i*10,items=[dict(item_id='m'+str((i+j)%8),label=(i+j)%3,genres=['Action','Drama'] if j%2 else ['Comedy'],popularity=((i+j)%8)/8) for j in range(4)]) for i in range(8)]
live={'measurements':{'freshness':{'availability':'available','exposures':10,'source':'published_at','freshShare':.4,'coverage':.7},'safety':{'availability':'available','evaluatedCandidates':20,'totalDecisions':3,'policyVersion':'p','reasons':{'expired':{'count':3}},'unknownShare':0},'feedbackCoverage':{'availability':'available','total':12,'signals':{'rating':{'coverage':.5},'request_id':{'coverage':1}}},'latency':{'availability':'available','endpoints':{'recommend':{'count':20,'p50':1,'p95':2,'p99':3,'errorRate':.1,'timeoutRate':0}},'stages':{'rank':{'count':20,'p50':.5,'p95':1,'p99':2}}}}}
config={'now':'2026-11-05T00:00:00Z','fairness_min_support':2,'catalog_size':20}
edge_samples=[{'user_id':'u','clicked':'bad','ordered':'false','reward':None,'rating':None,'negative_feedback_reason':'','filter_reason':' ','unsafe_label':'false','gender':None,'request_id':'r','position':1,'label':0}, {'user_id':'v','clicked':1,'ordered':1,'reward':2,'rating':5,'filter_reason':'muted_title','unsafe_label':None,'gender':'g','request_id':'r','position':0,'label':2}]
edge_slates=[{'request_id':'partial','items':[{'label':1,'item_id':'a'}, {'item_id':'b'}]}, {'request_id':'complete','user_id':'u','items':[{'label':0,'item_id':'b'}, {'label':2,'item_id':'a','genres':['Drama'],'popularity':0}]}]
cases=[]
for ss,sl,ll,cc in [(samples,slates,live,config),([],None,None,{}),(edge_samples,edge_slates,{'latency':{'availability':'available','endpoints':{'empty':{'count':0}}}},dict(config,catalog_size=1)),([{'new_release':True,'clicked':1,'item_id':'a'},{'new_release':False,'clicked':0,'item_id':'b'}],None,None,config)]:
 frame=pd.DataFrame(ss); slateframe=pd.DataFrame(sl) if sl is not None else None
 if slateframe is not None:slateframe.attrs['catalog_size']=cc.get('catalog_size')
 expected=build_measurement_dashboard(frame,slateframe,ll,cc);expected['schemaVersion']='2.0'
 cases.append(dict(samples=ss,slates=sl,live=ll,config=cc,expected=expected))
with open('recsys-pipeline/services/spark-streaming-job/src/test/resources/report/measurement-reference.json','w') as f:json.dump(cases,f,allow_nan=False,indent=2)
