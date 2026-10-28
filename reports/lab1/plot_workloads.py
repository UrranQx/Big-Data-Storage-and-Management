"""Summarize measured workload CSV and draw exportable figures; no synthetic data."""
from collections import defaultdict
import csv
import json
from pathlib import Path
import statistics
import sys

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

root=Path(sys.argv[1]);rows=list(csv.DictReader((root/'measurements.csv').open(encoding='utf-8')))
groups=defaultdict(list)
for row in rows:
    if row['seconds']:groups[int(row['size']),row['store'],row['phase'],row['operation']].append(row)
summary=[]
for (size,store,phase,op),values in sorted(groups.items()):
    times=[float(r['seconds']) for r in values]
    item={'size':size,'store':store,'phase':phase,'operation':op,'n':len(times),
          'median_seconds':statistics.median(times),'min_seconds':min(times),'max_seconds':max(times)}
    for field in ('get_calls','read_bytes','put_calls','written_bytes','copy_calls','copied_bytes','delete_calls'):
        item[field]=statistics.median(float(r[field] or 0) for r in values)
    summary.append(item)
with (root/'summary.csv').open('w',newline='',encoding='utf-8') as out:
    writer=csv.DictWriter(out,fieldnames=list(summary[0]));writer.writeheader();writer.writerows(summary)
lookup={(r['size'],r['store'],r['phase'],r['operation']):r for r in summary}
stores=['postgres','mongo','garage'];names=['PostgreSQL','MongoDB','Garage S3'];colors=['#336791','#4b9c46','#e2972c']
sizes=sorted({r['size'] for r in summary})
fig,axes=plt.subplots(2,2,figsize=(11,8))
for ax,(op,title) in zip(axes.flat,[('scope_top_100','Фильтр и сортировка: 100 строк'),('monthly_counts','Группировка по месяцам'),('join_1000','Связь с профилями: 1000 ранов'),('player_runs','Все раны одного участника')]):
    for store,name,color in zip(stores,names,colors):
        values=[lookup[size,store,'indexed',op] for size in sizes]
        ys=[r['median_seconds']*1000 for r in values]
        ax.errorbar(sizes,ys,yerr=[[y-r['min_seconds']*1000 for y,r in zip(ys,values)],
                                 [r['max_seconds']*1000-y for y,r in zip(ys,values)]],marker='o',capsize=3,label=name,color=color)
    ax.set(xscale='log',yscale='log',title=title,xlabel='Число ранов',ylabel='Время, мс (лог. шкала)');ax.grid(alpha=.2)
axes[0,0].legend();fig.suptitle('Рабочие проекции: медиана и диапазон 9 чтений\nPG/Mongo с индексами; S3 читает JSONL-блоки клиентом')
fig.tight_layout(rect=(0,0,1,.93));fig.savefig(root/'query_scaling.png',dpi=160);plt.close(fig)

size=max(sizes);ops=[('scope_top_100','Фильтр + sort'),('json_filter','Поле JSON'),('player_runs','Массив участников')]
fig,axes=plt.subplots(1,2,figsize=(11,4.5))
for ax,store,name in zip(axes,stores[:2],names[:2]):
    for index,phase in enumerate(('pk_only','indexed')):
        ax.bar([i+(index-.5)*.35 for i in range(len(ops))],
               [lookup[size,store,phase,op]['median_seconds']*1000 for op,title in ops],width=.35,
               label='Только PK' if phase=='pk_only' else 'Доп. индексы')
    ax.set_xticks(range(len(ops)),[title for op,title in ops]);ax.set(yscale='log',ylabel='Время, мс',title=name);ax.legend();ax.grid(axis='y',alpha=.2)
fig.suptitle(f'{size:,} ранов: до и после создания индексов\nКеш не сбрасывался; планы и examined rows сохранены отдельно')
fig.tight_layout(rect=(0,0,1,.88));fig.savefig(root/'index_effect.png',dpi=160);plt.close(fig)

ops=[('clone_table','Копия'),('rename_clone','Переименование'),('update_1000','UPDATE 1000'),
     ('append_100','INSERT 100'),('delete_100','DELETE 100'),('cache_build','Расчёт сводки'),('cached_counts','Чтение сводки')]
fig,ax=plt.subplots(figsize=(12,5.5))
for i,(store,name,color) in enumerate(zip(stores,names,colors)):
    ax.bar([j+(i-1)*.25 for j in range(len(ops))],[lookup[size,store,'table_ops',op]['median_seconds']*1000 for op,title in ops],width=.25,label=name,color=color)
ax.set_xticks(range(len(ops)),[title for op,title in ops],rotation=12);ax.set(yscale='log',ylabel='Время, мс (лог. шкала)',title=f'{size:,} ранов: операции только над тестовыми копиями');ax.legend();ax.grid(axis='y',alpha=.2)
fig.text(.02,.02,'S3: copy/delete объектов, изменение JSONL через copy-on-write, удаление — новая версия манифеста.\nPG/Mongo: операции сервера; копирование индексов и историческая семантика различаются.',fontsize=9)
fig.tight_layout(rect=(0,.1,1,1));fig.savefig(root/'table_operations.png',dpi=160);plt.close(fig)

if (root/'pg_bulk/measurements.csv').exists():
    bulk=list(csv.DictReader((root/'pg_bulk/measurements.csv').open(encoding='utf-8')))
    groups=defaultdict(list)
    for row in bulk:groups[row['method'],row['operation']].append(float(row['seconds']))
    results=[{'method':method,'operation':op,'n':len(v),'median_seconds':statistics.median(v),'min_seconds':min(v),'max_seconds':max(v)} for (method,op),v in sorted(groups.items())]
    with (root/'pg_bulk/summary.csv').open('w',newline='',encoding='utf-8') as out:
        writer=csv.DictWriter(out,fieldnames=list(results[0]));writer.writeheader();writer.writerows(results)
    fig,ax=plt.subplots(figsize=(8,4.5));keys=[('executemany','initial_load'),('copy','initial_load'),('copy','idempotent_copy_merge')]
    values=[groups[k] for k in keys];ys=[statistics.median(v) for v in values]
    ax.bar(['Пакетные INSERT','COPY','COPY staging +\nmerge (повтор)'],ys,
           yerr=[[y-min(v) for y,v in zip(ys,values)],[max(v)-y for y,v in zip(ys,values)]],capsize=4,color=['#336791','#4b9c46','#e2972c'])
    ax.set(ylabel='Время до commit, секунды',title='PostgreSQL: одинаковый вход, разные способы загрузки')
    fig.text(.02,.01,'Три повтора загрузки; merge показан для ранее загруженного набора.\nПолная сверка содержимого после таймера; PK созданы заранее.',fontsize=8)
    fig.tight_layout(rect=(0,.09,1,1));fig.savefig(root/'pg_bulk.png',dpi=160);plt.close(fig)
print('Saved summary.csv and measured charts in',root)
