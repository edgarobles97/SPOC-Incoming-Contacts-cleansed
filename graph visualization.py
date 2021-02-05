# -*- coding: utf-8 -*-
"""
Created on Thu Jan 21 14:08:10 2021

@author: Edgar
"""

import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import datetime as dt



if not os.path.exists("images"):
    os.mkdir("images")

os.chdir('C:\\Users\\Edgar\\Desktop\\Stori\\WBR Data\\SPOC')

spoc_final = pd.read_csv('spoc_final_all_test.csv')

spoc_final['date_entered'] = pd.to_datetime(spoc_final['date_entered'])

spoc_final['year'] = spoc_final['date_entered'].dt.year
spoc_final['month'] = spoc_final['date_entered'].dt.month

spoc_final['day_week'] = spoc_final['date_entered'].apply(dt.datetime.weekday)
spoc_final['hour'] = spoc_final['date_entered'].dt.hour

spoc_final['week_date'] = spoc_final['date_entered'].dt.to_period('W')
spoc_final['week_date'] = spoc_final['week_date'].astype(str)
spoc_final['week_date'] = [i[:10] for i in spoc_final['week_date']]
spoc_final['week_date'] = pd.to_datetime(spoc_final['week_date'])

dayweek = pd.DataFrame.from_dict({'date':[0,1,2,3,4,5,6],
'day_of_week':['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']})

spoc_final = spoc_final.merge(dayweek, how='left', left_on='day_week',right_on= 'date')


spoc_final.head()
# In[89]:


ticks = spoc_final.groupby(['day','name']).count()['case_id'].reset_index()


# In[90]:


ticks_w = spoc_final.groupby(['week_date','name']).count()['case_id'].reset_index().sort_values('week_date', ascending = False)


# In[91]:


top_names = spoc_final.groupby(['name']).count()['case_id'].sort_values(ascending = False).head(10)




top_ticks = spoc_final.groupby(['week_date'])['name'].value_counts()

top_ticks = top_ticks.to_frame().rename(columns={'name':'incoming_tickets'})

top_ticks = top_ticks.reset_index()

top_ticks = top_ticks[top_ticks['name'].isin(top_names.to_frame().reset_index().name.tolist())]


# In[97]:


hist_top = px.area(top_ticks, x = 'week_date', y = 'incoming_tickets', color='name')
hist_top.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=1.02,
    xanchor="right",
    x=1
))

hist_top.show()


# In[98]:


fig = px.bar(top_ticks, x = 'week_date', y='incoming_tickets', color = 'name'
            )

fig.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=1.02,
    xanchor="right",
    x=1
))

fig.show()



top_ticks_per = spoc_final.groupby(['week_date'])['name'].value_counts(normalize=True)

top_ticks_per = top_ticks_per.to_frame().rename(columns={'name':'percentage'})

top_ticks_per = top_ticks_per.reset_index()

top_ticks_per = top_ticks_per[top_ticks_per['name'].isin(top_names.to_frame().reset_index().name.tolist())]


ttp_fig = px.bar(top_ticks_per, x = 'week_date', y='percentage', color = 'name'
            )

ttp_fig.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=1.02,
    xanchor="right",
    x=1
))

ttp_fig.show()


current_week = str(spoc_final['week_date'].max())


spoc_cur = spoc_final[spoc_final['week_date'] == current_week]


spoc_cur_per = spoc_cur.name.value_counts(normalize=True).to_frame().rename(columns={'name':'percentage'}).reset_index()


spoc_cur_per = spoc_cur_per[spoc_cur_per['index'].isin(top_names.to_frame().reset_index().name.tolist())]


spoc_pie = px.pie(spoc_cur_per, values='percentage', names='index', title='Incoming tickets by category')
spoc_pie.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=1.02,
    xanchor="right",
    x=1
))
spoc_pie.show()


# In[110]:


spoc_heat_cur = spoc_cur.groupby(['day_of_week','hour']).count()['case_id'].to_frame().reset_index()


# In[111]:


order = ['Monday', 'Tuesday', 'Wednesday','Thursday','Friday','Saturday','Sunday']


# In[112]:


trace = go.Heatmap( x = spoc_heat_cur['hour'],
                    y = spoc_heat_cur['day_of_week'],
                    z = spoc_heat_cur['case_id'],
                    type = 'heatmap',
                    colorscale = 'Viridis')

heat = go.Figure(data = trace)
heat.update_layout(yaxis={'categoryarray':order})

heat.show()


from plotly.io import write_image

heat.write_image('heat_test.png')

# In[121]:


spoc_cur_table = spoc_cur.groupby('name')['case_id'].count()


# In[124]:


spoc_cur_table = spoc_cur_table.sort_values(ascending=False).to_frame().rename({'case_id':'incoming_tickets'}, axis=1).reset_index()


# In[125]:


spoc_cur_solved = spoc_cur[spoc_cur['state']== 'Closed'].groupby('name')['case_id'].count().sort_values(ascending=False).to_frame().rename({'case_id':'solved_tickets'}, axis=1).reset_index()


# In[128]:
spoc_cur_table = spoc_cur_table.merge(spoc_cur_solved, how = 'inner', on='name')

# In[130]:


spoc_cur_table['per_solved'] = spoc_cur_table['solved_tickets']/spoc_cur_table['incoming_tickets']


# In[132]:


spoc_cur_table