#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
import os
import jsonlines


# In[6]:


df_session = pd.read_json("./output-structured/ctimeeting2020/Session.jsonl", lines=True)
df_presentation = pd.read_json("./output-structured/ctimeeting2020/Presentation.jsonl", lines=True)
df_session_person = pd.read_json("./output-structured/ctimeeting2020/SessionPerson.jsonl", lines=True)
df_presentation_person = pd.read_json("./output-structured/ctimeeting2020/PresentationPerson.jsonl", lines=True)
df_person = pd.read_json("./output-structured/ctimeeting2020/Person.jsonl", lines=True)


# Check lengths

# In[7]:


print("{} {} {} {} {}".format(
    len(df_session), len(df_presentation), len(df_session_person), len(df_presentation_person), len(df_person)))


# test all person_id exist within person df

# In[8]:


all_person_ids = list(df_session_person.person_id) + list(df_presentation_person.person_id)


# In[9]:


[item for item in all_person_ids if item not in list(df_person.id)]


# test all session_id exist within session df

# In[10]:


all_session_ids = list(df_session_person.session_id) + list(df_presentation.session_id)


# In[11]:


[item for item in all_session_ids if item not in list(df_session.id)]


# visualize presentations having no persons

# In[12]:


all_ids = list(df_presentation.id)


# In[13]:


df_presentation[
    df_presentation.id.isin([item for item in all_ids if item not in list(df_presentation_person.presentation_id)])
]


# visualize sessions having no persons

# In[14]:


all_ids = list(df_session.id)


# In[15]:


df_session[
    ~df_session.id.isin(list(df_session_person.session_id))
]


# In[ ]:




