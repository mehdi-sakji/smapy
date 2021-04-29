#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import uuid
import os
import jsonlines
import datetime


# In[2]:


firstrawdf = pd.read_json("./rawoutput/firstraw.json")


# In[3]:


secondrawdf = pd.read_json("./rawoutput/secondraw.json")


# In[4]:


breaks = pd.read_json("./rawoutput/breaks.json", lines=True)


# In[5]:


len(firstrawdf)


# In[6]:


len(secondrawdf)


# In[7]:


len(breaks)


# Structure Sessions

# In[8]:


breaks


# In[9]:


sessionsdf = pd.concat([
    firstrawdf[firstrawdf["class"]=="session"].drop(columns=[
    "content_id", "url", "session_order", "session_range", "session_id", "role"
    ]), breaks
])


# In[10]:


len(sessionsdf)


# In[11]:


sessionsdf["rangeorder"] = sessionsdf["range"]*100 + sessionsdf["order"]


# In[12]:


sessionsdf["rangeorder"] = sessionsdf["rangeorder"].apply(int)


# In[13]:


sessionsdf.drop(columns=["class", "order", "range"], inplace=True)


# In[14]:


sessionsdf.sort_values(by="rangeorder", inplace=True)


# In[15]:


sessionsdf


# In[16]:


sessionsdf.reset_index(inplace=True, drop=True)


# In[17]:


sessionsdf["order"] = sessionsdf.index + 1


# In[18]:


sessionsdf.drop(columns=["rangeorder"], inplace=True)


# In[19]:


sessionsdf["description"] = sessionsdf["description"].apply(
    lambda x: np.nan if pd.isnull(x) else x)


# In[20]:


sessionsdf["session_type"] = sessionsdf["session_type"].apply(lambda value: value.strip())


# Structure Presentations

# In[21]:


presentationsdf = firstrawdf[firstrawdf["class"]=="presentation"]


# In[22]:


presentationsdf = firstrawdf[firstrawdf["class"]=="presentation"].drop(columns=[
    "class", "range", "event_id", "session_type", "location", "content_id", "role", "url"
    ])


# In[23]:


presentationsdf


# In[24]:


presentationsdf["rangeorder"] = presentationsdf[
    "session_range"]*10000 + presentationsdf["session_order"]*100 + presentationsdf["order"]
presentationsdf["rangeorder"] = presentationsdf["rangeorder"].apply(int)
presentationsdf.drop(columns=["session_range", "session_order", "order"], inplace=True)


# In[25]:


presentationsdf.sort_values(by="rangeorder", inplace=True)


# In[26]:


presentationsdf.reset_index(inplace=True, drop=True)
presentationsdf["order"] = presentationsdf.index + 1
presentationsdf.drop(columns=["rangeorder"], inplace=True)


# In[27]:


presentationsdf["description"] = presentationsdf["description"].apply(
    lambda x: np.nan if pd.isnull(x) else x)


# In[28]:


presentationsdf


# Structure Persons

# In[29]:


personsdf = secondrawdf.drop(columns=[
    "class", "content_id", "role", "full_name"])


# In[30]:


personsdf


# In[31]:


personsdf["id"] = str(uuid.uuid4())


# In[32]:


personsdf.rename(columns={"university": "affiliation"}, inplace=True)


# In[33]:


personsdf


# Structure SessionPersons and PresentationPersons

# In[34]:


sessionpersonsdf = firstrawdf[firstrawdf["class"]=="sessionperson"][["url", "role", "content_id"]]


# In[35]:


sessionpersonsdf.rename(columns={"content_id": "session_id"}, inplace=True)


# In[36]:


sessionpersonsdf = sessionpersonsdf.merge(personsdf[["url", "id"]], on="url").rename(
    columns={"id": "person_id"})


# In[37]:


sessionpersonsdf.drop(columns=["url"], inplace=True)


# In[38]:


sessionpersonsdf


# In[39]:


presentationpersonsdf = firstrawdf[firstrawdf["class"]=="presentationperson"][["url", "role", "content_id"]]
presentationpersonsdf.rename(columns={"content_id": "presentation_id"}, inplace=True)
presentationpersonsdf = presentationpersonsdf.merge(personsdf[["url", "id"]], on="url").rename(
    columns={"id": "person_id"})
presentationpersonsdf.drop(columns=["url"], inplace=True)


# In[40]:


presentationpersonsdf


# In[41]:


personsdf.drop(columns=["url"], inplace=True)


# In[42]:


personsdf


# Event structuring

# In[43]:


eventdf = pd.DataFrame([{
    "airfinity_id": "AE13078", "end_date": "2020-10-07", "id": personsdf.iloc[0]["event_id"], 
    "name": "88th EAS Congress", "source_platform": "ctimeetingtech",
    "source_url": "https://cslide.ctimeetingtech.com/eas20/attendee/confcal/session", "start_date": "2020-10-04"}])


# Saving all

# In[44]:


def save_as_jsonline(df_event, df_session, df_presentation, df_person,
                     df_presentation_person, df_session_person):

    event_output_folder = "ctimeeting2020"
    if not os.path.exists("./output-structured/{}".format(event_output_folder)):
        os.makedirs("output-structured/{}".format(event_output_folder))

    with jsonlines.open('./output-structured/{}/Event.jsonl'.format(event_output_folder), mode='w') as writer:
        df_event["downloaded_at"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
        writer.write_all(df_event.T.apply(lambda x: x.dropna().to_dict()).tolist())
    
    with jsonlines.open('./output-structured/{}/Session.jsonl'.format(event_output_folder), mode='w') as writer:
        df_session["start_time"] = df_session["start_time"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        df_session["end_time"] = df_session["end_time"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        writer.write_all(df_session.T.apply(lambda x: x.dropna().to_dict()).tolist())
        
    with jsonlines.open('./output-structured/{}/Presentation.jsonl'.format(event_output_folder), mode='w') as writer:
        df_presentation["start_time"] = df_presentation["start_time"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        df_presentation["end_time"] = df_presentation["end_time"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        writer.write_all(df_presentation.T.apply(lambda x: x.dropna().to_dict()).tolist())
        
    with jsonlines.open('./output-structured/{}/Person.jsonl'.format(event_output_folder), mode='w') as writer:
        writer.write_all(df_person.T.apply(lambda x: x.dropna().to_dict()).tolist())
        
    with jsonlines.open('./output-structured/{}/PresentationPerson.jsonl'.format(event_output_folder), mode='w') as writer:
        writer.write_all(df_presentation_person.T.apply(lambda x: x.dropna().to_dict()).tolist())
        
    with jsonlines.open('./output-structured/{}/SessionPerson.jsonl'.format(event_output_folder), mode='w') as writer:
        writer.write_all(df_session_person.T.apply(lambda x: x.dropna().to_dict()).tolist())
        
    return 1


# In[45]:


save_as_jsonline(
    eventdf, sessionsdf, presentationsdf, personsdf, presentationpersonsdf, sessionpersonsdf)


# In[ ]:




