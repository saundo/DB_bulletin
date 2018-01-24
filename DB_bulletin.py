import datetime
import re
import pandas as pd
import numpy as np

################################################################################
def timeframe_gen(start, end, hour_interval=24, tz='US/Eastern'):
    """creates timeframe for use in making Keen API calls
    + args
    start - start date (str - '2017-08-04'); inclusive
    end - end date (str - '2017-12-04'); inclusive; it will always include
        and never exceed this date
    + kwargs:
    hour_interval - interval for breaking up start, end tuple
    tz - timezone, default to US/Eastern

    returns:
        List of tuples; tuple - (start, end)
    """
    freq = str(hour_interval) + 'H'
    end = pd.to_datetime(end)
    end = end + datetime.timedelta(1)
    start_dates = pd.date_range(start, end, freq=freq, tz=tz)
    start_dates = start_dates.tz_convert('UTC')
    end_dates = start_dates + pd.Timedelta(1, unit='D')

    start_times = [datetime.datetime.strftime(
        i, '%Y-%m-%dT%H:%M:%S.000Z') for i in start_dates]
    end_times = [datetime.datetime.strftime(
        i, '%Y-%m-%dT%H:%M:%S.000Z') for i in end_dates]
    timeframe = [(start_times[i], end_times[i]) for i in range(len(start_times))]

    return timeframe[:-1]

############################### KEEN API FUNCTIONS #############################
def article_time(keen, start, end):

    event = 'read_article'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    target_property = 'read.time.incremental.seconds'

    group_by = ('article.id', 'glass.device',
        'article.authors.names','article.headline.content') # could potentially use 'article.permalink' instead of id

    property_name1 = 'article.bulletin.sponsor.slug'
    operator1 = 'exists'
    property_value1 = True

    property_name2 = 'read.type'
    operator2 = 'in'
    property_value2 = ['25','50','75','complete']

    property_name3 = 'read.time.incremental.seconds'
    operator3 = 'gte'
    property_value3 = 0

    property_name4 = 'read.time.incremental.seconds'
    operator4 = 'lte'
    property_value4 = 600

    filters = [{"property_name":property_name1,
                "operator":operator1,
                "property_value":property_value1},
               {"property_name":property_name2,
                "operator":operator2,
                "property_value":property_value2},
               {"property_name":property_name3,
                "operator":operator3,
                "property_value":property_value3},
               {"property_name":property_name4,
                "operator":operator4,
                "property_value":property_value4}]

    data = keen.sum(event,
                    timeframe=timeframe,
                    target_property=target_property,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=filters)

    df = pd.DataFrame(data)
    df['start'] = start
    df['end'] = end
    print(start+": Done", end=' | ')
    return df

def article_start_completes(keen, start, end):
    event = 'read_article'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('article.id', 'glass.device', 'read.type') # could potentially use 'article.permalink' instead of id

    property_name1 = 'article.bulletin.sponsor.slug'
    operator1 = 'exists'
    property_value1 = True

    property_name2 = 'read.type'
    operator2 = 'in'
    property_value2 = ['start','complete']


    filters = [{"property_name":property_name1,
                "operator":operator1,
                "property_value":property_value1},
               {"property_name":property_name2,
                "operator":operator2,
                "property_value":property_value2}]

    data = keen.count(event,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=filters)

    df = pd.DataFrame(data)
    df['start'] = start
    df['end'] = end
    print(start+": Done", end=' | ')
    return df

def hyperlink_clicks(keen, start, end):
    event = 'click_article_link'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('article.id', 'glass.device', 'link.share')

    property_name1 = 'article.bulletin.sponsor.slug'
    operator1 = 'exists'
    property_value1 = True

    filters = [{"property_name":property_name1,
                "operator":operator1,
                "property_value":property_value1}]

    data = keen.count(event,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=filters)

    df = pd.DataFrame(data)
    df['start'] = start
    df['end'] = end
    print(start+": Done", end=' | ')
    return df

def unique_users(keen, start, end):
    event = 'read_article'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    target_property = 'user.cookie.permanent.id'

    group_by = ('article.id', 'glass.device')

    property_name1 = 'article.bulletin.sponsor.slug'
    operator1 = 'exists'
    property_value1 = True

    filters = [{"property_name":property_name1,
                "operator":operator1,
                "property_value":property_value1}]

    data = keen.count_unique(event,
                    target_property=target_property,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=filters)

    df = pd.DataFrame(data)
    df['start'] = start
    df['end'] = end
    print(start+": Done", end=' | ')
    return df

def interactive_sessions(keen, start, end):
    event = 'ad_interaction'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None
    target_property = 'user.cookie.session.id'

    group_by = ('ad_meta.client.name', 'ad_meta.campaign.name',
                'ad_meta.creative.name', 'raw_url', 'glass.device')

    property_name1 = 'ad_meta.unit.type'
    operator1 = 'eq'
    property_value1 = 'content'

    filters = [{"property_name":property_name1,
                "operator":operator1,
                "property_value":property_value1}]

    data = keen.count_unique(event,
                    target_property=target_property,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=filters)

    df = pd.DataFrame(data)
    df['start'] = start
    df['end'] = end
    print(start+": Done", end=' | ')
    return df

########################### GENERAL PURPOSE FUNCTIONS ##########################
def id_cleaner(article_id):
    """
    for removing the hyphon's from compound article IDs
    """
    if isinstance(article_id, str):
        article_id = article_id.split("-")
        article_id = int(article_id[0])
        return article_id
    else:
        return article_id

def dataframe_cleaner(list_df):
    df = pd.concat(list_df)
    df = df.reset_index(drop=True)
    df['date'] = pd.to_datetime(df['start']).dt.date
    df['end'] = pd.to_datetime(df['end']).dt.date
    return df

def id_from_url(url):
    """
    doesn't work if there isn't an int in the url
    """
    try:
        ID = int(re.search(r'\d+', url).group())
        return ID
    except:
        return url

########################### KEEN DF WRANGLING ##################################
def df_time_wrangle(df_time):
    x = df_time
    columns = ['author', 'headline', 'id', 'glass.device', 'time', 'x', 'end', 'date', 'id_scrub']
    x.columns = columns

    cols = ['author', 'headline', 'glass.device', 'time', 'date', 'id_scrub']
    x = x[cols]

    # if authors are stored as lists this will cause groupby to fail
    x['author'] = x['author'].apply(
        lambda x: x.pop() if isinstance(x, list) else x)

    grouped = ('id_scrub', 'date', 'author', 'headline', 'glass.device')
    x = x.groupby(grouped, as_index=False).sum()

    return x

def df_start_wrangle(df_start):

    x = pd.pivot_table(
            df_start,
            index=['id_scrub', 'date', 'glass.device'],
            columns='read.type',
            values='result',
            aggfunc=np.sum)

    x = x.reset_index()
    x = x.fillna(0)

    grouped = ('date', 'glass.device', 'id_scrub')
    x = x.groupby(grouped, as_index=False).sum()

    return x

def df_clicks_wrangle(df_clicks):

    df_clicks['link.share'] = df_clicks['link.share'].fillna('hyperlink')
    x = pd.pivot_table(
            df_clicks,
            index=['id_scrub', 'date', 'glass.device'],
            columns='link.share',
            values='result',
            aggfunc=np.sum)

    x = x.reset_index()
    x = x.fillna(0)

    grouped = ('date', 'glass.device', 'id_scrub')
    x = x.groupby(grouped, as_index=False).sum()

    return x

def df_users_wrangle(df_users):

    grouped = ('date', 'glass.device', 'id_scrub')
    x = df_users.groupby(grouped, as_index=False).sum()
    x = x.rename(columns={'result':'uniques'})

    return x

def df_sessions_wrangle(df_sessions):
    #df_sessions
    grouped = ('date', 'id_scrub', 'glass.device')
    x = df_sessions.groupby(grouped, as_index=False).sum()
    x = x.rename(columns={'result':'sessions'})

    return x