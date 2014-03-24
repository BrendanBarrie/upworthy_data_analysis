import pandas as pd
import requests as r
import json
import time as t
import sqlite3

db = sqlite3.connect("cache.db")

def create_table():
    try:
        c = db.cursor()
        c.execute("""CREATE TABLE facebook_link_data (
        url TEXT,
        normalized_url TEXT,
        click_count INT,
        total_count INT,
        comment_count INT,
        like_count INT,
        share_count INT);
        """)
        db.commit()
    except sqlite3.OperationalError, e:
        pass

[{u'normalized_url': u'https://www.upworthy.com/uncle-sam-is-great-but-if-you-want-me-to-join-your-army-this-is-the-way-to-go', u'click_count': 0, u'total_count': 0, u'comment_count': 0, u'like_count': 0, u'share_count': 0}]

def get_facebook_data(link):
    c = db.cursor()
    c.execute("SELECT url, normalized_url, click_count, total_count, comment_count, like_count, share_count FROM facebook_link_data WHERE url=?;", (link,))
    d = c.fetchone()
    if (not (d is None)):
        result = {u'normalized_url': d[1], u'click_count': d[2], u'total_count': d[3], u'comment_count': d[4], u'like_count': d[5], u'share_count': d[6]}
        return result

    data = r.get("https://api.facebook.com/method/fql.query?query=select%20total_count,like_count,comment_count,share_count,click_count,normalized_url%20from%20link_stat%20where%20url=%27"+ link + "%27&format=json")
    row = data.json()[0]
    c.execute("INSERT INTO facebook_link_data (url, normalized_url, click_count, total_count, comment_count, like_count, share_count) VALUES (?,?,?,?,?,?,?);",
              (link,
               row['normalized_url'],
               row['click_count'],
               row['total_count'],
               row['comment_count'],
               row['like_count'],
               row['share_count']))
    db.commit()
    print ("Inserted into cache: " + str(row))
    return row

def fb_data_getter(df, column_name, new_data_column_append):
    total_count = []
    click_count = []
    comment_count = []
    like_count =[]
    share_count=[]
    normalized_url =[]
    special = []
    for link in df[column_name]:
        if pd.isnull(link) != True:
            json_obj = get_facebook_data(link)
            total_count.append(json_obj['total_count'])
            click_count.append(json_obj["click_count"])
            comment_count.append(json_obj['comment_count'])
            like_count.append(json_obj['like_count'])
            normalized_url.append(json_obj['normalized_url'])
            share_count.append(json_obj['share_count'])
            special.append(0)
            t.sleep(1)
        else:
            total_count.append("")
            click_count.append('')
            comment_count.append('')
            like_count.append('')
            normalized_url.append('')
            share_count.append('')
            special.append(1)

    df['total_count'+new_data_column_append] = total_count
    df['click_count'+new_data_column_append] = click_count
    df['like_count'+new_data_column_append] = like_count
    df['comment_count'+new_data_column_append] = comment_count
    df['share_count'+new_data_column_append] = share_count
    df['normalized_url'+new_data_column_append] = normalized_url
    return df



def data_creator(upworthy):
    upworthy2 = fb_data_getter(upworthy, "org_link", "base")
    return fb_data_getter(upworthy2, "Canonical Link Element 1", 'canonical')

if __name__=="__main__":
    create_table()
    df = pd.read_csv("internal_upworthy-4.csv")
    data_creator(df)
