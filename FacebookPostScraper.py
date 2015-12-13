#Library Files (Packages)
import copy
import csv
import json
import socket
import time
import urllib.request
import os

#Parameters
socket.setdefaulttimeout(30)
id_file = 'PageNames.txt'
clientid = '913090612110859'
clientsecret = '933d825823bb355288c9f24c79d296d8'
outfile = 'SamsungUSA.csv'
fb_urlobj = urllib.request.urlopen('https://graph.facebook.com/oauth/access_token?grant_type=client_credentials&client_id=' + clientid + '&client_secret=' + clientsecret)
fb_token = fb_urlobj.read().decode(encoding="latin1")


#Functions
def load_data(id_file,enc='utf-8'):
    fb_pages = []
    f=open(id_file,'r',encoding = enc,errors = 'replace')
    fb_pages=f.readlines()
    f.close()
    return fb_pages


def url_retry(url):
    succ = 0
    while succ == 0:
        try:
            json_out = json.loads(urllib.request.urlopen(url).read().decode(encoding="utf-8"))
            succ = 1
        except Exception as e:
            print(str(e))
            if 'HTTP Error 4' in str(e):
                return False
            else:
                time.sleep(1)
    return json_out

def optional_field(dict_item,dict_key):
    try:
        out = dict_item[dict_key]
        if dict_key == 'shares':
            out = dict_item[dict_key]['count']
    except KeyError:
        out = ''
    return out

def scrape_fb_page(fb_json_page,fb_post_filter):
    csv_chunk = []
    if fb_post_filter == 'None':
        for line in fb_json_page['data']:
            csv_line = [line['from']['name'], \
            '_' + line['from']['id'], \
            optional_field(line,'message'), \
            optional_field(line,'picture'), \
            optional_field(line,'link'), \
            optional_field(line,'name'), \
            optional_field(line,'description'), \
            optional_field(line,'type'), \
            line['created_time'], \
            optional_field(line,'shares'), \
            line['id']]
            csv_chunk.append(csv_line)
    else:
        for line in fb_json_page['data']:
            if fb_post_filter in optional_field(line,'message'):
                csv_line = [line['from']['name'], \
                '_' + line['from']['id'], \
                optional_field(line,'message'), \
                optional_field(line,'picture'), \
                optional_field(line,'link'), \
                optional_field(line,'name'), \
                optional_field(line,'description'), \
                optional_field(line,'type'), \
                line['created_time'], \
                optional_field(line,'shares'), \
                line['id']]
                csv_chunk.append(csv_line)

    return csv_chunk

def scrape_fb_post(fb_json_page,thread_starter,msg):
    csv_chunk = []
    for line in fb_json_page['data']:
        csv_line = [line['from']['name'], \
        '_' + line['from']['id'], \
        optional_field(line,'message'), \
        line['created_time'], \
        optional_field(line,'like_count'), \
        line['id'], \
        thread_starter, \
        msg]
        csv_chunk.append(csv_line)

    return csv_chunk

#function to create a dir
def create_structure(file_name):
    #make dir for the category
    if not os.path.exists(os.path.splitext(file_name)[0]):
        os.makedirs(os.path.splitext(file_name)[0])

def create_post_file(dir_name,file_name,post_details):
    #make log file for the run
    post_file=r'./'+dir_name+r'/'+file_name+'.csv'
    f=open(post_file,'w',encoding='utf-8')
    f.write('\"post from\",\"post from_id\",\"comment from\",\"comment from_id\",\"message\",\"picture\",\"link\",\"name\",\"description\",\"type\",\"created_time\",\"shares\",\"like_count\",\"post_id\",\"original_poster\",\"original_message\"\n')
    f.write('\"'+str(post_details[0])+'\",\"'+str(post_details[1])+'\",\"'+''+'\",\"'+''+'\",\"'
            +str(post_details[2])+'\",\"'
            +str(post_details[3])+'\",\"'+str(post_details[4])+'\",\"'
            +str(post_details[5])+'\",\"'+str(post_details[6])+'\",\"'
            +str(post_details[7])+'\",\"'+str(post_details[8])+'\",\"'+
            str(post_details[9])+'\",\"'+''+'\",\"'
            +str(post_details[10])+'\"\n')
    f.close()

def add_comments(dir_name,file_name,comment):
    post_file=r'./'+dir_name+r'/'+file_name+'.csv'
    f=open(post_file,'a',encoding='utf-8')
    f.write('\"\",\"\",\"'+str(comment[0])+'\",\"'+str(comment[1])+'\",\"'+str(comment[2]).strip('\n')+'\",\"'
            +''+'\",\"'
            +'\",\"'+''+'\",\"'+''+'\",\"'+''+'\",\"'
            +str(comment[3])+'\",\"'+''+'\",\"'
            +str(comment[4])+'\",\"'+str(comment[5])+'\",\"'
            +str(comment[6])+'\",\"'+str(comment[7])+'\"\n')
    f.close()

#Main
def main():
    fb_ids = load_data(id_file)
    scrape_posts = []

    for fid in fb_ids:
        print('\n------------------------------------------------------------------------\n')
        fb_pg_name=fid.strip('\n').split(':')[0]
        try:
            fb_post_filter=fid.strip('\n').split(':')[1]
        except Exception as e:
            fb_post_filter='None'
        print('Page Name : ',fb_pg_name)
        print('Post Filter : ',fb_post_filter)

        #create a folder for the PAge
        create_structure(fb_pg_name)

        #get the facebook posts for the page
        scrape_posts=getPostfor(fb_pg_name,fb_post_filter)

        for fb_post in scrape_posts:
            scrape_comments=[]
            fb_post_id=fb_post[10]
            #create csv file for each post
            create_post_file(fb_pg_name,fb_post_id,fb_post)

            #get comments for the post
            scrape_comments=getCommentsfor(fb_post_id)
            for post_comments in scrape_comments:
                #add comment to file
                scrape_comments2=[]
                add_comments(fb_pg_name,fb_post_id,post_comments)
                comment_id=post_comments[5]
                scrape_comments2=getCommentsfor(comment_id)
                for reply_comments in scrape_comments2:
                    add_comments(fb_pg_name,fb_post_id,reply_comments)

        print('\n------------------------------------------------------------------------\n')


def getPostfor(fb_pg_name,fb_post_filter):
    csv_data = []

    scrape_mode = 'posts'
    msg_user = ''
    msg_content = ''
    field_list = 'from,message,picture,link,name,description,type,created_time,shares'

    post_url = 'https://graph.facebook.com/v2.4/' + fb_pg_name + '/' + scrape_mode + '?fields=' + field_list + '&limit=100&' + fb_token
    next_item = url_retry(post_url)

    if next_item != False:
        csv_data = csv_data + scrape_fb_page(next_item,fb_post_filter)
    else:
        return csv_data

    while 'paging' in next_item and 'next' in next_item['paging']:
        next_item = url_retry(next_item['paging']['next'])
        csv_data = csv_data + scrape_fb_page(next_item,fb_post_filter)
        time.sleep(1)

    return csv_data

def getCommentsfor(fb_post_id):
    csv_data=[]
    if '_' in fb_post_id:
        scrape_mode = 'comments'
        msg_url = 'https://graph.facebook.com/v2.4/' + fb_post_id + '?fields=from,message&' + fb_token
        msg_json = url_retry(msg_url)
        msg_user = msg_json['from']['name']
        msg_content = optional_field(msg_json,'message')
        field_list = 'from,message,created_time,like_count'
    comment_url = 'https://graph.facebook.com/v2.4/' + fb_post_id + '/' + scrape_mode + '?fields=' + field_list + '&limit=100&' + fb_token
    next_item = url_retry(comment_url)
    if next_item != False:
        csv_data = csv_data + scrape_fb_post(next_item,msg_user,msg_content)
    else:
        return csv_data

    while 'paging' in next_item and 'next' in next_item['paging']:
        next_item = url_retry(next_item['paging']['next'])
        csv_data = csv_data + scrape_fb_post(next_item,msg_user,msg_content)
        time.sleep(1)

    return csv_data





#Call Main Function
main()
